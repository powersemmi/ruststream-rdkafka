//! Conversions between rdkafka message data and `RustStream` types.

use bytes::Bytes;
use rdkafka::message::{BorrowedMessage, Header, Headers as _, Message as _, OwnedHeaders};
use ruststream::HeaderMap;

use crate::eos::EOS_SOURCE_HEADER;
use crate::error::KafkaError;
use crate::message::{PARTITION_HEADER, PARTITION_KEY_HEADER};

/// Collects a delivery's native headers, plus its native record key surfaced as
/// [`PARTITION_KEY_HEADER`], into `RustStream` headers.
///
/// The key header always mirrors the native record key: a same-named wire header from a
/// foreign producer is skipped even when the record is keyless, so `key()` never reports a key
/// Kafka did not partition by. Null-valued wire headers arrive with an empty value (presence
/// preserved; core headers have no null representation).
pub(crate) fn headers_from_message(msg: &BorrowedMessage<'_>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Some(native) = msg.headers() {
        for header in native.iter() {
            if header.key.eq_ignore_ascii_case(PARTITION_KEY_HEADER) {
                continue;
            }
            let value = header.value.map_or_else(Bytes::new, Bytes::copy_from_slice);
            headers.insert(header.key, value);
        }
    }
    if let Some(key) = msg.key() {
        headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(key));
    }
    headers
}

/// Splits outgoing headers into native Kafka headers, the record key, and the explicit
/// destination partition.
///
/// [`PARTITION_KEY_HEADER`] becomes the native record key (so Kafka partitions by it) and
/// [`PARTITION_HEADER`] the record's explicit partition (winning over the key); neither is
/// duplicated as a wire header. Consuming through this crate reconstructs the key header from
/// the record.
///
/// # Errors
///
/// Returns [`KafkaError::InvalidOptions`] when the partition header is not an ASCII decimal
/// partition index: a typo must fail the publish, not fall back to the partitioner.
/// The outgoing record parts split from `RustStream` headers.
#[derive(Debug)]
pub(crate) struct PublishParts {
    pub(crate) headers: Option<OwnedHeaders>,
    pub(crate) key: Option<Bytes>,
    pub(crate) partition: Option<i32>,
}

pub(crate) fn headers_for_publish(headers: &HeaderMap) -> Result<PublishParts, KafkaError> {
    let key = headers
        .get(PARTITION_KEY_HEADER)
        .map(Bytes::copy_from_slice);
    let partition = headers
        .get_str(PARTITION_HEADER)
        .map(|value| {
            value.parse::<i32>().map_err(|_| {
                KafkaError::InvalidOptions(format!(
                    "the {PARTITION_HEADER} header must be an ASCII decimal partition index, \
                     got {value:?}",
                ))
            })
        })
        .transpose()?;
    let mut native = OwnedHeaders::new_with_capacity(headers.len());
    let mut count = 0;
    for (name, value) in headers.iter() {
        if name.eq_ignore_ascii_case(PARTITION_KEY_HEADER)
            || name.eq_ignore_ascii_case(PARTITION_HEADER)
            || name.eq_ignore_ascii_case(EOS_SOURCE_HEADER)
        {
            continue;
        }
        native = native.insert(Header {
            key: name,
            value: Some(value),
        });
        count += 1;
    }
    Ok(PublishParts {
        headers: (count > 0).then_some(native),
        key,
        partition,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_split_maps_key_and_skips_its_header() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");
        headers.insert(PARTITION_KEY_HEADER, "order-1");

        let parts = headers_for_publish(&headers).expect("valid headers");
        assert_eq!(parts.key.as_deref(), Some(b"order-1".as_slice()));
        assert_eq!(parts.partition, None);
        let native = parts.headers.expect("one wire header expected");
        assert_eq!(native.count(), 1);
        assert_eq!(native.get(0).key, "content-type");
    }

    #[test]
    fn publish_split_without_headers_is_empty() {
        let parts = headers_for_publish(&HeaderMap::new()).expect("empty ok");
        assert!(parts.headers.is_none());
        assert!(parts.key.is_none());
        assert!(parts.partition.is_none());
    }

    #[test]
    fn publish_split_extracts_the_explicit_partition() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_HEADER, "3");

        let parts = headers_for_publish(&headers).expect("valid headers");
        assert!(
            parts.headers.is_none(),
            "the partition header must not hit the wire"
        );
        assert!(parts.key.is_none());
        assert_eq!(parts.partition, Some(3));
    }

    #[test]
    fn publish_split_rejects_a_malformed_partition() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_HEADER, "three");

        let err = headers_for_publish(&headers).expect_err("must reject");
        assert!(matches!(err, KafkaError::InvalidOptions(_)));
        assert!(err.to_string().contains("kafka-partition"));
    }
}
