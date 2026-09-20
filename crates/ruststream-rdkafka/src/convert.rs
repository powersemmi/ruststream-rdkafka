//! Conversions between rdkafka message data and `RustStream` types.

use bytes::Bytes;
use rdkafka::message::{BorrowedMessage, Header, Headers as _, Message as _, OwnedHeaders};
use ruststream::{HeaderMap, Str};

use crate::eos::EOS_SOURCE_HEADER;
use crate::message::PARTITION_KEY_HEADER;

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
        headers.insert(
            Str::from_static(PARTITION_KEY_HEADER),
            Bytes::copy_from_slice(key),
        );
    }
    headers
}

/// The outgoing record parts split from `RustStream` headers: the native wire headers and the
/// record key.
#[derive(Debug)]
pub(crate) struct PublishParts {
    pub(crate) headers: Option<OwnedHeaders>,
    pub(crate) key: Option<Bytes>,
}

/// Splits outgoing headers into native Kafka headers and the record key.
///
/// [`PARTITION_KEY_HEADER`] becomes the native record key (so Kafka partitions by it) and is not
/// duplicated as a wire header. Consuming through this crate reconstructs the key header from the
/// record. Where the record goes when it carries no key is a per-record setting
/// ([`KafkaOptions`](crate::KafkaOptions)), not a header.
pub(crate) fn headers_for_publish(headers: &HeaderMap) -> PublishParts {
    let key = headers
        .get(PARTITION_KEY_HEADER)
        .map(Bytes::copy_from_slice);
    let mut native = OwnedHeaders::new_with_capacity(headers.len());
    let mut count = 0;
    for (name, value) in headers.iter() {
        if name.eq_ignore_ascii_case(PARTITION_KEY_HEADER)
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
    PublishParts {
        headers: (count > 0).then_some(native),
        key,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_split_maps_key_and_skips_its_header() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");
        headers.insert(PARTITION_KEY_HEADER, "order-1");

        let parts = headers_for_publish(&headers);
        assert_eq!(parts.key.as_deref(), Some(b"order-1".as_slice()));
        let native = parts.headers.expect("one wire header expected");
        assert_eq!(native.count(), 1);
        assert_eq!(native.get(0).key, "content-type");
    }

    #[test]
    fn publish_split_without_headers_is_empty() {
        let parts = headers_for_publish(&HeaderMap::new());
        assert!(parts.headers.is_none());
        assert!(parts.key.is_none());
    }
}
