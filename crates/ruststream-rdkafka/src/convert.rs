//! Conversions between rdkafka message data and `RustStream` types.

use bytes::Bytes;
use rdkafka::message::{BorrowedMessage, Header, Headers as _, Message as _, OwnedHeaders};
use ruststream::Headers;

use crate::message::PARTITION_KEY_HEADER;

/// Collects a delivery's native headers, plus its native record key surfaced as
/// [`PARTITION_KEY_HEADER`], into `RustStream` headers.
///
/// The key header always mirrors the native record key: a same-named wire header from a
/// foreign producer is skipped even when the record is keyless, so `key()` never reports a key
/// Kafka did not partition by. Null-valued wire headers arrive with an empty value (presence
/// preserved; core headers have no null representation).
pub(crate) fn headers_from_message(msg: &BorrowedMessage<'_>) -> Headers {
    let mut headers = Headers::new();
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

/// Splits outgoing headers into native Kafka headers plus the record key.
///
/// [`PARTITION_KEY_HEADER`] becomes the native record key (so Kafka partitions by it) and is not
/// duplicated as a wire header; consuming through this crate reconstructs it from the key.
pub(crate) fn headers_for_publish(headers: &Headers) -> (Option<OwnedHeaders>, Option<Bytes>) {
    let key = headers
        .get(PARTITION_KEY_HEADER)
        .map(Bytes::copy_from_slice);
    let mut native = OwnedHeaders::new_with_capacity(headers.len());
    let mut count = 0;
    for (name, value) in headers.iter() {
        if name.eq_ignore_ascii_case(PARTITION_KEY_HEADER) {
            continue;
        }
        native = native.insert(Header {
            key: name,
            value: Some(value),
        });
        count += 1;
    }
    ((count > 0).then_some(native), key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_split_maps_key_and_skips_its_header() {
        let mut headers = Headers::new();
        headers.insert("content-type", "application/json");
        headers.insert(PARTITION_KEY_HEADER, "order-1");

        let (native, key) = headers_for_publish(&headers);
        assert_eq!(key.as_deref(), Some(b"order-1".as_slice()));
        let native = native.expect("one wire header expected");
        assert_eq!(native.count(), 1);
        assert_eq!(native.get(0).key, "content-type");
    }

    #[test]
    fn publish_split_without_headers_is_empty() {
        let (native, key) = headers_for_publish(&Headers::new());
        assert!(native.is_none());
        assert!(key.is_none());
    }
}
