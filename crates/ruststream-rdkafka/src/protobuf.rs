//! Protobuf transcoding for the Schema Registry middleware.
//!
//! With the `protobuf` feature, the broker middleware converts Confluent-framed Protobuf both
//! ways without forcing code generation onto handlers: the registry stores Protobuf schemas
//! as `.proto` source, the client compiles them to descriptors once (protox, with the
//! well-known types available), and messages convert to and from JSON through dynamic
//! messages (prost-reflect) - proto field names preserved, 64-bit integers as numbers, so
//! plain serde structs match the `.proto` shape directly. The Confluent envelope's
//! message-indexes are handled on both sides: the compact `[0]` form and full paths for
//! nested and multi-message schemas (outgoing messages default to the schema's first
//! top-level message; pin another with
//! [`SchemaFrame::message`](crate::SchemaFrame::message)).

use prost_reflect::prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};

use crate::error::KafkaError;
use crate::schema_registry::{RegisteredSchema, SchemaRegistry};

/// Decodes a framed Protobuf datum (indexes + message) against its registry schema and
/// re-encodes it as JSON.
pub(crate) fn protobuf_to_json(
    registry: &SchemaRegistry,
    schema: &RegisteredSchema,
    datum: &[u8],
) -> Result<Vec<u8>, KafkaError> {
    let pool = registry.parsed_proto(schema)?;
    let (indexes, message_bytes) = decode_indexes(datum).ok_or_else(|| {
        KafkaError::InvalidOptions("malformed message-indexes in the Protobuf envelope".to_owned())
    })?;
    let descriptor = message_by_indexes(&pool, &indexes).ok_or_else(|| {
        KafkaError::InvalidOptions(format!(
            "message indexes {indexes:?} do not address a message in schema id {}",
            schema.id(),
        ))
    })?;
    let dynamic =
        DynamicMessage::decode(descriptor, message_bytes).map_err(KafkaError::schema_registry)?;
    let options = prost_reflect::SerializeOptions::new()
        .use_proto_field_name(true)
        .skip_default_fields(false)
        .stringify_64_bit_integers(false);
    let mut json = serde_json::Serializer::new(Vec::new());
    dynamic
        .serialize_with_options(&mut json, &options)
        .map_err(KafkaError::schema_registry)?;
    Ok(json.into_inner())
}

/// Serializes a JSON document as a framed Protobuf datum (indexes + message) against the
/// subject's registry schema; `message` picks the message within it (`None` = the first
/// top-level one).
pub(crate) fn json_to_protobuf(
    registry: &SchemaRegistry,
    schema: &RegisteredSchema,
    message: Option<&str>,
    payload: &[u8],
) -> Result<Vec<u8>, KafkaError> {
    let pool = registry.parsed_proto(schema)?;
    let descriptor = match message {
        Some(name) => pool.get_message_by_name(name).ok_or_else(|| {
            KafkaError::InvalidOptions(format!(
                "message {name:?} is not in the subject's schema (use the fully qualified \
                 name, package included)",
            ))
        })?,
        None => registry_file(&pool)
            .and_then(|file| file.messages().next())
            .ok_or_else(|| {
                KafkaError::InvalidOptions("the subject's schema declares no messages".to_owned())
            })?,
    };
    let indexes = message_indexes(&descriptor).ok_or_else(|| {
        KafkaError::InvalidOptions(format!(
            "message {:?} is not declared by the registered schema file",
            descriptor.full_name(),
        ))
    })?;

    let json: serde_json::Value =
        serde_json::from_slice(payload).map_err(KafkaError::schema_registry)?;
    let dynamic =
        DynamicMessage::deserialize(descriptor, json).map_err(KafkaError::schema_registry)?;
    let mut datum = encode_indexes(&indexes);
    datum.extend_from_slice(&dynamic.encode_to_vec());
    Ok(datum)
}

/// The registry's own file (imports precede it in the compiled set).
fn registry_file(pool: &DescriptorPool) -> Option<prost_reflect::FileDescriptor> {
    pool.files().last()
}

/// The message's index path within its file: `[i]` for the i-th top-level message, deeper
/// entries for nested declarations.
fn message_indexes(descriptor: &MessageDescriptor) -> Option<Vec<i32>> {
    let mut path = Vec::new();
    let mut current = descriptor.clone();
    while let Some(parent) = current.parent_message() {
        let index = parent.child_messages().position(|child| child == current)?;
        path.push(i32::try_from(index).ok()?);
        current = parent;
    }
    let file = current.parent_file();
    let index = file.messages().position(|message| message == current)?;
    path.push(i32::try_from(index).ok()?);
    path.reverse();
    Some(path)
}

/// Resolves the message a Confluent index path addresses, against the registry's own file.
fn message_by_indexes(pool: &DescriptorPool, indexes: &[i32]) -> Option<MessageDescriptor> {
    let file = registry_file(pool)?;
    let mut iter = indexes.iter();
    let first = usize::try_from(*iter.next()?).ok()?;
    let mut message = file.messages().nth(first)?;
    for index in iter {
        let index = usize::try_from(*index).ok()?;
        let child = message.child_messages().nth(index)?;
        message = child;
    }
    Some(message)
}

/// Encodes a message-index path the Confluent way: zigzag varints, with `[0]` compacted to a
/// single zero byte.
fn encode_indexes(indexes: &[i32]) -> Vec<u8> {
    if indexes == [0] {
        return vec![0];
    }
    let mut out = Vec::with_capacity(1 + indexes.len());
    write_zigzag(&mut out, i64::try_from(indexes.len()).unwrap_or(i64::MAX));
    for index in indexes {
        write_zigzag(&mut out, i64::from(*index));
    }
    out
}

/// Decodes a Confluent message-index path, returning it and the message bytes after it.
fn decode_indexes(datum: &[u8]) -> Option<(Vec<i32>, &[u8])> {
    let (count, mut rest) = read_zigzag(datum)?;
    if count == 0 {
        return Some((vec![0], rest));
    }
    let count = usize::try_from(count).ok()?;
    let mut indexes = Vec::with_capacity(count);
    for _ in 0..count {
        let (index, tail) = read_zigzag(rest)?;
        indexes.push(i32::try_from(index).ok()?);
        rest = tail;
    }
    Some((indexes, rest))
}

fn write_zigzag(out: &mut Vec<u8>, value: i64) {
    #[allow(clippy::cast_sign_loss)] // zigzag mapping is the point
    let mut encoded = ((value << 1) ^ (value >> 63)) as u64;
    loop {
        let byte = (encoded & 0x7f) as u8;
        encoded >>= 7;
        if encoded == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn read_zigzag(bytes: &[u8]) -> Option<(i64, &[u8])> {
    let mut encoded: u64 = 0;
    let mut shift = 0u32;
    for (position, byte) in bytes.iter().enumerate() {
        encoded |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            #[allow(clippy::cast_possible_wrap)] // zigzag mapping is the point
            let value = ((encoded >> 1) as i64) ^ -((encoded & 1) as i64);
            return Some((value, &bytes[position + 1..]));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::schema_registry::SchemaType;

    const ORDERS_PROTO: &str = r#"
syntax = "proto3";
package acme;

message Ignored {
  string noise = 1;
}

message Order {
  int64 id = 1;
  string item = 2;

  message Line {
    string sku = 1;
    int32 quantity = 2;
  }
}
"#;

    async fn registry_with_orders(id: u32) -> (MockServer, SchemaRegistry, RegisteredSchema) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "id": id })))
            .mount(&server)
            .await;
        let sr = SchemaRegistry::new(server.uri());
        sr.register("orders-value", SchemaType::Protobuf, ORDERS_PROTO)
            .await
            .expect("register");
        let schema = sr.cached_subject("orders-value").expect("cached");
        (server, sr, (*schema).clone())
    }

    #[tokio::test]
    async fn json_protobuf_json_roundtrips_with_index_paths() {
        let (_server, sr, schema) = registry_with_orders(5).await;
        let json = br#"{"id":42,"item":"anvil"}"#;

        // Order is the second top-level message: a real index path, not the compact zero.
        let datum = json_to_protobuf(&sr, &schema, Some("acme.Order"), json).expect("encode");
        assert_ne!(datum[0], 0, "a real index path must be encoded");

        let back = protobuf_to_json(&sr, &schema, &datum).expect("decode");
        let value: serde_json::Value = serde_json::from_slice(&back).expect("json");
        assert_eq!(value["id"], 42);
        assert_eq!(value["item"], "anvil");
    }

    #[tokio::test]
    async fn nested_messages_address_by_index_path() {
        let (_server, sr, schema) = registry_with_orders(6).await;
        let json = br#"{"sku":"SKU-1","quantity":3}"#;

        let datum = json_to_protobuf(&sr, &schema, Some("acme.Order.Line"), json).expect("encode");
        let back = protobuf_to_json(&sr, &schema, &datum).expect("decode");
        let value: serde_json::Value = serde_json::from_slice(&back).expect("json");
        assert_eq!(value["sku"], "SKU-1");
        assert_eq!(value["quantity"], 3);
    }

    #[tokio::test]
    async fn unknown_messages_error_clearly() {
        let (_server, sr, schema) = registry_with_orders(7).await;
        let err = json_to_protobuf(&sr, &schema, Some("acme.Missing"), b"{}")
            .expect_err("unknown message");
        assert!(err.to_string().contains("fully qualified"));
    }

    #[test]
    fn index_paths_roundtrip() {
        for path in [vec![0], vec![1], vec![0, 2], vec![3, 1, 4]] {
            let encoded = encode_indexes(&path);
            let (decoded, rest) = decode_indexes(&encoded).expect("decodes");
            assert_eq!(decoded, path);
            assert!(rest.is_empty());
        }
        assert_eq!(
            encode_indexes(&[0]),
            vec![0],
            "the compact single-zero form"
        );
    }

    #[test]
    fn zigzag_varints_roundtrip() {
        for value in [0i64, 1, -1, 63, 64, -64, 300, i64::from(i32::MAX)] {
            let mut out = Vec::new();
            write_zigzag(&mut out, value);
            let (decoded, rest) = read_zigzag(&out).expect("decodes");
            assert_eq!(decoded, value);
            assert!(rest.is_empty());
        }
    }
}
