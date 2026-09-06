//! Protobuf on the byte lanes, and the JSON transcode it replaces.
//!
//! A `prost`-generated message already owns its byte layout, and unlike an Avro model it is not
//! a serde type - so on a topic with no registry it rides the core's byte lanes directly, with
//! `#[derive(Serialized, Deserialized)]` and `#[wire(prost)]` and no help from this crate at all.
//!
//! What the registry adds is the envelope, and the envelope is where this module comes in. A
//! Confluent-framed Protobuf payload is the zero magic byte, the schema id, a message-index path
//! naming which message of the schema was written, and then the message. The envelope rides the
//! lane as [`IncomingFrame`] / [`OutgoingFrame`], and the two ends of the index path are handled
//! here:
//!
//! - [`decode_framed`] skips the indexes and hands the message bytes to `prost`. It needs no
//!   registry: a generated type knows its own fields, and the index path only says which message
//!   of the schema this is - which the reading type has already decided.
//! - [`Subject`] resolves a subject's id and its message's index path once, at startup, so
//!   framing a value is a pure byte operation with no I/O.
//!
//! # The JSON transcode
//!
//! [`KafkaBroker::schema_registry`](crate::KafkaBroker::schema_registry) and
//! [`SchemaFrame`](crate::SchemaFrame) convert framed Protobuf to and from plain JSON at the
//! broker's edges, through descriptors compiled from the registry's `.proto` source, so handlers
//! keep plain serde structs and never generate code. That is the compatibility path, and it is
//! the one to keep when a service must not carry generated types; it costs a JSON hop and a
//! dynamic message per delivery, and it depends on the registry being reachable to decode
//! anything at all. The two paths do not mix on one broker: a broker carrying
//! [`KafkaBroker::schema_registry`](crate::KafkaBroker::schema_registry) transcodes every
//! subscription it opens, so a frame-reading handler on it would be handed JSON.

use std::marker::PhantomData;

use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use ruststream::BytesMut;

use crate::error::KafkaError;
use crate::frame::{IncomingFrame, OutgoingFrame};
use crate::schema_registry::{RegisteredSchema, SchemaRegistry, SchemaType};

/// Reads a Confluent-framed delivery into a `prost`-generated message.
///
/// No registry is involved, and that is the point: the envelope's message-index path says which
/// message of the schema was written, and the reading type has already decided which one it
/// reads, so skipping the path is all the framing costs. Fields the writer added and this type
/// does not know are preserved as unknown fields, exactly as `prost` handles them anywhere else.
///
/// The schema id is available as [`IncomingFrame::schema_id`](crate::IncomingFrame::schema_id)
/// for a service that wants to check or log which version produced a delivery.
///
/// # Errors
///
/// Returns [`KafkaError::WireFormat`] when the message-index path is truncated, or when the
/// bytes after it are not a message of `T`.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::Deserialized;
/// use ruststream_rdkafka::{IncomingFrame, protobuf};
///
/// #[derive(Clone, PartialEq, prost::Message)]
/// struct Order {
///     #[prost(int64, tag = "1")]
///     id: i64,
/// }
///
/// # fn check() -> Result<(), Box<dyn std::error::Error>> {
/// // Schema id 42, the compact `[0]` index path, then `id = 7`.
/// let payload = [0x00, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x08, 0x07];
/// let frame = IncomingFrame::from_payload(&payload)?;
///
/// let order: Order = protobuf::decode_framed(&frame)?;
/// assert_eq!(order.id, 7);
/// # Ok(())
/// # }
/// # check().unwrap();
/// ```
pub fn decode_framed<T: prost::Message + Default>(
    frame: &IncomingFrame<'_>,
) -> Result<T, KafkaError> {
    let message = skip_indexes(frame.datum()).ok_or_else(|| {
        KafkaError::malformed(format!(
            "the message-index path of schema id {} is truncated, so the Protobuf message after \
             it cannot be found",
            frame.schema_id(),
        ))
    })?;
    T::decode(message).map_err(KafkaError::wire_format)
}

/// A registry subject resolved to a schema id and one message's index path, for one message type.
///
/// The framing a Protobuf producer owes the wire is entirely resolved here: which schema id, and
/// which message of that schema. Both are looked up once, at startup, so [`frame`](Self::frame)
/// is a pure byte operation and a subject that does not exist, or does not declare the message,
/// fails the app's startup rather than its first publish.
///
/// # Examples
///
/// ```no_run
/// use ruststream_rdkafka::{SchemaRegistry, protobuf};
///
/// #[derive(Clone, PartialEq, prost::Message)]
/// struct Confirmation {
///     #[prost(int64, tag = "1")]
///     id: i64,
/// }
///
/// # async fn check() -> Result<(), Box<dyn std::error::Error>> {
/// let sr = SchemaRegistry::new("http://localhost:8081");
/// let subject =
///     protobuf::Subject::<Confirmation>::resolve(&sr, "confirmations-value", "acme.Confirmation")
///         .await?;
///
/// let frame = subject.frame(&Confirmation { id: 7 })?;
/// assert_eq!(frame.schema_id(), subject.schema_id());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Subject<T> {
    schema_id: u32,
    /// The message-index path, already in its wire form: the same bytes prefix every message.
    indexes: Vec<u8>,
    // `fn() -> T` so the marker adds no auto-trait obligation of its own.
    message: PhantomData<fn() -> T>,
}

impl<T: prost::Message> Subject<T> {
    /// Resolves `subject`'s registered schema and the index path of the message named
    /// `message` (fully qualified, package included).
    ///
    /// The message name is a string rather than something read off `T`, because a plain
    /// `prost`-generated type carries no descriptor to read it from. Naming a message that `T`
    /// is not puts a mis-addressed index path on the wire, which consumers report as a decode
    /// failure; binding the two at compile time would mean requiring
    /// `prost_reflect::ReflectMessage` on every generated type, which is a code-generation
    /// dependency this crate should not impose on a service that only wants to publish.
    ///
    /// For the same reason this takes the subject's registered schema at face value, where
    /// [`avro::Subject`](crate::avro::Subject) resolves the id of the *producer's own* schema: a
    /// generated Protobuf type carries no `.proto` source to look up. Protobuf's wire format is
    /// tag-addressed and stays readable across compatible schema versions, which is what makes
    /// that acceptable here and would not be for Avro.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable or has no such
    /// subject, and [`KafkaError::InvalidOptions`] when the subject holds another format's
    /// schema or does not declare `message`.
    pub async fn resolve(
        registry: &SchemaRegistry,
        subject: &str,
        message: &str,
    ) -> Result<Self, KafkaError> {
        let schema = registry.warm(subject).await?;
        if schema.schema_type() != SchemaType::Protobuf {
            return Err(KafkaError::InvalidOptions(format!(
                "subject {subject:?} holds a {:?} schema, not Protobuf",
                schema.schema_type(),
            )));
        }
        let pool = registry.parsed_proto(&schema)?;
        let descriptor = pool.get_message_by_name(message).ok_or_else(|| {
            KafkaError::InvalidOptions(format!(
                "message {message:?} is not in subject {subject:?}'s schema (use the fully \
                 qualified name, package included)",
            ))
        })?;
        let indexes = message_indexes(&descriptor).ok_or_else(|| {
            KafkaError::InvalidOptions(format!(
                "message {message:?} is not declared by the registered schema file",
            ))
        })?;
        Ok(Self {
            schema_id: schema.id(),
            indexes: encode_indexes(&indexes),
            message: PhantomData,
        })
    }

    /// A subject whose id and message-index path are already known, taking no registry at all.
    ///
    /// [`resolve`](Self::resolve) exists to learn those two things; a service that already has
    /// them - a deployment pinning ids in configuration, a replay tool, a test with no registry
    /// in front of it - names them here. The path is the message's position in its schema file:
    /// `[0]` for the first top-level message, `[1, 0]` for the first message nested in the
    /// second, and so on.
    ///
    /// The caller owns the pairing `resolve` establishes: the id must name a schema that
    /// declares `T` at that path, or consumers address the wrong message.
    #[must_use]
    pub fn pinned(schema_id: u32, message_indexes: &[i32]) -> Self {
        Self {
            schema_id,
            indexes: encode_indexes(message_indexes),
            message: PhantomData,
        }
    }

    /// The registry-assigned id of the schema this subject holds.
    #[must_use]
    pub fn schema_id(&self) -> u32 {
        self.schema_id
    }

    /// Encodes `value` behind this subject's message-index path, ready to publish.
    ///
    /// Synchronous by construction: the only things that needed the registry were the id and the
    /// index path, and resolving the subject already took both.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::WireFormat`] when `prost` cannot encode the value.
    pub fn frame(&self, value: &T) -> Result<OutgoingFrame, KafkaError> {
        let mut datum = BytesMut::with_capacity(self.indexes.len() + value.encoded_len());
        datum.extend_from_slice(&self.indexes);
        value.encode(&mut datum).map_err(KafkaError::wire_format)?;
        Ok(OutgoingFrame::new(self.schema_id, datum.to_vec()))
    }
}

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

/// Steps over a Confluent message-index path, returning the message bytes after it.
///
/// The path itself is only needed to address a message inside a schema; a generated type has
/// already made that choice, so the reading lane skips the path rather than materializing it.
fn skip_indexes(datum: &[u8]) -> Option<&[u8]> {
    let (count, mut rest) = read_zigzag(datum)?;
    if count == 0 {
        return Some(rest);
    }
    for _ in 0..count {
        let (_, tail) = read_zigzag(rest)?;
        rest = tail;
    }
    Some(rest)
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
        Mock::given(method("GET"))
            .and(path("/subjects/orders-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": id,
                "version": 1,
                "schema": ORDERS_PROTO,
                "schemaType": "PROTOBUF",
            })))
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

    /// The generated shape a service would get from `prost-build` for `acme.Order`.
    #[derive(Clone, PartialEq, prost::Message)]
    struct Order {
        #[prost(int64, tag = "1")]
        id: i64,
        #[prost(string, tag = "2")]
        item: String,
    }

    #[tokio::test]
    async fn a_subject_frames_behind_its_messages_index_path() {
        let (_server, sr, schema) = registry_with_orders(9).await;

        let subject = Subject::<Order>::resolve(&sr, "orders-value", "acme.Order")
            .await
            .expect("resolve");
        assert_eq!(subject.schema_id(), 9);

        let frame = subject
            .frame(&Order {
                id: 42,
                item: "anvil".to_owned(),
            })
            .expect("frame");
        assert_ne!(frame.datum()[0], 0, "Order is not the first message");

        // The transcode reads the same bytes, so the index path really addresses `acme.Order`.
        let json = protobuf_to_json(&sr, &schema, frame.datum()).expect("transcode");
        let value: serde_json::Value = serde_json::from_slice(&json).expect("json");
        assert_eq!(value["id"], 42);
        assert_eq!(value["item"], "anvil");
    }

    #[tokio::test]
    async fn a_framed_delivery_decodes_with_no_registry() {
        let (_server, sr, _) = registry_with_orders(9).await;
        let subject = Subject::<Order>::resolve(&sr, "orders-value", "acme.Order")
            .await
            .expect("resolve");
        let order = Order {
            id: 7,
            item: "widget".to_owned(),
        };
        let mut wire = BytesMut::new();
        let payload = {
            use ruststream::runtime::Serialized;
            subject
                .frame(&order)
                .expect("frame")
                .wire_bytes(&mut wire)
                .expect("infallible")
                .to_vec()
        };

        let frame = {
            use ruststream::runtime::Deserialized;
            IncomingFrame::from_payload(&payload).expect("framed")
        };
        assert_eq!(decode_framed::<Order>(&frame).expect("decode"), order);
    }

    #[test]
    fn a_truncated_index_path_is_rejected() {
        use ruststream::runtime::Deserialized;

        // A path claiming two entries but carrying none.
        let payload = [0x00, 0x00, 0x00, 0x00, 0x01, 0x04];
        let frame = IncomingFrame::from_payload(&payload).expect("framed");

        let err = decode_framed::<Order>(&frame).expect_err("truncated");
        assert!(err.to_string().contains("truncated"));
    }

    #[test]
    fn skipping_a_path_lands_where_decoding_it_does() {
        for path in [vec![0], vec![1], vec![0, 2], vec![3, 1, 4]] {
            let mut encoded = encode_indexes(&path);
            encoded.extend_from_slice(b"message");
            let (_, decoded_rest) = decode_indexes(&encoded).expect("decodes");

            assert_eq!(skip_indexes(&encoded).expect("skips"), decoded_rest);
        }
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
