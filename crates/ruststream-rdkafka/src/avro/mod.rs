//! Avro behind the codec, and the JSON transcode it replaces.
//!
//! [`AvroCodec`] is how a service reads and writes Avro: the schema sits where a serializer
//! belongs, handlers stay ordinary functions over ordinary structs, and nothing about the wire
//! reaches a signature. That is the only way to consume an Avro payload here, and it is enough
//! for both kinds of topic.
//!
//! # A registry-backed topic
//!
//! [`AvroCodec::registry`] speaks the Confluent wire format. Encoding frames each value with the
//! id of its type's subject, and decoding reads every delivery with the writer schema that
//! delivery's envelope names - which is what makes a datum written by an older producer readable
//! by a newer consumer:
//!
//! ```no_run
//! use apache_avro::AvroSchema;
//! use ruststream::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, AvroSchema)]
//! struct Order {
//!     id: i64,
//! }
//!
//! #[derive(Serialize, Deserialize, AvroSchema, Outgoing)]
//! struct Confirmation {
//!     id: i64,
//! }
//!
//! #[subscriber("orders", publish("confirmations"))]
//! async fn confirm(order: &Order) -> Confirmation {
//!     Confirmation { id: order.id }
//! }
//! # let _ = confirm;
//! ```
//!
//! # A topic with no registry
//!
//! [`AvroCodec::local`] pins one schema: a bare datum on the wire, no envelope, no registry, and
//! no I/O anywhere on the path. [`encode`] and [`decode`] are that same conversion as free
//! functions, for a tool that works on datum bytes outside an app's dispatch.
//!
//! # The JSON transcode
//!
//! [`KafkaBroker::schema_registry`](crate::KafkaBroker::schema_registry) and
//! [`SchemaFrame`](crate::SchemaFrame) convert Avro to and from plain JSON at the broker's edges,
//! leaving handlers on the default codec. That is the compatibility path, for services that
//! deliberately keep plain serde models on registry-backed topics; it is no longer the only one.
//! It costs a JSON hop per message, it cannot express the Avro types JSON has no shape for, and
//! it resolves no writer schema onto a reader schema. The two paths do not mix on one broker: a
//! broker carrying [`KafkaBroker::schema_registry`](crate::KafkaBroker::schema_registry)
//! transcodes every subscription it opens, so a frame-reading handler on it would be handed JSON.

mod codec;

pub use apache_avro::AvroSchema;
pub use codec::AvroCodec;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use apache_avro::Schema;
use apache_avro::reader::datum::{GenericDatumReader, SpecificDatumReader};
use apache_avro::types::Value as AvroValue;
use apache_avro::writer::datum::GenericDatumWriter;
use bytes::BufMut;
use ruststream::BytesMut;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::KafkaError;
use crate::schema_registry::{RegisteredSchema, SchemaRegistry};

/// One message type's Avro artefacts, built once per type.
///
/// `AvroSchema::get_schema` is documented as expensive, and both a datum writer and a specific
/// reader resolve the schema's names when they are built - so building either per message would
/// put a schema-driven format's startup cost on the delivery path. The map memoizes a pure
/// function of the type (its own schema); no configuration, no registry and nothing about a
/// running app reaches it, so two apps in one process share it without interfering.
struct Prepared<T: AvroSchema> {
    writer: GenericDatumWriter<'static>,
    reader: SpecificDatumReader<T>,
}

/// The prepared artefacts of every message type this process has encoded or decoded.
static PREPARED: OnceLock<RwLock<HashMap<TypeId, &'static (dyn Any + Send + Sync)>>> =
    OnceLock::new();

fn prepared<T>() -> Result<&'static Prepared<T>, KafkaError>
where
    T: AvroSchema + Send + Sync + 'static,
{
    let cache = PREPARED.get_or_init(|| RwLock::new(HashMap::new()));
    let key = TypeId::of::<T>();
    if let Some(entry) = cache
        .read()
        .expect("prepared schema cache mutex poisoned")
        .get(&key)
    {
        return Ok(downcast(*entry));
    }

    // Leaked rather than reference-counted: one entry per message type, so the set is bounded by
    // the program's own types, and a `&'static Schema` is what a datum writer borrows for life.
    let schema: &'static Schema = Box::leak(Box::new(T::get_schema()));
    let writer = GenericDatumWriter::builder(schema)
        .build()
        .map_err(KafkaError::wire_format)?;
    let reader = SpecificDatumReader::<T>::builder()
        .build()
        .map_err(KafkaError::wire_format)?;
    let entry: &'static (dyn Any + Send + Sync) = Box::leak(Box::new(Prepared { writer, reader }));

    // A racing thread may have inserted first; either entry is the same schema, so the one
    // already in the map wins and this one is simply never looked up again.
    let stored = *cache
        .write()
        .expect("prepared schema cache mutex poisoned")
        .entry(key)
        .or_insert(entry);
    Ok(downcast(stored))
}

/// The map is keyed by the very `TypeId` the entry was built under, so the downcast is a type
/// identity the insert established rather than a guess.
fn downcast<T: AvroSchema + 'static>(
    entry: &'static (dyn Any + Send + Sync),
) -> &'static Prepared<T> {
    entry
        .downcast_ref()
        .expect("the prepared entry of a type id is that type's")
}

/// Writes `value` as an Avro datum under the schema its own type declares.
///
/// The schema drives the encoding, so the value's Rust types reach the wire as the numeric and
/// string types the schema names, with no JSON document in between. No registry is involved and
/// no envelope is written: on a registry-backed topic the datum travels inside one, which
/// [`AvroCodec::registry`] adds. A service publishes through a codec; this is for a tool that
/// works on datum bytes outside an app's dispatch.
///
/// The buffer is written into rather than returned, so a caller that already holds one (a
/// publish path, a batch) pays no intermediate allocation.
///
/// # Errors
///
/// Returns [`KafkaError::WireFormat`] when the type's schema cannot be built, or when the value
/// does not fit the schema it declares.
///
/// # Examples
///
/// ```
/// use apache_avro::AvroSchema;
/// use ruststream::BytesMut;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, AvroSchema)]
/// struct Order {
///     id: i64,
/// }
///
/// # fn check() -> Result<(), Box<dyn std::error::Error>> {
/// let mut buf = BytesMut::new();
/// ruststream_rdkafka::avro::encode(&Order { id: 7 }, &mut buf)?;
/// assert_eq!(&buf[..], &[14]); // one zigzag varint
/// # Ok(())
/// # }
/// # check().unwrap();
/// ```
pub fn encode<T>(value: &T, buf: &mut BytesMut) -> Result<(), KafkaError>
where
    T: AvroSchema + Serialize + Send + Sync + 'static,
{
    let prepared = prepared::<T>()?;
    prepared
        .writer
        .write_ser(&mut buf.writer(), value)
        .map_err(KafkaError::wire_format)?;
    Ok(())
}

/// Reads an Avro datum written with the reading type's own schema.
///
/// Writer and reader schema are the same one here, which is what a topic with no registry means:
/// there is no second schema to resolve against. A Confluent-framed delivery goes through
/// [`AvroCodec::registry`] instead, which resolves the writer schema the envelope names - reading
/// a framed payload with this function would decode the envelope's own bytes as if they were the
/// datum.
///
/// # Errors
///
/// Returns [`KafkaError::WireFormat`] when the type's schema cannot be built, or when the bytes
/// are not a datum of that schema.
///
/// # Examples
///
/// ```
/// use apache_avro::AvroSchema;
/// use ruststream::BytesMut;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
/// struct Order {
///     id: i64,
/// }
///
/// # fn check() -> Result<(), Box<dyn std::error::Error>> {
/// let mut buf = BytesMut::new();
/// ruststream_rdkafka::avro::encode(&Order { id: 7 }, &mut buf)?;
///
/// let back: Order = ruststream_rdkafka::avro::decode(&buf)?;
/// assert_eq!(back, Order { id: 7 });
/// # Ok(())
/// # }
/// # check().unwrap();
/// ```
pub fn decode<T>(payload: &[u8]) -> Result<T, KafkaError>
where
    T: AvroSchema + DeserializeOwned + Send + Sync + 'static,
{
    let prepared = prepared::<T>()?;
    let mut cursor = payload;
    prepared
        .reader
        .read(&mut cursor)
        .map_err(KafkaError::wire_format)
}

/// The JSON definition a schema is registered under.
///
/// Deliberately not `Schema::canonical_form`: the Parsing Canonical Form keeps only what two
/// schemas must agree on to be *the same* schema, and drops field defaults, aliases, docs and
/// logical types. Those are exactly what a reader resolves an older writer's datum with, so a
/// subject registered in canonical form can never carry an evolution.
pub(crate) fn schema_json(schema: &Schema) -> Result<String, KafkaError> {
    serde_json::to_string(schema).map_err(KafkaError::wire_format)
}

/// Decodes an Avro datum against its registry schema and re-encodes it as JSON.
pub(crate) fn avro_to_json(
    registry: &SchemaRegistry,
    schema: &RegisteredSchema,
    datum: &[u8],
) -> Result<Vec<u8>, KafkaError> {
    let writer = registry.parsed_avro(schema)?;
    let mut cursor = datum;
    // No reader schema: the registry hands back the schema the datum was written with, and the
    // handler sees plain JSON, so there is nothing to resolve the value onto.
    let value = GenericDatumReader::builder(&writer)
        .build()
        .map_err(KafkaError::schema_registry)?
        .read_value(&mut cursor)
        .map_err(KafkaError::schema_registry)?;
    let json: serde_json::Value =
        apache_avro::from_value(&value).map_err(KafkaError::schema_registry)?;
    serde_json::to_vec(&json).map_err(KafkaError::schema_registry)
}

/// Serializes a JSON document as an Avro datum against the subject's registry schema.
pub(crate) fn json_to_avro(
    registry: &SchemaRegistry,
    schema: &RegisteredSchema,
    payload: &[u8],
) -> Result<Vec<u8>, KafkaError> {
    let parsed = registry.parsed_avro(schema)?;
    let json: serde_json::Value =
        serde_json::from_slice(payload).map_err(KafkaError::schema_registry)?;
    // The direct JSON conversion, not the serde one: serde sees every non-negative JSON integer
    // as a `u64`, which apache-avro encodes as its `org.apache.avro.rust.u64` logical type (a
    // Fixed of 8 bytes) and which then resolves against no numeric Avro schema. This conversion
    // picks `int` or `long` by magnitude, which is what a registry schema declares. The lanes
    // above have no such hazard: they never see a JSON document.
    let value = AvroValue::try_from(json)
        .map_err(KafkaError::schema_registry)?
        .resolve(&parsed)
        .map_err(KafkaError::schema_registry)?;
    GenericDatumWriter::builder(&parsed)
        .build()
        .map_err(KafkaError::schema_registry)?
        .write_value_to_vec(value)
        .map_err(KafkaError::schema_registry)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
    struct Order {
        id: i64,
        item: String,
    }

    async fn registry_with_order(id: u32) -> (MockServer, SchemaRegistry, RegisteredSchema) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "id": id })))
            .mount(&server)
            .await;
        let registry = SchemaRegistry::new(server.uri());
        registry
            .register_avro::<Order>("orders-value")
            .await
            .expect("register");
        let schema = registry.cached_subject("orders-value").expect("cached");
        (server, registry, (*schema).clone())
    }

    #[test]
    fn a_value_roundtrips_through_its_own_schema() {
        let order = Order {
            id: 42,
            item: "anvil".to_owned(),
        };
        let mut buf = BytesMut::new();
        encode(&order, &mut buf).expect("encode");

        assert_ne!(
            &buf[..],
            br#"{"id":42,"item":"anvil"}"#,
            "the wire form is Avro"
        );
        assert_eq!(decode::<Order>(&buf).expect("decode"), order);
    }

    /// The bug the JSON transcode carries and the codec cannot: through serde a JSON document's
    /// every non-negative integer is a `u64`, which apache-avro writes as its own logical type.
    /// A value that never becomes a JSON document has real Rust types all the way down.
    #[test]
    fn positive_integers_encode_as_the_declared_numeric_type() {
        let mut buf = BytesMut::new();
        encode(
            &Order {
                id: i64::from(u32::MAX),
                item: String::new(),
            },
            &mut buf,
        )
        .expect("encode");

        assert_eq!(
            decode::<Order>(&buf).expect("decode").id,
            i64::from(u32::MAX),
        );
    }

    #[tokio::test]
    async fn json_avro_json_roundtrips() {
        let (_server, registry, schema) = registry_with_order(7).await;
        let json = br#"{"id":42,"item":"anvil"}"#;

        let datum = json_to_avro(&registry, &schema, json).expect("encode");
        assert_ne!(datum.as_slice(), json, "the wire form is Avro, not JSON");

        let back = avro_to_json(&registry, &schema, &datum).expect("decode");
        let order: Order = serde_json::from_slice(&back).expect("deserialize");
        assert_eq!(
            order,
            Order {
                id: 42,
                item: "anvil".to_owned(),
            },
        );
    }

    #[tokio::test]
    async fn schema_mismatches_error_clearly() {
        let (_server, registry, schema) = registry_with_order(7).await;
        let err = json_to_avro(&registry, &schema, br#"{"id":"not-a-number"}"#)
            .expect_err("a document violating the schema must fail");
        assert!(matches!(err, KafkaError::SchemaRegistry(_)));
    }
}
