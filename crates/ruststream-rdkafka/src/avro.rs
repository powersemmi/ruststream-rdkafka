//! Avro on the byte lanes, and the JSON transcode it replaces.
//!
//! Avro owns its byte layout, so the wire form belongs on the core's `Serialized` /
//! `Deserialized` lanes rather than behind a codec: an Avro payload arrives as the bytes it was
//! written as, and a value is made from them by Avro's own reader, against Avro's own schema.
//!
//! # What rides the lane, and why it is the envelope
//!
//! The lane type is the Confluent envelope ([`IncomingFrame`], [`OutgoingFrame`]), not the
//! message model, and the value is converted by an explicit call. That split is forced from two
//! sides and neither of them is a preference:
//!
//! - `apache-avro` is a serde-driven implementation of Avro: reading and writing a Rust struct as
//!   a datum goes through `Serialize` / `Deserialize`, guided by the schema. The core's lanes are
//!   selected by the type and are reserved for types that are *not* serde types - `MessageWire`,
//!   `ReplyShape` and `Input` are blanket-implemented for every `Serialize` / `DeserializeOwned`
//!   value - so a type that Avro can encode is a type the lanes will not accept. This is why
//!   `#[wire(prost)]` works and an equivalent `#[wire(avro)]` cannot: a `prost` message is not a
//!   serde type.
//! - Resolving a schema id is a registry conversation and therefore `async`, while
//!   `Deserialized::from_payload` is a sync associated function with no context to reach a
//!   registry from. The envelope needs nothing but the bytes, so it rides the lane; the
//!   resolution stays where `async` is allowed.
//!
//! So the wire form is a lane type, the value's conversion is one call, and nothing hides an I/O
//! stall inside a decode or reaches for a process-wide registry singleton.
//!
//! # A registry-backed topic
//!
//! [`decode_framed`] awaits the writer schema the envelope names and resolves the datum onto the
//! handler's own reader schema - which is what makes a datum written by an older producer
//! readable by a newer consumer, and what a fixed-schema decoder cannot do. On the publish side a
//! [`Subject`] resolves its id once, at startup, so publishing itself does no I/O:
//!
//! ```no_run
//! use apache_avro::AvroSchema;
//! use ruststream::prelude::*;
//! use ruststream_rdkafka::{IncomingFrame, OutgoingFrame, SchemaRegistry, avro};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, AvroSchema)]
//! struct Order {
//!     id: i64,
//! }
//!
//! #[derive(Serialize, Deserialize, AvroSchema)]
//! struct Confirmation {
//!     id: i64,
//! }
//!
//! /// Resolved once at startup and injected into the handler.
//! #[derive(Clone)]
//! struct Wiring {
//!     registry: SchemaRegistry,
//!     confirmations: avro::Subject<Confirmation>,
//! }
//!
//! #[subscriber("orders", publish("confirmations"))]
//! async fn confirm(
//!     frame: &IncomingFrame<'_>,
//!     State(wiring): State<Wiring>,
//! ) -> Result<OutgoingFrame, HandlerOutcome> {
//!     let order: Order = avro::decode_framed(&wiring.registry, frame)
//!         .await
//!         .map_err(|_| HandlerOutcome::drop())?;
//!     wiring
//!         .confirmations
//!         .frame(&Confirmation { id: order.id })
//!         .map_err(|_| HandlerOutcome::drop())
//! }
//! # let _ = confirm;
//! ```
//!
//! # A topic with no registry
//!
//! [`encode`] and [`decode`] are the same conversion without the envelope: the type's own schema
//! is both writer and reader, no registry is involved, and no JSON is in the path. They are what
//! the framed pair is built on, and what a plain Avro topic uses directly.
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

pub use apache_avro::AvroSchema;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;
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
use crate::frame::{IncomingFrame, OutgoingFrame};
use crate::schema_registry::{RegisteredSchema, SchemaRegistry, SchemaType};

/// One message type's Avro artefacts, built once per type.
///
/// `AvroSchema::get_schema` is documented as expensive, and both a datum writer and a specific
/// reader resolve the schema's names when they are built - so building either per message would
/// put a schema-driven format's startup cost on the delivery path, which is exactly what the
/// lanes exist to avoid. The map memoizes a pure function of the type (its own schema); no
/// configuration, no registry and nothing about a running app reaches it, so two apps in one
/// process share it without interfering.
struct Prepared<T: AvroSchema> {
    schema: &'static Schema,
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
    let entry: &'static (dyn Any + Send + Sync) = Box::leak(Box::new(Prepared {
        schema,
        writer,
        reader,
    }));

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
/// [`Subject::frame`] adds.
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
/// [`decode_framed`] instead, which resolves the writer schema the envelope names - reading a
/// framed payload with this function would decode the envelope's own bytes as if they were the
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

/// Reads a Confluent-framed delivery, resolving the schema the envelope names onto `T`'s own.
///
/// This is the read half of a registry-backed topic, and the reason it is an `async` function
/// the handler calls rather than a lane a type declares: the writer schema is discovered from
/// the delivery and fetched from the registry, which no sync associated function can do. Every
/// id resolves over the network once per process and from the shared cache afterwards.
///
/// The resolution is what distinguishes this from [`decode`]: a datum written under an older
/// version of the subject's schema is projected onto the reader's, so added fields take their
/// declared defaults and dropped ones are skipped.
///
/// # Errors
///
/// Returns [`KafkaError::SchemaRegistry`] when the registry cannot resolve the envelope's id,
/// and [`KafkaError::WireFormat`] when the id names a non-Avro schema, the two schemas do not
/// resolve against each other, or the datum does not match the writer schema.
///
/// # Examples
///
/// ```no_run
/// use apache_avro::AvroSchema;
/// use ruststream::prelude::*;
/// use ruststream_rdkafka::{IncomingFrame, SchemaRegistry};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize, AvroSchema)]
/// struct Order {
///     id: i64,
/// }
///
/// #[subscriber("orders")]
/// async fn consume(frame: &IncomingFrame<'_>, State(sr): State<SchemaRegistry>) -> HandlerOutcome {
///     let Ok(order) = ruststream_rdkafka::avro::decode_framed::<Order>(&sr, frame).await else {
///         return HandlerOutcome::drop();
///     };
///     println!("order {}", order.id);
///     HandlerOutcome::ack()
/// }
/// # let _ = consume;
/// ```
pub async fn decode_framed<T>(
    registry: &SchemaRegistry,
    frame: &IncomingFrame<'_>,
) -> Result<T, KafkaError>
where
    T: AvroSchema + DeserializeOwned + Send + Sync + 'static,
{
    let schema = registry.schema_by_id(frame.schema_id()).await?;
    let writer = avro_schema(registry, &schema)?;
    let reader = prepared::<T>()?.schema;
    let mut cursor = frame.datum();
    // Through a resolved `Value` rather than the direct reader: the writer schema is the
    // delivery's, not the type's, so the projection onto the reader schema is the whole point of
    // this path and the direct reader cannot express it.
    let value = GenericDatumReader::builder(&writer)
        .reader_schema(reader)
        .build()
        .map_err(KafkaError::wire_format)?
        .read_value(&mut cursor)
        .map_err(KafkaError::wire_format)?;
    apache_avro::from_value(&value).map_err(KafkaError::wire_format)
}

/// A registry subject resolved to the id its schema has there, for one message type.
///
/// The id and the datum have to name the same schema or the payload is unreadable, so this type
/// is the pairing: it is minted from `T`'s own schema and frames `T`'s values with the id the
/// registry gave exactly that schema. A subject resolved for one type cannot frame another's
/// value, and the resolution happens once, at startup, so [`frame`](Self::frame) does no I/O at
/// all - a publish never stalls on the registry, and a subject that is missing or incompatible
/// fails the app's startup instead of its first message.
///
/// # Examples
///
/// ```no_run
/// use apache_avro::AvroSchema;
/// use ruststream_rdkafka::{SchemaRegistry, avro::Subject};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, AvroSchema)]
/// struct Confirmation {
///     id: i64,
/// }
///
/// # async fn check() -> Result<(), Box<dyn std::error::Error>> {
/// let sr = SchemaRegistry::new("http://localhost:8081");
/// let subject = Subject::<Confirmation>::register(&sr, "confirmations-value").await?;
///
/// let frame = subject.frame(&Confirmation { id: 7 })?;
/// assert_eq!(frame.schema_id(), subject.schema_id());
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Subject<T> {
    schema_id: u32,
    // `fn() -> T` so the marker adds no auto-trait obligation of its own: a resolved subject is
    // shared across handler tasks whether or not `T` is.
    message: PhantomData<fn() -> T>,
}

impl<T> Clone for Subject<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Subject<T> {}

impl<T> Subject<T>
where
    T: AvroSchema + Serialize + Send + Sync + 'static,
{
    /// Registers `T`'s schema under `subject` (idempotent registry-side: an identical schema
    /// keeps its id) and takes the id back.
    ///
    /// The counterpart of [`resolve`](Self::resolve), for producers that own their subject.
    /// Deployments where producers must not create schemas (Confluent's own guidance, with
    /// `auto.register.schemas` off) use `resolve` instead.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable, rejects the
    /// credentials, or refuses the schema as incompatible with the subject's history, and
    /// [`KafkaError::WireFormat`] when `T`'s own schema cannot be built.
    pub async fn register(registry: &SchemaRegistry, subject: &str) -> Result<Self, KafkaError> {
        let schema_id = registry
            .register(
                subject,
                SchemaType::Avro,
                schema_json(prepared::<T>()?.schema)?,
            )
            .await?;
        Ok(Self {
            schema_id,
            message: PhantomData,
        })
    }

    /// A subject whose id is already known, taking no registry at all.
    ///
    /// The two async constructors exist to learn one number; a service that already has it - a
    /// deployment pinning ids in configuration, a replay tool reading an id off a captured
    /// record, a test with no registry in front of it - names it here instead of standing up a
    /// registry to be told what it knows.
    ///
    /// The caller owns the pairing that [`register`](Self::register) and
    /// [`resolve`](Self::resolve) establish: `schema_id` must be the id of `T`'s own schema, or
    /// consumers decode this producer's datums against the wrong one.
    #[must_use]
    pub fn pinned(schema_id: u32) -> Self {
        Self {
            schema_id,
            message: PhantomData,
        }
    }

    /// Resolves the id `T`'s schema already has under `subject`, registering nothing.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable or does not hold
    /// this exact schema under the subject - which is the diagnostic a producer wants at
    /// startup, naming the subject rather than failing on its first message.
    pub async fn resolve(registry: &SchemaRegistry, subject: &str) -> Result<Self, KafkaError> {
        let schema_id = registry
            .lookup_id(
                subject,
                SchemaType::Avro,
                schema_json(prepared::<T>()?.schema)?,
            )
            .await?;
        Ok(Self {
            schema_id,
            message: PhantomData,
        })
    }

    /// The registry-assigned id of `T`'s schema under this subject.
    #[must_use]
    pub fn schema_id(&self) -> u32 {
        self.schema_id
    }

    /// Writes `value` as an Avro datum and pairs it with this subject's id, ready to publish.
    ///
    /// Synchronous by construction: the only thing that needed the registry was the id, and
    /// resolving the subject already took it.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::WireFormat`] when the value does not fit the schema its type
    /// declares.
    pub fn frame(&self, value: &T) -> Result<OutgoingFrame, KafkaError> {
        let mut buf = BytesMut::new();
        encode(value, &mut buf)?;
        Ok(OutgoingFrame::new(self.schema_id, buf.to_vec()))
    }
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

/// The parsed Avro schema of a registered one, rejecting the other flavors by name.
fn avro_schema(
    registry: &SchemaRegistry,
    schema: &RegisteredSchema,
) -> Result<std::sync::Arc<Schema>, KafkaError> {
    if schema.schema_type() != SchemaType::Avro {
        return Err(KafkaError::malformed(format!(
            "schema id {} is {:?}, not Avro; the delivery was written by a producer of another \
             format",
            schema.id(),
            schema.schema_type(),
        )));
    }
    registry.parsed_avro(schema)
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
    use ruststream::runtime::{Deserialized, Serialized};
    use serde::{Deserialize, Serialize};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
    struct Order {
        id: i64,
        item: String,
    }

    /// The same record with one field added, carrying a default - the evolution case Avro's
    /// schema resolution exists for.
    #[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
    #[serde(rename = "Order")]
    struct OrderV2 {
        id: i64,
        item: String,
        #[avro(default = r#""""#)]
        note: String,
    }

    async fn registry_with_order(id: u32) -> (MockServer, SchemaRegistry, RegisteredSchema) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "id": id })))
            .mount(&server)
            .await;
        let sr = SchemaRegistry::new(server.uri());
        sr.register_avro::<Order>("orders-value")
            .await
            .expect("register");
        let schema = sr.cached_subject("orders-value").expect("cached");
        (server, sr, (*schema).clone())
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

    /// The bug the JSON transcode carries and the lanes cannot: through serde a JSON document's
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

    #[test]
    fn the_lane_traits_reach_the_same_bytes() {
        struct Wire(Vec<u8>);

        impl Serialized for Wire {
            type Error = KafkaError;

            fn wire_bytes<'a>(&'a self, _buf: &'a mut BytesMut) -> Result<&'a [u8], KafkaError> {
                Ok(&self.0)
            }
        }

        let mut buf = BytesMut::new();
        encode(
            &Order {
                id: 1,
                item: "x".to_owned(),
            },
            &mut buf,
        )
        .expect("encode");
        let wire = Wire(buf.to_vec());
        let mut lend = BytesMut::new();
        assert_eq!(wire.wire_bytes(&mut lend).expect("lend"), &buf[..]);
    }

    #[tokio::test]
    async fn a_framed_delivery_resolves_the_writer_schema_onto_the_reader() {
        let (_server, sr, schema) = registry_with_order(7).await;

        // Written by a producer on the old schema, read by a consumer on the new one.
        let mut buf = BytesMut::new();
        encode(
            &Order {
                id: 5,
                item: "anvil".to_owned(),
            },
            &mut buf,
        )
        .expect("encode");
        let framed = OutgoingFrame::new(schema.id(), buf.to_vec());
        let mut wire = BytesMut::new();
        let payload = framed.wire_bytes(&mut wire).expect("infallible").to_vec();

        let frame = IncomingFrame::from_payload(&payload).expect("framed");
        let read: OrderV2 = decode_framed(&sr, &frame).await.expect("resolve");
        assert_eq!(
            read,
            OrderV2 {
                id: 5,
                item: "anvil".to_owned(),
                note: String::new(),
            },
        );
    }

    #[tokio::test]
    async fn a_non_avro_id_names_the_format_it_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/3"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": "{\"type\":\"object\"}",
                "schemaType": "JSON",
            })))
            .mount(&server)
            .await;
        let sr = SchemaRegistry::new(server.uri());
        let payload = [0u8, 0, 0, 0, 3, 1];

        let frame = IncomingFrame::from_payload(&payload).expect("framed");
        let err = decode_framed::<Order>(&sr, &frame)
            .await
            .expect_err("not avro");
        assert!(err.to_string().contains("not Avro"));
    }

    #[tokio::test]
    async fn a_subject_frames_with_the_id_of_its_own_schema() {
        let (_server, sr, schema) = registry_with_order(11).await;

        let subject = Subject::<Order>::register(&sr, "orders-value")
            .await
            .expect("register");
        assert_eq!(subject.schema_id(), schema.id());

        let frame = subject
            .frame(&Order {
                id: 1,
                item: "x".to_owned(),
            })
            .expect("frame");
        assert_eq!(frame.schema_id(), schema.id());
        assert_eq!(decode::<Order>(frame.datum()).expect("decode").id, 1);
    }

    #[tokio::test]
    async fn json_avro_json_roundtrips() {
        let (_server, sr, schema) = registry_with_order(7).await;
        let json = br#"{"id":42,"item":"anvil"}"#;

        let datum = json_to_avro(&sr, &schema, json).expect("encode");
        assert_ne!(datum.as_slice(), json, "the wire form is Avro, not JSON");

        let back = avro_to_json(&sr, &schema, &datum).expect("decode");
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
        let (_server, sr, schema) = registry_with_order(7).await;
        let err = json_to_avro(&sr, &schema, br#"{"id":"not-a-number"}"#)
            .expect_err("a document violating the schema must fail");
        assert!(matches!(err, KafkaError::SchemaRegistry(_)));
    }
}
