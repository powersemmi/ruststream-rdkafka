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

use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use ruststream::runtime::{Outgoing, PublishLayer, PublishNext, PublishPipeline};
use ruststream::{BytesMut, OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::ConnectedKafkaBroker;
use crate::error::KafkaError;
use crate::frame::{IncomingFrame, OutgoingFrame};
use crate::publisher::{KafkaPublish, KafkaPublisher};
use crate::schema_registry::{
    RegisteredSchema, SchemaRegistry, SchemaType, SubjectMap, SubjectStrategy, WIRE_MAGIC,
    parse_envelope,
};

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
    decode_datum(frame.schema_id(), frame.datum())
}

/// Reads a Confluent-framed delivery into a `prost`-generated message, straight off the wire.
///
/// This is the function a generated type names in `#[wire(decode = ..)]`, so its handlers take
/// the message itself and never see the envelope.
///
/// Everything the envelope carries in front of the message is parseable without asking anyone:
/// the magic byte, the four-byte id, and the message-index path. The id is not needed to read
/// the message at all, because Protobuf's compatibility model is the wire format's own - fields
/// are tag-addressed, a field the reader does not know is kept as an unknown field and one the
/// writer never wrote takes its default - so a delivery decodes against the reader's own
/// generated type. That is what lets this be a synchronous function on the decode lane, where
/// Avro needs the writer schema its id names and therefore an `await`.
///
/// A payload with no envelope is read as a bare message, so a topic that some producers frame
/// and others do not still decodes. What is lost by not looking at the id is the *governance*
/// half - which schema version a delivery claims - and a handler that needs it takes
/// [`IncomingFrame`] and calls [`decode_framed`] instead.
///
/// # Errors
///
/// Returns [`KafkaError::WireFormat`] when the message-index path is truncated, or when the
/// bytes after it are not a message of `T`.
///
/// # Examples
///
/// ```
/// use ruststream::prelude::*;
///
/// #[derive(Clone, PartialEq, prost::Message, Deserialized, Serialized)]
/// #[wire(
///     encode = ::prost::Message::encode,
///     decode = ruststream_rdkafka::protobuf::decode_confluent
/// )]
/// struct Order {
///     #[prost(int64, tag = "1")]
///     id: i64,
/// }
///
/// # fn check() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// // Schema id 42, the compact `[0]` index path, then `id = 7`.
/// let payload = [0x00, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x08, 0x07];
/// let order = Order::from_payload(&payload)?;
/// assert_eq!(order.id, 7);
/// # Ok(())
/// # }
/// # check().unwrap();
/// ```
pub fn decode_confluent<T: prost::Message + Default>(payload: &[u8]) -> Result<T, KafkaError> {
    match parse_envelope(payload) {
        Some((schema_id, datum)) => decode_datum(schema_id, datum),
        // An unframed payload is a bare message: no envelope means no index path either.
        None => T::decode(payload).map_err(KafkaError::wire_format),
    }
}

/// The shared half of both decode entry points: step over the index path, read the message.
fn decode_datum<T: prost::Message + Default>(
    schema_id: u32,
    datum: &[u8],
) -> Result<T, KafkaError> {
    let message = skip_indexes(datum).ok_or_else(|| {
        KafkaError::malformed(format!(
            "the message-index path of schema id {schema_id} is truncated, so the Protobuf \
             message after it cannot be found",
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
/// let registry = SchemaRegistry::new("http://localhost:8081");
/// let subject = protobuf::Subject::<Confirmation>::resolve(
///     &registry,
///     "confirmations-value",
///     "acme.Confirmation",
/// )
/// .await?;
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
    /// [`AvroCodec`](crate::avro::AvroCodec) resolves the id of the *producer's own* schema: a
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
}

impl<T: prost::Message> Subject<T> {
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

/// One topic's message-index prefix, kept with the schema id it was computed against so a
/// re-registered subject recomputes it instead of framing behind a stale path.
#[derive(Clone)]
struct IndexPrefix {
    schema_id: u32,
    bytes: Arc<[u8]>,
}

/// The prefixes resolved so far, by topic.
type IndexPrefixes = Arc<Mutex<HashMap<String, IndexPrefix>>>;

/// What framing one payload came to, so each caller can decide what its own surface makes of
/// the three ways there was nothing to do.
#[derive(Debug)]
pub(crate) enum Framing {
    /// The envelope and the message-index path, in front of the bytes that came in.
    Framed(Vec<u8>),
    /// The payload already carried an envelope, so it was left alone.
    AlreadyFramed,
    /// The registry holds no subject for the destination.
    NoSubject,
    /// The destination's subject holds another format's schema.
    OtherFormat(SchemaType),
}

/// The shared framing engine: which subject a topic publishes under, which message of its
/// schema, and the envelope in front of a `prost` message's own bytes.
///
/// Two public surfaces sit on this and differ only in what they make of a destination the
/// registry does not describe: [`ProtobufFrame`], an app-wide layer that sees every topic and so
/// passes those through, and [`KafkaFramedPublish`](crate::KafkaFramedPublish), a mount-site
/// policy that was pointed at one destination and so refuses.
#[derive(Clone)]
pub(crate) struct ProtobufFraming {
    subjects: SubjectMap,
    /// Per-topic message names (fully qualified), for schemas declaring several messages.
    messages: HashMap<String, String>,
    prefixes: IndexPrefixes,
}

impl fmt::Debug for ProtobufFraming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtobufFraming")
            .field("strategy", &self.subjects.strategy())
            .field("subjects", self.subjects.pins())
            .field("messages", &self.messages)
            .finish_non_exhaustive()
    }
}

impl ProtobufFraming {
    pub(crate) fn new(registry: SchemaRegistry) -> Self {
        Self {
            subjects: SubjectMap::new(registry),
            messages: HashMap::new(),
            prefixes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn set_strategy(&mut self, strategy: SubjectStrategy) {
        self.subjects.set_strategy(strategy);
    }

    pub(crate) fn pin_subject(&mut self, topic: String, subject: String) {
        self.subjects.pin(topic, subject);
    }

    pub(crate) fn pin_message(&mut self, topic: String, message: String) {
        self.messages.insert(topic, message);
    }

    /// The subject `topic` publishes under, for a diagnostic that has to name it.
    pub(crate) fn subject_for(&self, topic: &str) -> String {
        self.subjects.subject_for(topic)
    }

    /// Frames `payload` for `topic`, or says why there was nothing to frame.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable, and
    /// [`KafkaError::InvalidOptions`] when the subject's schema does not declare the message
    /// the index path must address.
    pub(crate) async fn frame(&self, topic: &str, payload: &[u8]) -> Result<Framing, KafkaError> {
        // A bare `prost` message opens with a field tag, whose field number is at least 1, so
        // its first byte is at least 0x08 and never the zero magic byte. An envelope here is
        // therefore a payload something already framed, not a message that happens to look
        // like one.
        if parse_envelope(payload).is_some() {
            return Ok(Framing::AlreadyFramed);
        }
        let Some(schema) = self.subjects.schema_for(topic).await? else {
            return Ok(Framing::NoSubject);
        };
        if schema.schema_type() != SchemaType::Protobuf {
            return Ok(Framing::OtherFormat(schema.schema_type()));
        }
        let prefix = self.prefix_for(topic, &schema)?;

        let mut framed = Vec::with_capacity(1 + 4 + prefix.len() + payload.len());
        framed.push(WIRE_MAGIC);
        framed.extend_from_slice(&schema.id().to_be_bytes());
        framed.extend_from_slice(&prefix);
        framed.extend_from_slice(payload);
        Ok(Framing::Framed(framed))
    }

    /// The message-index prefix `topic`'s payloads ride behind, resolved once per schema id.
    fn prefix_for(&self, topic: &str, schema: &RegisteredSchema) -> Result<Arc<[u8]>, KafkaError> {
        if let Some(prefix) = self
            .prefixes
            .lock()
            .expect("message-index prefix mutex poisoned")
            .get(topic)
            && prefix.schema_id == schema.id()
        {
            return Ok(Arc::clone(&prefix.bytes));
        }
        let pool = self.subjects.registry().parsed_proto(schema)?;
        let descriptor = match self.messages.get(topic) {
            Some(name) => pool.get_message_by_name(name).ok_or_else(|| {
                KafkaError::InvalidOptions(format!(
                    "message {name:?}, pinned for topic {topic:?}, is not in its subject's \
                     schema (use the fully qualified name, package included)",
                ))
            })?,
            None => registry_file(&pool)
                .and_then(|file| file.messages().next())
                .ok_or_else(|| {
                    KafkaError::InvalidOptions(format!(
                        "the subject of topic {topic:?} holds a Protobuf schema declaring no \
                         messages, so there is no message-index path to frame with",
                    ))
                })?,
        };
        let indexes = message_indexes(&descriptor).ok_or_else(|| {
            KafkaError::InvalidOptions(format!(
                "message {:?} is not declared by the registered schema file of topic {topic:?}",
                descriptor.full_name(),
            ))
        })?;
        let bytes: Arc<[u8]> = Arc::from(encode_indexes(&indexes));
        self.prefixes
            .lock()
            .expect("message-index prefix mutex poisoned")
            .insert(
                topic.to_owned(),
                IndexPrefix {
                    schema_id: schema.id(),
                    bytes: Arc::clone(&bytes),
                },
            );
        Ok(bytes)
    }
}

/// Publish middleware putting the Confluent envelope on a `prost` message's own bytes.
///
/// It is the encode-side half of the pair [`decode_confluent`] completes on the read side: with
/// both in place a Protobuf handler returns the message and nothing else. Added app-wide with
/// `RustStream::publish_layer`.
///
/// The envelope needs a schema id, and a value cannot fetch one: `Serialized::wire_bytes` is
/// synchronous and has only `&self` to work with. The publish path can, because it knows the
/// destination topic, which is exactly what names the subject - so the value writes bare
/// Protobuf through the core's own `#[wire(encode = ::prost::Message::encode)]` and this layer
/// puts the id and the message-index path in front of it. That division is why nothing here
/// needs a process-wide registry singleton keyed by message type, which is the other way a
/// value could have reached an id.
///
/// # What it touches, and what it leaves alone
///
/// A publish is framed only when its topic's subject is registered **and holds a Protobuf
/// schema**. Everything else passes through untouched: a topic with no subject (mixed
/// registry/plain topologies need no configuration), and a topic whose subject is Avro or JSON,
/// whose payload some other path already framed. That is what lets one app carry an Avro codec,
/// a JSON codec and Protobuf lanes at once with this layer installed app-wide.
///
/// A payload that already carries an envelope also passes through, so a handler that framed its
/// own message with [`Subject::frame`] is not framed twice. The check is exact rather than a
/// guess: a bare `prost` message begins with a field tag, whose field number is at least 1, so
/// its first byte is never the zero magic byte.
///
/// It is an alternative to [`SchemaFrame`](crate::SchemaFrame) rather than a companion. That
/// layer's contract is "the payload is a JSON document, transcode it to the subject's flavor";
/// this one's is "the payload is already the subject's datum, put the envelope on". Install one
/// or the other.
///
/// # The layer does not see a `publish(..)` reply
///
/// A handler's outgoing message reaches this layer when it leaves through an
/// [`Out`](ruststream::runtime::Out) slot, and **not** when it is returned as the reply of a
/// `publish(..)` mount. That is the core's own division rather than a gap here: a byte-for-byte
/// reply goes straight to its paired publisher, deliberately, so a value that owns its bytes
/// leaves exactly as it wrote them.
///
/// So there are two shapes, and which one to reach for is a question of where the framing should
/// be declared. A handler that replies takes
/// [`KafkaPublish::framed`](crate::KafkaPublish::framed) at its mount site, which frames on the
/// way through the reply's own publisher and keeps the body a plain
/// `async fn confirm(order: &Order) -> Confirmation`. This layer is for setting framing once for
/// a whole app, where publishes leave through slots and through publishers the mount sites never
/// name.
///
/// The two compose rather than conflict: whichever runs first frames the payload, and the other
/// sees an envelope already there and leaves it alone.
///
/// # Examples
///
/// ```no_run
/// use ruststream::runtime::{AppInfo, RustStream};
/// use ruststream_rdkafka::{ProtobufFrame, SchemaRegistry};
///
/// let registry = SchemaRegistry::new("http://localhost:8081");
/// let app = RustStream::new(AppInfo::new("orders", "1.0.0"))
///     .publish_layer(ProtobufFrame::new(registry));
/// # let _ = app;
/// ```
#[derive(Clone, Debug)]
pub struct ProtobufFrame {
    framing: ProtobufFraming,
}

impl ProtobufFrame {
    /// Builds the framing middleware over `registry`. Subjects default to the Confluent
    /// `TopicName` strategy (`{topic}-value`).
    #[must_use]
    pub fn new(registry: SchemaRegistry) -> Self {
        Self {
            framing: ProtobufFraming::new(registry),
        }
    }

    /// How destination topics map onto subjects (default: [`SubjectStrategy::TopicName`]).
    #[must_use]
    pub fn subject_strategy(mut self, strategy: SubjectStrategy) -> Self {
        self.framing.set_strategy(strategy);
        self
    }

    /// Pins `topic`'s subject explicitly, overriding the strategy.
    #[must_use]
    pub fn subject(mut self, topic: impl Into<String>, subject: impl Into<String>) -> Self {
        self.framing.pin_subject(topic.into(), subject.into());
        self
    }

    /// Pins the message `topic`'s payloads are, by fully qualified name (package included).
    ///
    /// The message-index path says which message of the schema was written, so it has to be the
    /// one the publishing type actually is. Without this pin the layer takes the schema's first
    /// top-level message, which is the common case and the one Confluent optimises to a single
    /// zero byte - so a single-message `.proto`, and a multi-message one whose published type
    /// is declared first, need no pin. Anything else does: naming the wrong message puts a
    /// mis-addressed path on the wire, which consumers report as a decode failure.
    #[must_use]
    pub fn message(mut self, topic: impl Into<String>, message: impl Into<String>) -> Self {
        self.framing.pin_message(topic.into(), message.into());
        self
    }
}

impl PublishLayer for ProtobufFrame {
    async fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Every "nothing to do" is a pass-through here: this layer sees every topic the app
        // publishes to, most of which are not Protobuf, so refusing one would be refusing the
        // ordinary case. The mount-site policy, pointed at a single destination, refuses instead.
        if let Framing::Framed(framed) = self.framing.frame(out.name(), out.payload()).await? {
            let payload = out.payload_mut();
            payload.clear();
            payload.extend_from_slice(&framed);
        }
        next.run(out).await
    }
}

/// The publish policy of a Protobuf producer that frames what it sends: pure declaration, no
/// connection, no publish surface, like every other policy here.
///
/// Built with [`KafkaPublish::framed`](crate::KafkaPublish::framed), and named where a policy is
/// named - which is what makes the short reply form work:
///
/// ```no_run
/// # use ruststream::prelude::*;
/// # use ruststream::runtime::{AppInfo, Reply, RustStream};
/// # use ruststream_rdkafka::{KafkaBroker, KafkaPublish, SchemaRegistry};
/// # #[derive(Clone, PartialEq, prost::Message, Deserialized)]
/// # #[wire(decode = ruststream_rdkafka::protobuf::decode_confluent)]
/// # struct Order { #[prost(int64, tag = "1")] id: i64 }
/// #[derive(Clone, PartialEq, prost::Message, Serialized, Outgoing)]
/// #[wire(encode = ::prost::Message::encode)]
/// struct Confirmation {
///     #[prost(int64, tag = "1")]
///     id: i64,
///     #[prost(bool, tag = "2")]
///     accepted: bool,
/// }
///
/// #[subscriber("orders", publish("confirmations"))]
/// async fn confirm(order: &Order) -> Confirmation {
///     Confirmation { id: order.id, accepted: true }
/// }
///
/// let registry = SchemaRegistry::new("http://localhost:8081");
/// let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
///     .with_broker(KafkaBroker::new(["localhost:9092"]), |b| {
///         b.include(confirm).out(Reply, KafkaPublish::framed(&registry));
///     });
/// # let _ = app;
/// ```
///
/// The handler returns its reply and nothing else: no slot parameter, no `publish().await`, no
/// error branch in the body. The reply type declares no destination of its own - its `Outgoing`
/// derive names nothing, so the address comes from the `publish("confirmations")` clause - and
/// beyond that it carries only `#[derive(Serialized)]` and the encode half of `#[wire(..)]`.
///
/// The same policy works wherever else one is named, an [`Out`](ruststream::runtime::Out) slot
/// included, so a handler that publishes several messages frames them the same way.
///
/// # It refuses what the layer passes through
///
/// Naming this policy at a mount site **declares** that destination registry-backed, so a
/// subject the registry does not hold, or one holding another format's schema, fails the publish
/// and says which subject and which format. That is the opposite of
/// [`ProtobufFrame`]'s reaction, deliberately and for the same reason the Avro codec's
/// `MissingSubject::Refuse` is its default: the layer sees every topic an app publishes to and
/// most of them are not Protobuf, so passing those through is the ordinary case, while this
/// policy was pointed at one destination and silence there would put records on the topic that no
/// registry-backed consumer can read.
///
/// A payload that already carries an envelope is still left alone, so a handler that framed its
/// own message, or a `ProtobufFrame` earlier in the pipeline, does not frame it twice.
///
/// # When the subject is resolved
///
/// On the first publish to each destination, and cached from then on. It cannot be at startup:
/// [`PublishPolicy::pair`] is where a policy could do I/O, and the destination topic is not known
/// there - it comes from the mount's `publish(..)` clause, or from the call site of a slot
/// publish, neither of which a policy is handed. So a missing subject surfaces as a failed
/// publish rather than a failed startup, which the handler's failure policy then settles.
#[derive(Clone, Debug)]
#[must_use]
pub struct KafkaFramedPublish {
    publish: KafkaPublish,
    framing: ProtobufFraming,
}

impl KafkaFramedPublish {
    /// Frames what `publish` sends, resolving subjects through `registry`.
    ///
    /// [`KafkaPublish::framed`](crate::KafkaPublish::framed) is the short form of this over
    /// [`KafkaPublish::default`]; take this one to carry the producer settings a configured
    /// [`KafkaPublish`] holds.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use ruststream_rdkafka::{KafkaFramedPublish, KafkaPublish, SchemaRegistry};
    ///
    /// let registry = SchemaRegistry::new("http://localhost:8081");
    /// let policy = KafkaFramedPublish::over(
    ///     KafkaPublish::default().queue_timeout(Duration::from_secs(5)),
    ///     &registry,
    /// );
    /// # let _ = policy;
    /// ```
    pub fn over(publish: KafkaPublish, registry: &SchemaRegistry) -> Self {
        Self {
            publish,
            framing: ProtobufFraming::new(registry.clone()),
        }
    }

    /// How destination topics map onto subjects (default: [`SubjectStrategy::TopicName`]).
    pub fn subject_strategy(mut self, strategy: SubjectStrategy) -> Self {
        self.framing.set_strategy(strategy);
        self
    }

    /// Pins `topic`'s subject explicitly, overriding the strategy.
    pub fn subject(mut self, topic: impl Into<String>, subject: impl Into<String>) -> Self {
        self.framing.pin_subject(topic.into(), subject.into());
        self
    }

    /// Pins the message `topic`'s payloads are, by fully qualified name (package included).
    ///
    /// The default is the schema's first top-level message, which Confluent optimises to a
    /// single zero byte; see [`ProtobufFrame::message`] for when that is not the one you want.
    pub fn message(mut self, topic: impl Into<String>, message: impl Into<String>) -> Self {
        self.framing.pin_message(topic.into(), message.into());
        self
    }
}

impl KafkaFramedPublish {
    /// Splits the policy into producer settings and framing, for a broker that mints its own
    /// publisher (the in-process test broker does).
    pub(crate) fn into_parts(self) -> (KafkaPublish, ProtobufFraming) {
        (self.publish, self.framing)
    }
}

impl PublishPolicy<ConnectedKafkaBroker> for KafkaFramedPublish {
    type Live = KafkaFramedPublisher<KafkaPublisher>;

    async fn pair(self, connected: &ConnectedKafkaBroker) -> Result<Self::Live, PairError> {
        Ok(KafkaFramedPublisher::new(
            self.publish.pair(connected).await?,
            self.framing,
        ))
    }
}

/// The live half of [`KafkaFramedPublish`]: a publisher that puts the Confluent envelope on each
/// message before it goes on the topic.
///
/// Generic over the publisher underneath, so a mount site naming
/// [`KafkaPublish::framed`](crate::KafkaPublish::framed) compiles unchanged against the real
/// broker and against the in-process [`KafkaTestBroker`](crate::testing::KafkaTestBroker) - the
/// same promise [`KafkaPublish`] itself makes.
#[derive(Clone, Debug)]
pub struct KafkaFramedPublisher<P = KafkaPublisher> {
    inner: P,
    framing: ProtobufFraming,
}

impl<P> KafkaFramedPublisher<P> {
    pub(crate) const fn new(inner: P, framing: ProtobufFraming) -> Self {
        Self { inner, framing }
    }
}

impl<P: Publisher<Error = KafkaError> + Send + Sync> Publisher for KafkaFramedPublisher<P> {
    type Error = KafkaError;
    // Framing is a payload transform: the record's own settings are whatever the publisher
    // underneath speaks, passed through untouched.
    type Options = P::Options;

    /// Frames `msg` by its destination topic's subject and publishes it.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable or holds no
    /// subject for the destination, [`KafkaError::InvalidOptions`] when the subject holds
    /// another format's schema or does not declare the message the index path must address, and
    /// whatever [`KafkaPublisher`] reports for the send itself.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe, for the same reason [`KafkaPublisher::publish`] is not.
    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let framed = match self.framing.frame(msg.name(), msg.payload()).await? {
            Framing::Framed(framed) => framed,
            Framing::AlreadyFramed => return self.inner.publish(msg, options).await,
            Framing::NoSubject => {
                return Err(KafkaError::SchemaRegistry(
                    format!(
                        "topic {:?} publishes through a framing publisher, but the registry \
                         holds no subject {:?} to frame its records with. Register the schema, \
                         point the policy at the subject that carries it with `.subject(..)`, or \
                         publish through a plain KafkaPublish if this topic is not \
                         registry-backed.",
                        msg.name(),
                        self.framing.subject_for(msg.name()),
                    )
                    .into(),
                ));
            }
            Framing::OtherFormat(format) => {
                return Err(KafkaError::InvalidOptions(format!(
                    "subject {:?}, which topic {:?} publishes under, holds a {format:?} schema; a \
                     framing publisher writes Protobuf. Use the codec for that format, or the \
                     transcoding SchemaFrame layer.",
                    self.framing.subject_for(msg.name()),
                    msg.name(),
                )));
            }
        };
        let framed = OutgoingMessage::new(msg.name(), &framed).with_headers(msg.headers().clone());
        self.inner.publish(framed, options).await
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
        let registry = SchemaRegistry::new(server.uri());
        registry
            .register("orders-value", SchemaType::Protobuf, ORDERS_PROTO)
            .await
            .expect("register");
        let schema = registry.cached_subject("orders-value").expect("cached");
        (server, registry, (*schema).clone())
    }

    #[tokio::test]
    async fn json_protobuf_json_roundtrips_with_index_paths() {
        let (_server, registry, schema) = registry_with_orders(5).await;
        let json = br#"{"id":42,"item":"anvil"}"#;

        // Order is the second top-level message: a real index path, not the compact zero.
        let datum = json_to_protobuf(&registry, &schema, Some("acme.Order"), json).expect("encode");
        assert_ne!(datum[0], 0, "a real index path must be encoded");

        let back = protobuf_to_json(&registry, &schema, &datum).expect("decode");
        let value: serde_json::Value = serde_json::from_slice(&back).expect("json");
        assert_eq!(value["id"], 42);
        assert_eq!(value["item"], "anvil");
    }

    #[tokio::test]
    async fn nested_messages_address_by_index_path() {
        let (_server, registry, schema) = registry_with_orders(6).await;
        let json = br#"{"sku":"SKU-1","quantity":3}"#;

        let datum =
            json_to_protobuf(&registry, &schema, Some("acme.Order.Line"), json).expect("encode");
        let back = protobuf_to_json(&registry, &schema, &datum).expect("decode");
        let value: serde_json::Value = serde_json::from_slice(&back).expect("json");
        assert_eq!(value["sku"], "SKU-1");
        assert_eq!(value["quantity"], 3);
    }

    #[tokio::test]
    async fn unknown_messages_error_clearly() {
        let (_server, registry, schema) = registry_with_orders(7).await;
        let err = json_to_protobuf(&registry, &schema, Some("acme.Missing"), b"{}")
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
        let (_server, registry, schema) = registry_with_orders(9).await;

        let subject = Subject::<Order>::resolve(&registry, "orders-value", "acme.Order")
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
        let json = protobuf_to_json(&registry, &schema, frame.datum()).expect("transcode");
        let value: serde_json::Value = serde_json::from_slice(&json).expect("json");
        assert_eq!(value["id"], 42);
        assert_eq!(value["item"], "anvil");
    }

    #[tokio::test]
    async fn a_framed_delivery_decodes_with_no_registry() {
        let (_server, registry, _) = registry_with_orders(9).await;
        let subject = Subject::<Order>::resolve(&registry, "orders-value", "acme.Order")
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

    /// The shape a service declares: `prost` writes the bytes, this crate reads the envelope.
    #[derive(
        Clone, PartialEq, prost::Message, ruststream::Deserialized, ruststream::Serialized,
    )]
    #[wire(encode = ::prost::Message::encode, decode = decode_confluent)]
    struct LaneOrder {
        #[prost(int64, tag = "1")]
        id: i64,
        #[prost(string, tag = "2")]
        item: String,
    }

    #[test]
    fn a_generated_type_reads_a_framed_delivery_with_no_registry() {
        use ruststream::runtime::{Deserialized, Serialized};

        let order = LaneOrder {
            id: 42,
            item: "anvil".to_owned(),
        };
        // The encode half is bare prost: no envelope, no index path, no id.
        let mut buf = BytesMut::new();
        let bare = order.wire_bytes(&mut buf).expect("encode").to_vec();
        assert_eq!(bare, order.encode_to_vec());

        // The framing a publish layer would add: id 9, the compact `[0]` path.
        let mut framed = vec![0x00, 0x00, 0x00, 0x00, 0x09, 0x00];
        framed.extend_from_slice(&bare);
        assert_eq!(LaneOrder::from_payload(&framed).expect("decode"), order);

        // And the same bytes with no envelope at all, for a topic only some producers frame.
        assert_eq!(LaneOrder::from_payload(&bare).expect("decode"), order);
    }

    #[test]
    fn a_bare_message_never_looks_like_an_envelope() {
        // The discriminator the publish layer relies on: a field number is at least 1, so the
        // first byte of a non-empty message is at least 0x08 and never the zero magic byte.
        for id in [i64::MIN, -1, 0, 1, i64::MAX] {
            let bare = LaneOrder {
                id,
                item: String::new(),
            }
            .encode_to_vec();
            assert!(parse_envelope(&bare).is_none(), "id {id} looked framed");
        }
    }

    /// Builds the shared framing engine both public surfaces sit on.
    fn framing(registry: SchemaRegistry, message: Option<&str>) -> ProtobufFraming {
        let mut framing = ProtobufFraming::new(registry);
        if let Some(message) = message {
            framing.pin_message("orders".to_owned(), message.to_owned());
        }
        framing
    }

    #[tokio::test]
    async fn the_prefix_addresses_the_pinned_message() {
        let (_server, registry, _) = registry_with_orders(11).await;
        let framing = framing(registry, Some("acme.Order"));

        let Framing::Framed(framed) = framing.frame("orders", &[0x08, 0x07]).await.expect("frame")
        else {
            panic!("a Protobuf subject frames");
        };
        let (id, datum) = parse_envelope(&framed).expect("framed");
        assert_eq!(id, 11);
        // `acme.Order` is the second top-level message, so the path is real, not the compact
        // zero, and the message bytes follow it unchanged.
        let (indexes, message) = decode_indexes(datum).expect("decodes");
        assert_eq!(indexes, vec![1]);
        assert_eq!(message, &[0x08, 0x07]);
    }

    #[tokio::test]
    async fn an_unpinned_topic_takes_the_first_top_level_message() {
        let (_server, registry, _) = registry_with_orders(12).await;
        let framing = framing(registry, None);

        let Framing::Framed(framed) = framing.frame("orders", &[0x08, 0x07]).await.expect("frame")
        else {
            panic!("a Protobuf subject frames");
        };
        // `acme.Ignored` is declared first, and `[0]` is Confluent's single-zero form.
        assert_eq!(parse_envelope(&framed).expect("framed").1[0], 0);
    }

    #[tokio::test]
    async fn an_already_framed_payload_is_left_alone() {
        let (_server, registry, _) = registry_with_orders(14).await;
        let framing = framing(registry, Some("acme.Order"));

        let already = [0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x08, 0x07];
        assert!(matches!(
            framing.frame("orders", &already).await.expect("frame"),
            Framing::AlreadyFramed,
        ));
    }

    #[tokio::test]
    async fn a_destination_the_registry_does_not_know_is_reported_not_guessed() {
        let (server, registry, _) = registry_with_orders(15).await;
        Mock::given(method("GET"))
            .and(path("/subjects/unknown-value/versions/latest"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": 40401,
                "message": "Subject not found",
            })))
            .mount(&server)
            .await;
        let framing = framing(registry, None);

        assert!(matches!(
            framing
                .frame("unknown", &[0x08, 0x07])
                .await
                .expect("frame"),
            Framing::NoSubject,
        ));
    }

    #[tokio::test]
    async fn a_message_the_schema_does_not_declare_is_named_in_the_error() {
        let (_server, registry, _) = registry_with_orders(13).await;
        let framing = framing(registry, Some("acme.Missing"));

        let err = framing
            .frame("orders", &[0x08, 0x07])
            .await
            .expect_err("unknown message");
        let message = err.to_string();
        assert!(message.contains("acme.Missing"), "{message}");
        assert!(message.contains("orders"), "{message}");
    }
}
