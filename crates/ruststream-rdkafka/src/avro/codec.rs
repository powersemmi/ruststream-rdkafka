//! The Avro codec: a schema held by the codec, and serde types riding it unchanged.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::writer::datum::GenericDatumWriter;
use bytes::BufMut;
use ruststream::BytesMut;
use ruststream::codec::{Codec, CodecError};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::KafkaError;
use crate::schema_registry::{SchemaPrefetch, SchemaRegistry, WIRE_MAGIC, parse_envelope};

/// One schema prepared for both directions, built once and borrowed for the process's life.
///
/// `apache-avro` resolves a schema's names when a datum writer or reader is built, and both
/// borrow the schema they were built from. Leaking the schema is what lets the built pair be
/// kept: the set is bounded by the schemas a service actually meets, and building either per
/// message would put a schema-driven format's setup cost on every delivery.
struct Prepared {
    /// The schema the wire carries: what a value is resolved against on the way out, and what a
    /// datum is read with on the way in.
    schema: &'static Schema,
    writer: GenericDatumWriter<'static>,
    reader: GenericDatumReader<'static>,
}

impl Prepared {
    fn build(writer_schema: Schema, reader_schema: Option<Schema>) -> Result<Self, KafkaError> {
        let schema: &'static Schema = Box::leak(Box::new(writer_schema));
        let reader_schema: Option<&'static Schema> =
            reader_schema.map(|reader| &*Box::leak(Box::new(reader)));
        Ok(Self {
            schema,
            writer: GenericDatumWriter::builder(schema)
                .build()
                .map_err(KafkaError::wire_format)?,
            reader: GenericDatumReader::builder(schema)
                .maybe_reader_schema(reader_schema)
                .build()
                .map_err(KafkaError::wire_format)?,
        })
    }
}

/// Where the codec's schema comes from.
///
/// An enum rather than a registry plus an optional local schema: the two sources answer
/// different questions (one schema for every message, or one per delivery named by the wire),
/// they put a different thing on the wire, and no combination of them is meaningful.
#[derive(Clone)]
enum SchemaSource {
    /// One schema, known at construction. A bare datum on the wire, and no registry anywhere.
    Local(&'static Prepared),
    /// The subject a message is published under, and the envelope's id on the way back.
    Registry {
        registry: SchemaRegistry,
        subject: String,
        /// The schemas met so far, keyed by registry id (the subject's own included). Keyed per
        /// codec rather than per process, because an id means nothing outside the registry that
        /// issued it - and shared by clones, because the mount machinery clones a codec once per
        /// registration and those are the same codec by every measure that matters here.
        prepared: Arc<Mutex<HashMap<u32, &'static Prepared>>>,
    },
}

/// An Avro [`Codec`]: the schema lives in the codec, and the messages riding it are ordinary
/// serde types.
///
/// Avro is a schema-driven format with a serde front end, so the codec position is where it
/// belongs: `apache-avro` converts a value through its own dynamic representation in both
/// directions, guided by the schema the codec holds, and asks nothing of the message type beyond
/// `Serialize` / `Deserialize`. A handler on this codec is an ordinary handler and its models are
/// ordinary structs - no derive of ours, no wire type in the signature.
///
/// # The two schema sources
///
/// [`local`](Self::local) pins one schema at construction. The wire carries a bare Avro datum,
/// nothing reaches a registry, and the whole path is I/O-free - which is what a topic with a
/// fixed schema, and every unit test, wants.
///
/// [`registry`](Self::registry) speaks the Confluent wire format: the encode side frames with the
/// id its subject holds, and the decode side reads each delivery with the writer schema that
/// delivery's envelope names, so a producer still on an older version stays readable. It needs a
/// [`SchemaPrefetch`] attached to the broker, because those lookups are `async` and a
/// [`Codec`] is not; see that type for why the split falls where it does.
///
/// # Schema evolution
///
/// Reading a datum with its writer schema recovers what the writer wrote, and no more. A field
/// the writer never had is filled from a *reader* schema's default, which is Avro's own schema
/// resolution, so a consumer that has moved ahead of its producers names the schema it expects
/// with [`resolve_onto`](Self::resolve_onto). Without it a model with a field the writer lacks
/// fails to deserialize, which is the honest outcome: the value is genuinely not on the wire.
///
/// # Examples
///
/// ```
/// use apache_avro::AvroSchema;
/// use ruststream::codec::Codec;
/// use ruststream_rdkafka::avro::AvroCodec;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
/// struct Order {
///     id: i64,
///     item: String,
/// }
///
/// # fn check() -> Result<(), Box<dyn std::error::Error>> {
/// let codec = AvroCodec::local(Order::get_schema())?;
///
/// let order = Order { id: 7, item: "anvil".to_owned() };
/// let bytes = codec.encode(&order)?;
/// assert_eq!(codec.decode::<Order>(&bytes)?, order);
/// # Ok(())
/// # }
/// # check().unwrap();
/// ```
#[derive(Clone)]
pub struct AvroCodec {
    source: SchemaSource,
    /// The schema the reading side expects, when it is not the writer's. Applies to decoding
    /// only: encoding always writes the schema the wire is supposed to carry.
    reader_schema: Option<Arc<Schema>>,
}

impl std::fmt::Debug for AvroCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("AvroCodec");
        match &self.source {
            SchemaSource::Local(_) => out.field("source", &"local"),
            SchemaSource::Registry { subject, .. } => out.field("subject", subject),
        };
        out.finish_non_exhaustive()
    }
}

impl AvroCodec {
    /// A codec over one schema, known here and used for every message.
    ///
    /// Nothing on this path reaches a registry and nothing frames an envelope: the wire carries
    /// the datum alone. Pass the schema the wire is supposed to carry - `T::get_schema()` when
    /// the model defines it, a parsed `.avsc` when the schema is the source of truth.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::WireFormat`] when the schema's own names cannot be resolved (a
    /// reference to a record the schema does not define).
    pub fn local(schema: Schema) -> Result<Self, KafkaError> {
        Ok(Self {
            source: SchemaSource::Local(Box::leak(Box::new(Prepared::build(schema, None)?))),
            reader_schema: None,
        })
    }

    /// A codec on the Confluent wire format, publishing under `subject`.
    ///
    /// Construction is synchronous and does no I/O: it records `subject` on `prefetch`, which
    /// resolves it when the broker connects. Reading takes the writer schema from whatever
    /// envelope arrives, which the same prefetch resolves on the delivery path.
    ///
    /// # Panics
    ///
    /// Panics when the prefetch's internal mutex is poisoned, which requires a prior panic
    /// inside it (an invariant violation, not an operational failure).
    #[must_use]
    pub fn registry(prefetch: &SchemaPrefetch, subject: impl Into<String>) -> Self {
        let subject = subject.into();
        prefetch.record_subject(&subject);
        Self {
            source: SchemaSource::Registry {
                registry: prefetch.registry().clone(),
                subject,
                prepared: Arc::new(Mutex::new(HashMap::new())),
            },
            reader_schema: None,
        }
    }

    /// Reads every delivery onto `schema` instead of the schema it was written with, so Avro's
    /// resolution fills the fields this consumer expects and the writer never wrote.
    ///
    /// This is the reader schema of Avro's resolution rules, and it is the decode side only: a
    /// publish writes what the wire is supposed to carry, which is the writer schema either way.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::WireFormat`] when the schema cannot be prepared against the local
    /// writer schema. A registry codec's writer schemas arrive later, so a pairing that cannot
    /// resolve is reported by the decode that meets it.
    pub fn resolve_onto(mut self, schema: Schema) -> Result<Self, KafkaError> {
        let schema = Arc::new(schema);
        if let SchemaSource::Local(prepared) = &self.source {
            // Rebuilt against the local writer schema so a reader that cannot resolve against it
            // is reported here, at wiring time, rather than on the first delivery.
            self.source = SchemaSource::Local(Box::leak(Box::new(Prepared::build(
                prepared.schema.clone(),
                Some((*schema).clone()),
            )?)));
        }
        self.reader_schema = Some(schema);
        Ok(self)
    }

    /// The prepared pair for a registry schema id, built once per codec.
    fn prepared_for(&self, id: u32) -> Result<&'static Prepared, CodecError> {
        let SchemaSource::Registry {
            registry, prepared, ..
        } = &self.source
        else {
            unreachable!("a local codec never looks a schema id up");
        };
        if let Some(found) = prepared
            .lock()
            .expect("prepared schema cache mutex poisoned")
            .get(&id)
        {
            return Ok(found);
        }
        // Only ever the cache: the prefetch resolved this id on the delivery path (or the
        // subject at connect), and a synchronous codec cannot await a miss without blocking a
        // runtime worker - so a miss is reported, and the subscription's decode failure policy
        // settles the delivery.
        let schema = registry.cached_schema(id).ok_or_else(|| {
            decode_error(format!(
                "no schema is cached for id {id}: the delivery's writer schema was not resolved \
                 before it reached the codec. Attach a SchemaPrefetch to the broker \
                 (KafkaBroker::schema_prefetch), and check the registry is reachable - a \
                 synchronous codec cannot fetch it here"
            ))
        })?;
        let parsed = registry.parsed_avro(&schema).map_err(decode_source)?;
        let built: &'static Prepared = Box::leak(Box::new(
            Prepared::build((*parsed).clone(), self.reader_schema.as_deref().cloned())
                .map_err(decode_source)?,
        ));
        Ok(prepared
            .lock()
            .expect("prepared schema cache mutex poisoned")
            .entry(id)
            .or_insert(built))
    }
}

impl Codec for AvroCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        let mut buf = BytesMut::new();
        let prepared = match &self.source {
            SchemaSource::Local(prepared) => *prepared,
            SchemaSource::Registry {
                registry, subject, ..
            } => {
                let schema = registry.cached_subject(subject).ok_or_else(|| {
                    encode_error(format!(
                        "no schema is cached for subject {subject:?}: it was not resolved at \
                         startup. Attach the SchemaPrefetch this codec was built from to the \
                         broker (KafkaBroker::schema_prefetch), so connect resolves it"
                    ))
                })?;
                buf.put_u8(WIRE_MAGIC);
                buf.put_u32(schema.id());
                self.prepared_for(schema.id())
                    .map_err(|err| encode_error(err.to_string()))?
            }
        };
        // Through the format's dynamic value: serde produces it, the schema resolves it, and the
        // writer emits the bytes. Nothing here needs anything of `T` but `Serialize`, which is
        // what lets one codec instance carry a schema for message types it has never seen.
        let value = apache_avro::to_value(value)
            .map_err(encode_source)?
            .resolve(prepared.schema)
            .map_err(|err| {
                encode_error(format!(
                    "the value does not fit the schema this codec writes: {err}"
                ))
            })?;
        prepared
            .writer
            .write_value(&mut (&mut buf).writer(), value)
            .map_err(encode_source)?;
        Ok(buf)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        let (prepared, mut datum) = match &self.source {
            SchemaSource::Local(prepared) => (*prepared, bytes),
            SchemaSource::Registry { .. } => {
                let (id, datum) = parse_envelope(bytes).ok_or_else(|| {
                    decode_error(format!(
                        "the delivery does not carry the Confluent wire format (a zero magic \
                         byte and a 4-byte schema id), so no writer schema can be named for it; \
                         its first bytes are {:02x?}",
                        &bytes[..bytes.len().min(8)],
                    ))
                })?;
                (self.prepared_for(id)?, datum)
            }
        };
        let value = prepared
            .reader
            .read_value(&mut datum)
            .map_err(decode_source)?;
        apache_avro::from_value(&value).map_err(decode_source)
    }
}

fn encode_error(message: String) -> CodecError {
    CodecError::Encode(Box::new(KafkaError::malformed(message)))
}

fn encode_source(err: impl std::error::Error + Send + Sync + 'static) -> CodecError {
    CodecError::Encode(Box::new(err))
}

fn decode_error(message: String) -> CodecError {
    CodecError::Decode(Box::new(KafkaError::malformed(message)))
}

fn decode_source(err: impl std::error::Error + Send + Sync + 'static) -> CodecError {
    CodecError::Decode(Box::new(err))
}

#[cfg(test)]
mod tests {
    use apache_avro::AvroSchema;
    use serde::Deserialize;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::schema_registry::SchemaType;

    #[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
    #[serde(rename = "CodecOrder")]
    struct Order {
        id: i64,
        item: String,
    }

    /// The same record one version on: a field added, carrying the default that lets a reader
    /// project an older writer's datum onto it.
    #[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
    #[serde(rename = "CodecOrder")]
    struct OrderV2 {
        id: i64,
        item: String,
        #[avro(default = r#""none""#)]
        note: String,
    }

    fn order() -> Order {
        Order {
            id: 42,
            item: "anvil".to_owned(),
        }
    }

    #[test]
    fn a_local_schema_round_trips_with_no_registry() {
        let codec = AvroCodec::local(Order::get_schema()).expect("local");
        let bytes = codec.encode(&order()).expect("encode");

        assert_ne!(
            &bytes[..],
            br#"{"id":42,"item":"anvil"}"#,
            "the wire is Avro"
        );
        assert_eq!(codec.decode::<Order>(&bytes).expect("decode"), order());
    }

    #[test]
    fn a_local_codec_writes_no_envelope() {
        let codec = AvroCodec::local(Order::get_schema()).expect("local");
        let bytes = codec.encode(&order()).expect("encode");

        assert!(
            parse_envelope(&bytes).is_none() || bytes[0] != WIRE_MAGIC,
            "a local codec puts a bare datum on the wire",
        );
    }

    #[test]
    fn a_value_the_schema_does_not_describe_is_refused() {
        #[derive(Serialize)]
        struct Other {
            nope: String,
        }
        let codec = AvroCodec::local(Order::get_schema()).expect("local");

        let err = codec
            .encode(&Other {
                nope: "x".to_owned(),
            })
            .expect_err("a value of another shape");
        assert!(err.to_string().contains("does not fit the schema"));
    }

    #[test]
    fn a_reader_schema_fills_what_the_writer_never_wrote() {
        let writing = AvroCodec::local(Order::get_schema()).expect("writer");
        let bytes = writing.encode(&order()).expect("encode");

        // Without a reader schema the field is genuinely absent, and serde says so.
        let reading = AvroCodec::local(Order::get_schema()).expect("reader");
        assert!(reading.decode::<OrderV2>(&bytes).is_err());

        let resolving = AvroCodec::local(Order::get_schema())
            .expect("reader")
            .resolve_onto(OrderV2::get_schema())
            .expect("resolve");
        assert_eq!(
            resolving.decode::<OrderV2>(&bytes).expect("resolve"),
            OrderV2 {
                id: 42,
                item: "anvil".to_owned(),
                note: "none".to_owned(),
            },
        );
    }

    async fn registry_with_order(id: u32) -> (MockServer, SchemaPrefetch) {
        let server = MockServer::start().await;
        let definition = crate::avro::schema_json(&Order::get_schema()).expect("json");
        Mock::given(method("GET"))
            .and(path("/subjects/orders-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": id,
                "version": 1,
                "schema": definition,
                "schemaType": "AVRO",
            })))
            .mount(&server)
            .await;
        let prefetch = SchemaPrefetch::new(SchemaRegistry::new(server.uri()));
        (server, prefetch)
    }

    #[tokio::test]
    async fn a_registry_codec_frames_with_the_subjects_id() {
        let (_server, prefetch) = registry_with_order(11).await;
        let codec = AvroCodec::registry(&prefetch, "orders-value");
        prefetch.warm_subjects().await.expect("warm");

        let bytes = codec.encode(&order()).expect("encode");
        let (id, datum) = parse_envelope(&bytes).expect("the wire format");
        assert_eq!(id, 11);
        assert!(!datum.is_empty());

        // And it reads its own frame back through the id the envelope names.
        assert_eq!(codec.decode::<Order>(&bytes).expect("decode"), order());
    }

    #[tokio::test]
    async fn an_unresolved_subject_names_the_attachment_it_is_missing() {
        let (_server, prefetch) = registry_with_order(11).await;
        // Never warmed: the codec was built, but the prefetch never reached a broker's connect.
        let codec = AvroCodec::registry(&prefetch, "orders-value");

        let err = codec.encode(&order()).expect_err("cold subject");
        assert!(err.to_string().contains("schema_prefetch"));
    }

    #[tokio::test]
    async fn an_unresolved_id_names_the_attachment_it_is_missing() {
        let (_server, prefetch) = registry_with_order(11).await;
        let codec = AvroCodec::registry(&prefetch, "orders-value");
        let framed = [0u8, 0, 0, 0, 99, 1];

        let err = codec.decode::<Order>(&framed).expect_err("cold id");
        assert!(err.to_string().contains("no schema is cached for id 99"));
        assert!(err.to_string().contains("SchemaPrefetch"));
    }

    #[tokio::test]
    async fn an_unframed_delivery_on_a_registry_codec_is_refused() {
        let (_server, prefetch) = registry_with_order(11).await;
        let codec = AvroCodec::registry(&prefetch, "orders-value");

        let err = codec
            .decode::<Order>(br#"{"id":42}"#)
            .expect_err("not framed");
        assert!(err.to_string().contains("Confluent wire format"));
    }

    #[tokio::test]
    async fn the_prefetch_resolves_a_deliverys_writer_schema() {
        let (server, prefetch) = registry_with_order(11).await;
        let definition = crate::avro::schema_json(&Order::get_schema()).expect("json");
        Mock::given(method("GET"))
            .and(path("/schemas/ids/11"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": definition,
                "schemaType": "AVRO",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let codec = AvroCodec::registry(&prefetch, "orders-value");
        let framed = [0u8, 0, 0, 0, 11, 84, 10, 97, 110, 118, 105, 108];

        // Cold: the sync codec refuses rather than reaching for the network.
        assert!(codec.decode::<Order>(&framed).is_err());

        // The async edge resolves it, and the same bytes now decode with no I/O of their own.
        prefetch.warm_delivery(&framed).await;
        assert_eq!(codec.decode::<Order>(&framed).expect("decode"), order());
    }

    #[tokio::test]
    async fn a_missing_subject_fails_startup_rather_than_the_first_publish() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/absent-value/versions/latest"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let prefetch = SchemaPrefetch::new(SchemaRegistry::new(server.uri()));
        let _codec = AvroCodec::registry(&prefetch, "absent-value");

        let err = prefetch.warm_subjects().await.expect_err("absent subject");
        assert!(err.to_string().contains("absent-value"));
    }

    /// The one shape a registry schema cannot take from a Rust model: `apache-avro` maps an
    /// unsigned Rust integer onto its own fixed logical type, which does not resolve against the
    /// `long` a registered schema declares. It is reported, not silently mis-encoded.
    #[tokio::test]
    async fn an_unsigned_field_against_a_long_schema_is_reported() {
        #[derive(Serialize)]
        struct Wide {
            id: u64,
            item: String,
        }
        let (_server, prefetch) = registry_with_order(11).await;
        let codec = AvroCodec::registry(&prefetch, "orders-value");
        prefetch.warm_subjects().await.expect("warm");

        let err = codec
            .encode(&Wide {
                id: 7,
                item: "anvil".to_owned(),
            })
            .expect_err("u64 against a long schema");
        assert!(err.to_string().contains("does not fit the schema"));
    }

    #[tokio::test]
    async fn the_registered_flavor_is_checked_before_it_is_parsed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/json-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 3,
                "version": 1,
                "schema": "{\"type\":\"object\"}",
                "schemaType": "JSON",
            })))
            .mount(&server)
            .await;
        let prefetch = SchemaPrefetch::new(SchemaRegistry::new(server.uri()));
        let codec = AvroCodec::registry(&prefetch, "json-value");
        prefetch.warm_subjects().await.expect("warm");

        // A JSON Schema document is not an Avro schema, and the parse says so.
        let err = codec.encode(&order()).expect_err("not an avro schema");
        assert!(matches!(err, CodecError::Encode(_)));
        let _ = SchemaType::Json;
    }
}
