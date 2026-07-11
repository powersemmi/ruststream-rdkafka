//! The Confluent Schema Registry client and the wire-format envelope.
//!
//! Payloads on registry-backed topics carry the Confluent wire format: a zero magic byte, a
//! big-endian 4-byte schema id, then the encoded datum. [`SchemaRegistry`] is the async
//! client the schema-aware codecs build on; because the core `Codec` is synchronous, the hot
//! paths never call it directly - the subscriber prefetches schemas by id into the client's
//! cache before deliveries reach the codec (see
//! [`KafkaBroker::schema_registry`](crate::KafkaBroker::schema_registry)), and the encode
//! side resolves its subject once at startup ([`register`](SchemaRegistry::register) /
//! [`warm`](SchemaRegistry::warm)), mirroring Confluent's production guidance of not
//! auto-registering schemas from producers.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use serde::Deserialize;

pub use schemars::JsonSchema;

use crate::error::KafkaError;

/// The zero magic byte opening every Confluent-framed payload.
const WIRE_MAGIC: u8 = 0;

/// The schema flavors the registry stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaType {
    /// An Avro schema (the registry default; older registries omit the type field entirely).
    Avro,
    /// A Protobuf schema, stored as `.proto` source text.
    Protobuf,
    /// A JSON Schema document.
    Json,
}

impl SchemaType {
    fn as_api(self) -> &'static str {
        match self {
            Self::Avro => "AVRO",
            Self::Protobuf => "PROTOBUF",
            Self::Json => "JSON",
        }
    }

    fn from_api(value: Option<&str>) -> Self {
        match value {
            Some("PROTOBUF") => Self::Protobuf,
            Some("JSON") => Self::Json,
            _ => Self::Avro,
        }
    }
}

/// One schema as the registry knows it: its id, flavor, and definition text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredSchema {
    id: u32,
    schema_type: SchemaType,
    definition: String,
}

impl RegisteredSchema {
    /// The registry-assigned schema id (what the wire-format envelope carries).
    #[must_use]
    pub fn id(&self) -> u32 {
        self.id
    }

    /// The schema flavor.
    #[must_use]
    pub fn schema_type(&self) -> SchemaType {
        self.schema_type
    }

    /// The schema definition: Avro or JSON Schema as JSON text, Protobuf as `.proto` source.
    #[must_use]
    pub fn definition(&self) -> &str {
        &self.definition
    }
}

/// How a topic maps onto registry subjects, per Confluent's naming strategies.
///
/// The strategies name the subject a value schema is registered under;
/// [`subject`](Self::subject) builds the name from the destination topic and the record's
/// fully qualified name (unused by [`TopicName`](Self::TopicName)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SubjectStrategy {
    /// `{topic}-value`: one value schema per topic (the Confluent default).
    #[default]
    TopicName,
    /// `{record}`: one subject per record type, shared across topics.
    RecordName,
    /// `{topic}-{record}`: several record types per topic, each its own subject.
    TopicRecordName,
}

impl SubjectStrategy {
    /// The value subject for `topic` and `record` under this strategy.
    #[must_use]
    pub fn subject(self, topic: &str, record: &str) -> String {
        match self {
            Self::TopicName => format!("{topic}-value"),
            Self::RecordName => record.to_owned(),
            Self::TopicRecordName => format!("{topic}-{record}"),
        }
    }
}

enum Auth {
    None,
    Basic { user: String, password: String },
    Bearer(String),
}

struct RegistryInner {
    base_url: String,
    http: reqwest::Client,
    auth: Auth,
    by_id: Mutex<HashMap<u32, Arc<RegisteredSchema>>>,
    by_subject: Mutex<HashMap<String, u32>>,
}

/// The async Confluent Schema Registry client, shared by clones.
///
/// Construction is synchronous and does no I/O (the broker contract's lazy-startup shape);
/// every lookup caches, so a schema id or subject resolves over the network once per process.
///
/// # Examples
///
/// ```no_run
/// use ruststream_rdkafka::{KafkaBroker, SchemaRegistry};
///
/// let sr = SchemaRegistry::new("http://localhost:8081").basic_auth("svc", "secret");
/// // Consuming: the broker prefetches schemas by id before deliveries reach the codec.
/// let broker = KafkaBroker::new(["localhost:9092"]).schema_registry(sr.clone());
/// # let _ = broker;
/// ```
#[derive(Clone)]
pub struct SchemaRegistry {
    inner: Arc<RegistryInner>,
}

impl fmt::Debug for SchemaRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaRegistry")
            .field("base_url", &self.inner.base_url)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct SchemaByIdResponse {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
}

#[derive(Deserialize)]
struct RegisterResponse {
    id: u32,
}

#[derive(Deserialize)]
struct LatestVersionResponse {
    id: u32,
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
}

impl SchemaRegistry {
    /// Builds a client for the registry at `base_url` (for example `http://localhost:8081`).
    /// No I/O happens until the first lookup.
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        let mut base_url = base_url.into();
        while base_url.ends_with('/') {
            base_url.pop();
        }
        Self {
            inner: Arc::new(RegistryInner {
                base_url,
                http: reqwest::Client::new(),
                auth: Auth::None,
                by_id: Mutex::new(HashMap::new()),
                by_subject: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// HTTP basic authentication for every registry request. Configure before handing the
    /// client out: clones share the configuration they were made from.
    #[must_use]
    pub fn basic_auth(self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.with_auth(Auth::Basic {
            user: user.into(),
            password: password.into(),
        })
    }

    /// Bearer-token authentication for every registry request. Configure before handing the
    /// client out.
    #[must_use]
    pub fn bearer_token(self, token: impl Into<String>) -> Self {
        self.with_auth(Auth::Bearer(token.into()))
    }

    fn with_auth(self, auth: Auth) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                base_url: self.inner.base_url.clone(),
                http: self.inner.http.clone(),
                auth,
                by_id: Mutex::new(HashMap::new()),
                by_subject: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{path}", self.inner.base_url);
        let request = self.inner.http.request(method, url);
        match &self.inner.auth {
            Auth::None => request,
            Auth::Basic { user, password } => request.basic_auth(user, Some(password)),
            Auth::Bearer(token) => request.bearer_auth(token),
        }
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, KafkaError> {
        let response = self
            .request(reqwest::Method::GET, path)
            .send()
            .await
            .map_err(KafkaError::schema_registry)?;
        let response = response
            .error_for_status()
            .map_err(KafkaError::schema_registry)?;
        response.json().await.map_err(KafkaError::schema_registry)
    }

    /// The schema registered under `id`, from the cache or the registry.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable, rejects the
    /// request, or does not know the id.
    ///
    /// # Panics
    ///
    /// Panics when the internal cache mutex is poisoned, which requires a prior panic inside
    /// the client (an invariant violation, not an operational failure).
    pub async fn schema_by_id(&self, id: u32) -> Result<Arc<RegisteredSchema>, KafkaError> {
        if let Some(schema) = self.cached_schema(id) {
            return Ok(schema);
        }
        let fetched: SchemaByIdResponse = self.get_json(&format!("/schemas/ids/{id}")).await?;
        let schema = Arc::new(RegisteredSchema {
            id,
            schema_type: SchemaType::from_api(fetched.schema_type.as_deref()),
            definition: fetched.schema,
        });
        self.inner
            .by_id
            .lock()
            .expect("schema cache mutex poisoned")
            .insert(id, Arc::clone(&schema));
        Ok(schema)
    }

    /// Registers `definition` under `subject` (idempotent registry-side: an identical schema
    /// keeps its id) and caches the subject's id for the sync encode path.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable, rejects the
    /// request, or refuses the schema (compatibility, syntax).
    ///
    /// # Panics
    ///
    /// Panics when the internal cache mutex is poisoned, which requires a prior panic inside
    /// the client (an invariant violation, not an operational failure).
    pub async fn register(
        &self,
        subject: &str,
        schema_type: SchemaType,
        definition: impl Into<String>,
    ) -> Result<u32, KafkaError> {
        let definition = definition.into();
        let body = serde_json::json!({
            "schema": definition,
            "schemaType": schema_type.as_api(),
        });
        let response = self
            .request(
                reqwest::Method::POST,
                &format!("/subjects/{subject}/versions"),
            )
            .json(&body)
            .send()
            .await
            .map_err(KafkaError::schema_registry)?
            .error_for_status()
            .map_err(KafkaError::schema_registry)?;
        let registered: RegisterResponse =
            response.json().await.map_err(KafkaError::schema_registry)?;
        let schema = Arc::new(RegisteredSchema {
            id: registered.id,
            schema_type,
            definition,
        });
        self.cache(subject, &schema);
        Ok(registered.id)
    }

    /// Resolves `subject`'s latest version and caches it for the sync encode path - the
    /// warm-only alternative to [`register`](Self::register) when producers must not create
    /// schemas.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable, rejects the
    /// request, or has no such subject.
    ///
    /// # Panics
    ///
    /// Panics when the internal cache mutex is poisoned, which requires a prior panic inside
    /// the client (an invariant violation, not an operational failure).
    pub async fn warm(&self, subject: &str) -> Result<Arc<RegisteredSchema>, KafkaError> {
        let fetched: LatestVersionResponse = self
            .get_json(&format!("/subjects/{subject}/versions/latest"))
            .await?;
        let schema = Arc::new(RegisteredSchema {
            id: fetched.id,
            schema_type: SchemaType::from_api(fetched.schema_type.as_deref()),
            definition: fetched.schema,
        });
        self.cache(subject, &schema);
        Ok(schema)
    }

    fn cache(&self, subject: &str, schema: &Arc<RegisteredSchema>) {
        self.inner
            .by_id
            .lock()
            .expect("schema cache mutex poisoned")
            .insert(schema.id, Arc::clone(schema));
        self.inner
            .by_subject
            .lock()
            .expect("subject cache mutex poisoned")
            .insert(subject.to_owned(), schema.id);
    }

    /// The cached schema for `id`, when a prefetch or an earlier lookup resolved it. The sync
    /// decode path reads this; the subscriber prefetch keeps it warm.
    ///
    /// # Panics
    ///
    /// Panics when the internal cache mutex is poisoned, which requires a prior panic inside
    /// the client (an invariant violation, not an operational failure).
    #[must_use]
    pub fn cached_schema(&self, id: u32) -> Option<Arc<RegisteredSchema>> {
        self.inner
            .by_id
            .lock()
            .expect("schema cache mutex poisoned")
            .get(&id)
            .cloned()
    }

    /// The cached schema for `subject`, when [`register`](Self::register) or
    /// [`warm`](Self::warm) resolved it at startup. The sync encode path reads this.
    ///
    /// # Panics
    ///
    /// Panics when the internal cache mutex is poisoned, which requires a prior panic inside
    /// the client (an invariant violation, not an operational failure).
    #[must_use]
    pub fn cached_subject(&self, subject: &str) -> Option<Arc<RegisteredSchema>> {
        let id = *self
            .inner
            .by_subject
            .lock()
            .expect("subject cache mutex poisoned")
            .get(subject)?;
        self.cached_schema(id)
    }

    /// Registers the JSON Schema generated from `T` (via schemars) under `subject` - the
    /// typed shorthand for [`register`](Self::register) with a hand-written document.
    ///
    /// # Errors
    ///
    /// As [`register`](Self::register).
    pub async fn register_json<T: JsonSchema>(&self, subject: &str) -> Result<u32, KafkaError> {
        let schema = schemars::SchemaGenerator::default().into_root_schema_for::<T>();
        let definition = serde_json::to_string(&schema).map_err(KafkaError::schema_registry)?;
        self.register(subject, SchemaType::Json, definition).await
    }

    /// Transcodes a Confluent-framed payload to plain JSON, so deliveries reach handlers in
    /// the default codec's format: the JSON flavor loses its envelope, Avro and Protobuf
    /// datums (with their features enabled) convert through their registry schema. Non-framed
    /// payloads pass through untouched (`None`); a failed fetch or transcode warns and passes
    /// the payload through, so the handler's decode failure carries the delivery's fate.
    pub(crate) async fn incoming_to_json(&self, payload: &[u8]) -> Option<Vec<u8>> {
        let (id, datum) = parse_envelope(payload)?;
        let schema = match self.schema_by_id(id).await {
            Ok(schema) => schema,
            Err(err) => {
                tracing::warn!(
                    target: "ruststream_rdkafka",
                    schema_id = id,
                    error = %err,
                    "schema fetch failed; the delivery passes through un-transcoded until \
                     the registry recovers",
                );
                return None;
            }
        };
        let transcoded = incoming_datum_to_json(self, &schema, datum);
        match transcoded {
            Ok(json) => Some(json),
            Err(err) => {
                tracing::warn!(
                    target: "ruststream_rdkafka",
                    schema_id = id,
                    error = %err,
                    "transcoding a framed delivery failed; the delivery passes through \
                     un-transcoded",
                );
                None
            }
        }
    }

    /// Frames a plain-JSON payload for the wire in `format` under `subject`, resolving the
    /// subject lazily (cache first, then the registry) - the publish path is async, so no
    /// startup ceremony is required when the subject already exists.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the subject cannot be resolved,
    /// [`KafkaError::InvalidOptions`] when the resolved schema's flavor does not match
    /// `format` or the format's cargo feature is off, and the format's own conversion errors.
    pub(crate) async fn json_to_wire(
        &self,
        format: SchemaFormat,
        subject: &str,
        payload: &[u8],
    ) -> Result<Vec<u8>, KafkaError> {
        let schema = match self.cached_subject(subject) {
            Some(schema) => schema,
            None => self.warm(subject).await?,
        };
        let expected = format.schema_type();
        if schema.schema_type != expected {
            return Err(KafkaError::InvalidOptions(format!(
                "subject {subject:?} holds a {:?} schema, but the publisher is configured \
                 for {expected:?}",
                schema.schema_type,
            )));
        }
        let datum = outgoing_json_to_datum(self, format, &schema, payload)?;
        Ok(encode_envelope(schema.id, &datum))
    }
}

/// Converts an incoming framed datum to plain JSON by its schema's flavor. The `avro` and
/// `protobuf` features extend the match; without them those flavors error.
fn incoming_datum_to_json(
    registry: &SchemaRegistry,
    schema: &RegisteredSchema,
    datum: &[u8],
) -> Result<Vec<u8>, KafkaError> {
    let _ = registry;
    match schema.schema_type {
        SchemaType::Json => Ok(datum.to_vec()),
        other => Err(KafkaError::InvalidOptions(format!(
            "schema id {} is {other:?}, but the matching cargo feature is not enabled on \
             ruststream-rdkafka; enable it to consume this topic",
            schema.id,
        ))),
    }
}

/// Converts an outgoing plain-JSON payload to the wire datum for `format`. The `avro` and
/// `protobuf` features extend the match; without them those formats error.
fn outgoing_json_to_datum(
    registry: &SchemaRegistry,
    format: SchemaFormat,
    schema: &RegisteredSchema,
    payload: &[u8],
) -> Result<Vec<u8>, KafkaError> {
    let _ = (registry, schema);
    match format {
        SchemaFormat::Json => Ok(payload.to_vec()),
        other => Err(KafkaError::InvalidOptions(format!(
            "publishing as {other:?} needs the matching cargo feature enabled on \
             ruststream-rdkafka",
        ))),
    }
}

/// The wire format a schema-registry publisher frames its payloads in.
///
/// Picked on the publisher
/// ([`KafkaPublisher::schema_format`](crate::KafkaPublisher::schema_format)); the handler and
/// codec side stays plain JSON either way - the publish path transcodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaFormat {
    /// Frame the JSON document as-is (the registry's JSON Schema flavor).
    Json,
    /// Transcode to an Avro datum against the subject's schema (needs the `avro` feature).
    Avro,
    /// Transcode to a Protobuf message against the subject's schema (needs the `protobuf`
    /// feature).
    Protobuf,
}

impl SchemaFormat {
    fn schema_type(self) -> SchemaType {
        match self {
            Self::Json => SchemaType::Json,
            Self::Avro => SchemaType::Avro,
            Self::Protobuf => SchemaType::Protobuf,
        }
    }
}

/// Splits a Confluent-framed payload into its schema id and the datum after the envelope.
///
/// Returns `None` for payloads that do not carry the wire format (no zero magic byte or too
/// short), which callers treat as "not registry-framed" rather than an error.
///
/// # Panics
///
/// Does not panic: the id slice is length-checked before conversion (the internal `expect`
/// is unreachable).
#[must_use]
pub fn parse_envelope(payload: &[u8]) -> Option<(u32, &[u8])> {
    let (&magic, rest) = payload.split_first()?;
    if magic != WIRE_MAGIC || rest.len() < 4 {
        return None;
    }
    let (id_bytes, datum) = rest.split_at(4);
    let id = u32::from_be_bytes(id_bytes.try_into().expect("4-byte slice"));
    Some((id, datum))
}

/// Frames `datum` with the Confluent wire format for `schema_id`.
#[must_use]
pub fn encode_envelope(schema_id: u32, datum: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(1 + 4 + datum.len());
    framed.push(WIRE_MAGIC);
    framed.extend_from_slice(&schema_id.to_be_bytes());
    framed.extend_from_slice(datum);
    framed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrips() {
        let framed = encode_envelope(1234, b"datum");
        let (id, datum) = parse_envelope(&framed).expect("framed");
        assert_eq!(id, 1234);
        assert_eq!(datum, b"datum");
    }

    #[test]
    fn non_framed_payloads_are_recognized() {
        assert!(parse_envelope(b"").is_none());
        assert!(parse_envelope(b"{\"json\":1}").is_none());
        assert!(parse_envelope(&[0, 1, 2]).is_none(), "short id");
    }

    #[test]
    fn subject_strategies_name_confluent_style() {
        assert_eq!(
            SubjectStrategy::TopicName.subject("orders", "com.acme.Order"),
            "orders-value"
        );
        assert_eq!(
            SubjectStrategy::RecordName.subject("orders", "com.acme.Order"),
            "com.acme.Order"
        );
        assert_eq!(
            SubjectStrategy::TopicRecordName.subject("orders", "com.acme.Order"),
            "orders-com.acme.Order"
        );
    }
}
