//! The Confluent Schema Registry client, the wire-format envelope, and the registry
//! middleware.
//!
//! Payloads on registry-backed topics carry the Confluent wire format: a zero magic byte, a
//! big-endian 4-byte schema id, then the encoded datum. The registry integrates as
//! middleware on the broker's async edges, so handlers and publishers keep speaking plain
//! JSON through the default codec:
//!
//! - Consuming: [`KafkaBroker::schema_registry`](crate::KafkaBroker::schema_registry)
//!   transcodes framed deliveries to plain JSON on the subscription's delivery path.
//! - Publishing: [`SchemaFrame`], added app-wide with `RustStream::publish_layer`, frames
//!   outgoing JSON for the wire by the destination subject's registered flavor.
//!
//! [`SchemaRegistry`] is the shared async client both sides resolve and cache schemas
//! through. Registering schemas stays an explicit step ([`register`](SchemaRegistry::register)
//! and the typed shorthands), mirroring Confluent's production guidance of not
//! auto-registering schemas from producers.

use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::fmt;
use std::sync::{Arc, Mutex};

use ruststream::Publisher;
use ruststream::runtime::{Outgoing, PublishLayer, PublishNext, PublishPipeline};
use serde::Deserialize;

pub use schemars::JsonSchema;

use crate::error::KafkaError;

/// The zero magic byte opening every Confluent-framed payload.
pub(crate) const WIRE_MAGIC: u8 = 0;

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
    #[cfg(feature = "avro")]
    parsed_avro: Mutex<HashMap<u32, Arc<apache_avro::Schema>>>,
    #[cfg(feature = "protobuf")]
    parsed_proto: Mutex<HashMap<u32, Arc<prost_reflect::DescriptorPool>>>,
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
/// // Consuming: subscriptions transcode framed deliveries to plain JSON through the client.
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
                #[cfg(feature = "avro")]
                parsed_avro: Mutex::new(HashMap::new()),
                #[cfg(feature = "protobuf")]
                parsed_proto: Mutex::new(HashMap::new()),
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
                #[cfg(feature = "avro")]
                parsed_avro: Mutex::new(HashMap::new()),
                #[cfg(feature = "protobuf")]
                parsed_proto: Mutex::new(HashMap::new()),
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
    /// keeps its id) and caches the subject, so the framing middleware resolves it locally.
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

    /// Resolves `subject`'s latest version and caches it - the warm-only alternative to
    /// [`register`](Self::register) when producers must not create schemas, and a startup
    /// probe that a required subject exists (the framing middleware itself resolves subjects
    /// lazily).
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

    /// Like [`warm`](Self::warm), but a missing subject resolves to `None` instead of an
    /// error - the framing middleware treats it as "this topic is not registry-backed".
    pub(crate) async fn latest(
        &self,
        subject: &str,
    ) -> Result<Option<Arc<RegisteredSchema>>, KafkaError> {
        let response = self
            .request(
                reqwest::Method::GET,
                &format!("/subjects/{subject}/versions/latest"),
            )
            .send()
            .await
            .map_err(KafkaError::schema_registry)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = response
            .error_for_status()
            .map_err(KafkaError::schema_registry)?;
        let fetched: LatestVersionResponse =
            response.json().await.map_err(KafkaError::schema_registry)?;
        let schema = Arc::new(RegisteredSchema {
            id: fetched.id,
            schema_type: SchemaType::from_api(fetched.schema_type.as_deref()),
            definition: fetched.schema,
        });
        self.cache(subject, &schema);
        Ok(Some(schema))
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

    /// The cached schema for `id`, when an earlier lookup resolved it (the subscription's
    /// transcoding keeps ids it has seen warm).
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

    /// The cached schema for `subject`, when [`register`](Self::register), [`warm`](Self::warm),
    /// or the framing middleware's lazy resolution cached it.
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

    /// Registers the Avro schema derived from `T` under `subject` - the typed shorthand for
    /// [`register`](Self::register) with `T::get_schema()`.
    ///
    /// # Errors
    ///
    /// As [`register`](Self::register).
    #[cfg(feature = "avro")]
    pub async fn register_avro<T: apache_avro::AvroSchema>(
        &self,
        subject: &str,
    ) -> Result<u32, KafkaError> {
        let schema = T::get_schema();
        self.register(subject, SchemaType::Avro, schema.canonical_form())
            .await
    }

    /// The parsed Avro schema for a cached registered schema, parsed once and shared.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the definition is not valid Avro.
    ///
    /// # Panics
    ///
    /// Panics when the internal cache mutex is poisoned, which requires a prior panic inside
    /// the client (an invariant violation, not an operational failure).
    #[cfg(feature = "avro")]
    pub(crate) fn parsed_avro(
        &self,
        schema: &RegisteredSchema,
    ) -> Result<Arc<apache_avro::Schema>, KafkaError> {
        if let Some(parsed) = self
            .inner
            .parsed_avro
            .lock()
            .expect("parsed schema cache mutex poisoned")
            .get(&schema.id)
        {
            return Ok(Arc::clone(parsed));
        }
        let parsed = Arc::new(
            apache_avro::Schema::parse_str(&schema.definition)
                .map_err(KafkaError::schema_registry)?,
        );
        self.inner
            .parsed_avro
            .lock()
            .expect("parsed schema cache mutex poisoned")
            .insert(schema.id, Arc::clone(&parsed));
        Ok(parsed)
    }

    /// The compiled descriptor pool for a cached Protobuf schema, compiled once (protox,
    /// with the well-known types available) and shared. Registry schema references are not
    /// resolved; a schema importing anything beyond the well-known types fails to compile.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the source does not compile.
    ///
    /// # Panics
    ///
    /// Panics when the internal cache mutex is poisoned, which requires a prior panic inside
    /// the client (an invariant violation, not an operational failure).
    #[cfg(feature = "protobuf")]
    pub(crate) fn parsed_proto(
        &self,
        schema: &RegisteredSchema,
    ) -> Result<Arc<prost_reflect::DescriptorPool>, KafkaError> {
        if let Some(parsed) = self
            .inner
            .parsed_proto
            .lock()
            .expect("descriptor cache mutex poisoned")
            .get(&schema.id)
        {
            return Ok(Arc::clone(parsed));
        }
        let pool = compile_proto(&schema.definition).map_err(KafkaError::schema_registry)?;
        let pool = Arc::new(pool);
        self.inner
            .parsed_proto
            .lock()
            .expect("descriptor cache mutex poisoned")
            .insert(schema.id, Arc::clone(&pool));
        Ok(pool)
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
}

#[cfg(feature = "protobuf")]
fn compile_proto(source: &str) -> Result<prost_reflect::DescriptorPool, protox::Error> {
    use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};

    /// Serves the registry schema as a single in-memory file next to the well-known types.
    struct Single(String);
    impl FileResolver for Single {
        fn open_file(&self, name: &str) -> Result<File, protox::Error> {
            if name == "registry.proto" {
                File::from_source(name, &self.0)
            } else {
                Err(protox::Error::file_not_found(name))
            }
        }
    }

    let mut resolver = ChainFileResolver::new();
    resolver.add(GoogleFileResolver::new());
    resolver.add(Single(source.to_owned()));
    let mut compiler = protox::Compiler::with_file_resolver(resolver);
    compiler.include_imports(true);
    compiler.open_file("registry.proto")?;
    let set = compiler.file_descriptor_set();
    prost_reflect::DescriptorPool::from_file_descriptor_set(set)
        .map_err(|err| protox::Error::new(err.to_string()))
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
        #[cfg(feature = "avro")]
        SchemaType::Avro => crate::avro::avro_to_json(registry, schema, datum),
        #[cfg(feature = "protobuf")]
        SchemaType::Protobuf => crate::protobuf::protobuf_to_json(registry, schema, datum),
        #[allow(unreachable_patterns)] // the disabled-format arms
        other => Err(KafkaError::InvalidOptions(format!(
            "schema id {} is {other:?}, but the matching cargo feature is not enabled on \
             ruststream-rdkafka; enable it to consume this topic",
            schema.id,
        ))),
    }
}

/// Converts an outgoing plain-JSON payload to the wire datum for the subject's schema
/// flavor. The `avro` and `protobuf` features extend the match; without them those flavors
/// error.
fn outgoing_json_to_datum(
    registry: &SchemaRegistry,
    schema: &RegisteredSchema,
    message: Option<&str>,
    payload: &[u8],
) -> Result<Vec<u8>, KafkaError> {
    let _ = (registry, message);
    match schema.schema_type {
        SchemaType::Json => Ok(payload.to_vec()),
        #[cfg(feature = "avro")]
        SchemaType::Avro => crate::avro::json_to_avro(registry, schema, payload),
        #[cfg(feature = "protobuf")]
        SchemaType::Protobuf => {
            crate::protobuf::json_to_protobuf(registry, schema, message, payload)
        }
        #[allow(unreachable_patterns)] // the disabled-format arms
        other => Err(KafkaError::InvalidOptions(format!(
            "the subject's schema (id {}) is {other:?}, but the matching cargo feature is \
             not enabled on ruststream-rdkafka; enable it to publish to this topic",
            schema.id,
        ))),
    }
}

/// Publish middleware framing outgoing JSON in the Confluent wire format - the publish-side
/// half of the registry integration, added app-wide with `RustStream::publish_layer`.
///
/// For every publish it resolves the destination topic's value subject (lazily: the shared
/// cache first, the registry once on a miss) and frames the codec's plain-JSON payload by
/// the subject's registered flavor: JSON keeps its bytes under the envelope, Avro and
/// Protobuf transcode against the schema (with their cargo features). A topic with no
/// registered subject publishes untouched - mixed registry/plain topologies need no
/// configuration. The miss is cached (and logged) once per subject, so plain topics pay no
/// per-publish registry round-trip; a subject registered later is picked up after a restart
/// or an explicit [`SchemaRegistry::warm`]. A registry outage or a payload that does not fit
/// the schema fails the publish, so a publishing handler nacks and the delivery is retried
/// rather than a mis-framed record reaching the topic.
///
/// Subjects follow [`SubjectStrategy::TopicName`] by default;
/// [`subject_strategy`](Self::subject_strategy) switches the naming, and
/// [`subject`](Self::subject) pins a topic's subject explicitly (the `RecordName` layouts,
/// where the topic alone cannot name the subject).
///
/// # Examples
///
/// ```no_run
/// use ruststream::runtime::{AppInfo, RustStream};
/// use ruststream_rdkafka::{SchemaFrame, SchemaRegistry};
///
/// let sr = SchemaRegistry::new("http://localhost:8081");
/// let app = RustStream::new(AppInfo::new("orders", "1.0.0"))
///     .publish_layer(SchemaFrame::new(sr.clone()));
/// # let _ = app;
/// ```
#[derive(Clone)]
pub struct SchemaFrame {
    registry: SchemaRegistry,
    strategy: SubjectStrategy,
    subjects: HashMap<String, String>,
    /// Per-topic Protobuf message names (fully qualified), for multi-message schemas.
    messages: HashMap<String, String>,
    /// Subjects the registry answered 404 for: their topics publish un-framed without
    /// re-querying.
    skipped: Arc<Mutex<HashSet<String>>>,
}

impl fmt::Debug for SchemaFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaFrame")
            .field("strategy", &self.strategy)
            .field("subjects", &self.subjects)
            .finish_non_exhaustive()
    }
}

impl SchemaFrame {
    /// Builds the framing middleware over `registry`. Subjects default to the Confluent
    /// `TopicName` strategy (`{topic}-value`).
    #[must_use]
    pub fn new(registry: SchemaRegistry) -> Self {
        Self {
            registry,
            strategy: SubjectStrategy::default(),
            subjects: HashMap::new(),
            messages: HashMap::new(),
            skipped: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// How destination topics map onto subjects (default:
    /// [`SubjectStrategy::TopicName`]). The `RecordName` strategies need the record's name,
    /// which the publish path does not know; pin those subjects per topic with
    /// [`subject`](Self::subject) instead.
    #[must_use]
    pub fn subject_strategy(mut self, strategy: SubjectStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Pins `topic`'s subject explicitly, overriding the strategy.
    #[must_use]
    pub fn subject(mut self, topic: impl Into<String>, subject: impl Into<String>) -> Self {
        self.subjects.insert(topic.into(), subject.into());
        self
    }

    /// Pins the Protobuf message `topic`'s payloads serialize as, by fully qualified name
    /// (package included) - for subjects whose `.proto` schema declares several messages.
    /// Without it a Protobuf subject serializes as the schema's first top-level message.
    #[must_use]
    pub fn message(mut self, topic: impl Into<String>, message: impl Into<String>) -> Self {
        self.messages.insert(topic.into(), message.into());
        self
    }

    /// The subject `topic` publishes under.
    fn subject_for(&self, topic: &str) -> String {
        self.subjects
            .get(topic)
            .cloned()
            .unwrap_or_else(|| self.strategy.subject(topic, ""))
    }

    fn is_skipped(&self, subject: &str) -> bool {
        self.skipped
            .lock()
            .expect("skipped-subjects mutex poisoned")
            .contains(subject)
    }

    /// Remembers (and logs, once) that `subject` is unregistered, so `topic` publishes
    /// un-framed without re-querying the registry.
    fn skip(&self, topic: &str, subject: &str) {
        let mut skipped = self
            .skipped
            .lock()
            .expect("skipped-subjects mutex poisoned");
        if skipped.insert(subject.to_owned()) {
            tracing::info!(
                target: "ruststream_rdkafka",
                topic,
                subject,
                "no schema registered for the topic's subject; its publishes go out un-framed",
            );
        }
    }

    /// Frames `out`'s payload in place when its topic's subject is registered.
    async fn frame(&self, out: &mut Outgoing<'_>) -> Result<(), KafkaError> {
        let topic = out.name().to_owned();
        let subject = self.subject_for(&topic);
        let cached = self.registry.cached_subject(&subject);
        let schema = if let Some(schema) = cached {
            schema
        } else {
            if self.is_skipped(&subject) {
                return Ok(());
            }
            let Some(schema) = self.registry.latest(&subject).await? else {
                self.skip(&topic, &subject);
                return Ok(());
            };
            schema
        };
        let message = self.messages.get(&topic).map(String::as_str);
        let datum = outgoing_json_to_datum(&self.registry, &schema, message, out.payload())?;
        let framed = encode_envelope(schema.id, &datum);
        let payload = out.payload_mut();
        payload.clear();
        payload.extend_from_slice(&framed);
        Ok(())
    }
}

impl PublishLayer for SchemaFrame {
    async fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.frame(out).await?;
        next.run(out).await
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

    #[tokio::test]
    async fn frame_wraps_registered_json_subjects() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/orders-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 11,
                "version": 1,
                "schema": "{\"type\":\"object\"}",
                "schemaType": "JSON",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let frame = SchemaFrame::new(SchemaRegistry::new(server.uri()));
        let json = br#"{"id":7}"#;
        let mut out = Outgoing::new("orders", json.as_slice());
        frame.frame(&mut out).await.expect("frame");
        let (id, datum) = parse_envelope(out.payload()).expect("framed");
        assert_eq!(id, 11);
        assert_eq!(datum, json);

        // The second publish resolves from the cache (expect(1) verifies).
        let mut again = Outgoing::new("orders", json.as_slice());
        frame.frame(&mut again).await.expect("frame cached");
        assert_eq!(again.payload(), out.payload());
    }

    #[tokio::test]
    async fn unregistered_subjects_pass_through_and_cache_the_miss() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/plain-value/versions/latest"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;

        let frame = SchemaFrame::new(SchemaRegistry::new(server.uri()));
        let json = br#"{"plain":true}"#;
        for _ in 0..2 {
            // The miss is remembered: the registry sees one query (expect(1) verifies).
            let mut out = Outgoing::new("plain", json.as_slice());
            frame.frame(&mut out).await.expect("pass through");
            assert_eq!(out.payload(), json, "un-framed");
        }
    }

    #[tokio::test]
    async fn registry_outages_fail_the_publish() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/orders-value/versions/latest"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let frame = SchemaFrame::new(SchemaRegistry::new(server.uri()));
        let mut out = Outgoing::new("orders", br#"{"id":7}"#.as_slice());
        let err = frame.frame(&mut out).await.expect_err("outage");
        assert!(matches!(err, KafkaError::SchemaRegistry(_)));
    }

    #[tokio::test]
    async fn pinned_subjects_override_the_strategy() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/com.acme.Order/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 3,
                "version": 1,
                "schema": "{\"type\":\"object\"}",
                "schemaType": "JSON",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let frame =
            SchemaFrame::new(SchemaRegistry::new(server.uri())).subject("orders", "com.acme.Order");
        let mut out = Outgoing::new("orders", br#"{"id":1}"#.as_slice());
        frame.frame(&mut out).await.expect("frame");
        let (id, _) = parse_envelope(out.payload()).expect("framed");
        assert_eq!(id, 3);
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
