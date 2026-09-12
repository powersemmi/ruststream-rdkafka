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

mod cache;
mod client;
mod name;

use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use cache::{MemorySchemaCache, SchemaCache, SchemaCachePolicy};
pub use client::{HttpRegistryClient, RegistryClient};

use client::{Auth, HttpConfig};
pub(crate) use name::serde_name;
use ruststream::codec::{Codec, CodecError};
use ruststream::runtime::{Outgoing, PublishLayer, PublishNext, PublishPipeline};
use ruststream::{BytesMut, Publisher};

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
    /// Builds one, for a [`RegistryClient`] implementation answering a lookup.
    #[must_use]
    pub fn new(id: u32, schema_type: SchemaType, definition: impl Into<String>) -> Self {
        Self {
            id,
            schema_type,
            definition: definition.into(),
        }
    }

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

struct RegistryInner {
    /// What answers a registry question, and what remembers the answer. Both are seams: a
    /// service that wants a different HTTP stack, a published client crate, a binding to a
    /// non-Rust client, or a cache of its own replaces one without touching anything above.
    client: Arc<dyn RegistryClient>,
    cache: Arc<dyn SchemaCache>,
    /// How the shipped HTTP client is configured, when that is what sits behind this facade, so
    /// `basic_auth`, `bearer_token` and `request_timeout` can rebuild it without dropping the
    /// settings they do not touch. `None` for a caller-supplied client.
    shipped: Option<HttpConfig>,
    /// The parsed forms of what the cache holds, which belong to the format features rather than
    /// to the registry conversation, and are dropped alongside the entries they came from.
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
/// It is a facade over two seams, and everything else in this crate is written against it rather
/// than against either: [`RegistryClient`] answers the registry's questions asynchronously, and
/// [`SchemaCache`] remembers the answers synchronously. Replacing one - a different HTTP stack, a
/// published client crate, a binding to a non-Rust client, a cache with different bounds - leaves
/// the codecs, the byte-lane subjects, the prefetch and the transcoding middleware untouched.
///
/// # Examples
///
/// ```no_run
/// use ruststream_rdkafka::{KafkaBroker, SchemaRegistry};
///
/// let registry = SchemaRegistry::new("http://localhost:8081").basic_auth("svc", "secret");
/// // Consuming: subscriptions transcode framed deliveries to plain JSON through the client.
/// let broker = KafkaBroker::new(["localhost:9092"]).schema_registry(registry.clone());
/// # let _ = broker;
/// ```
#[derive(Clone)]
pub struct SchemaRegistry {
    inner: Arc<RegistryInner>,
}

impl fmt::Debug for SchemaRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaRegistry").finish_non_exhaustive()
    }
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
        Self::shipping(HttpConfig::new(base_url))
    }

    /// A client over a [`RegistryClient`] of your own and the default cache.
    ///
    /// This is the seam for a registry this crate does not speak to: a different HTTP stack, a
    /// published client crate, a binding to a non-Rust client, a fake in a test.
    #[must_use]
    pub fn with_client(client: Arc<dyn RegistryClient>) -> Self {
        Self::assembled(
            client,
            Arc::new(MemorySchemaCache::new(SchemaCachePolicy::default())),
        )
    }

    /// Replaces what this client remembers with a [`SchemaCache`] of your own.
    ///
    /// Configure before handing the client out: clones share one cache.
    #[must_use]
    pub fn with_cache(self, cache: Arc<dyn SchemaCache>) -> Self {
        let assembled = Self::assembled(Arc::clone(&self.inner.client), cache);
        match self.inner.shipped.clone() {
            Some(config) => assembled.remembering(config),
            None => assembled,
        }
    }

    /// How much of what it resolves this client remembers; see [`SchemaCachePolicy`].
    ///
    /// Sugar for [`with_cache`](Self::with_cache) over the default cache. Configure before
    /// handing the client out: clones share one cache and the configuration it was made with.
    #[must_use]
    pub fn cache_policy(self, policy: SchemaCachePolicy) -> Self {
        self.with_cache(Arc::new(MemorySchemaCache::new(policy)))
    }

    /// HTTP basic authentication for every registry request. Configure before handing the
    /// client out: clones share the configuration they were made from.
    ///
    /// # Panics
    ///
    /// Panics when this client was built with [`with_client`](Self::with_client): the
    /// credentials of a client this crate did not write are that client's own business.
    #[must_use]
    pub fn basic_auth(self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.with_auth(Auth::Basic {
            user: user.into(),
            password: password.into(),
        })
    }

    /// The deadline for one registry request, counted from the start of the request until its
    /// response body is read. The default is ten seconds.
    ///
    /// Both edges resolve schemas while a delivery or a publish waits, so this is what bounds a
    /// registry that accepts the connection and then goes silent. On expiry the request returns
    /// [`KafkaError::SchemaRegistryTimeout`], which names the path and the deadline: a framed
    /// delivery then reaches the handler un-transcoded with a warning, and a publish returns the
    /// error so the publishing handler asks for a redelivery.
    ///
    /// Configure before handing the client out.
    ///
    /// # Panics
    ///
    /// As [`basic_auth`](Self::basic_auth): the deadline configures the HTTP client this crate
    /// ships, and a client supplied with [`with_client`](Self::with_client) bounds its own
    /// requests.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use ruststream_rdkafka::SchemaRegistry;
    ///
    /// let registry =
    ///     SchemaRegistry::new("http://localhost:8081").request_timeout(Duration::from_secs(2));
    /// # let _ = registry;
    /// ```
    #[must_use]
    pub fn request_timeout(self, timeout: Duration) -> Self {
        let mut config = self.shipped_config();
        config.timeout = timeout;
        Self::shipping(config)
    }

    /// Bearer-token authentication for every registry request. Configure before handing the
    /// client out.
    ///
    /// # Panics
    ///
    /// As [`basic_auth`](Self::basic_auth).
    #[must_use]
    pub fn bearer_token(self, token: impl Into<String>) -> Self {
        self.with_auth(Auth::Bearer(token.into()))
    }

    /// Rebuilds the shipped HTTP client with new credentials, and empties the caches: what the
    /// previous credentials could see is not an answer these ones may give.
    fn with_auth(self, auth: Auth) -> Self {
        let mut config = self.shipped_config();
        config.auth = auth;
        Self::shipping(config)
    }

    /// The configuration of the shipped HTTP client behind this facade.
    ///
    /// # Panics
    ///
    /// Panics when the facade holds a caller-supplied client, which configures itself.
    fn shipped_config(&self) -> HttpConfig {
        self.inner.shipped.clone().expect(
            "basic_auth, bearer_token and request_timeout configure the HTTP client this crate \
             ships; a RegistryClient supplied with `with_client` carries its own credentials and \
             bounds its own requests",
        )
    }

    /// A facade over a freshly built shipped HTTP client and an empty default cache.
    fn shipping(config: HttpConfig) -> Self {
        Self::assembled(
            Arc::new(HttpRegistryClient::new(config.clone())),
            Arc::new(MemorySchemaCache::new(SchemaCachePolicy::default())),
        )
        .remembering(config)
    }

    fn assembled(client: Arc<dyn RegistryClient>, cache: Arc<dyn SchemaCache>) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                client,
                cache,
                shipped: None,
                #[cfg(feature = "avro")]
                parsed_avro: Mutex::new(HashMap::new()),
                #[cfg(feature = "protobuf")]
                parsed_proto: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Records that the client behind this facade is the shipped HTTP one, and how it is
    /// configured, so the settings builders can rebuild it. Absent for a caller-supplied client.
    fn remembering(self, config: HttpConfig) -> Self {
        let mut inner = self.inner;
        let slot = Arc::get_mut(&mut inner).expect("freshly assembled, not yet shared");
        slot.shipped = Some(config);
        Self { inner }
    }

    /// Records a freshly resolved schema, and drops the parsed forms of anything the cache let go
    /// of to make room.
    fn remember(&self, subject: Option<&str>, schema: &Arc<RegisteredSchema>) {
        self.inner.cache.store(subject, schema);
        let evicted = self.inner.cache.evicted_ids();
        if !evicted.is_empty() {
            self.purge_parsed(&evicted);
        }
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
        let schema = self.inner.client.schema_by_id(id).await?;
        // No subject: a lookup by id says nothing about which subject points at it.
        self.remember(None, &schema);
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
        let id = self
            .inner
            .client
            .register(subject, schema_type, definition.clone())
            .await?;
        self.remember(
            Some(subject),
            &Arc::new(RegisteredSchema::new(id, schema_type, definition)),
        );
        Ok(id)
    }

    /// Whether `definition` is compatible with `subject`'s latest version, or `None` when the
    /// client behind this facade cannot answer.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable, rejects the
    /// request, or reports the schemas incompatible - that message carries the registry's own
    /// account of what differs.
    pub async fn is_compatible(
        &self,
        subject: &str,
        schema_type: SchemaType,
        definition: impl Into<String>,
    ) -> Result<Option<bool>, KafkaError> {
        self.inner
            .client
            .is_compatible(subject, schema_type, definition.into())
            .await
    }

    /// The id the registry already gave exactly this schema under `subject`, registering
    /// nothing.
    ///
    /// This is the lookup a producer that owns its schema needs and [`warm`](Self::warm) cannot
    /// answer: `warm` resolves *the subject's latest* schema, while a byte-lane producer writes
    /// datums under *its own*, and framing one with the other's id puts an unreadable record on
    /// the topic. Nothing is cached, for the same reason: the answer is about a schema the
    /// caller brought, not about what the subject currently holds.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::SchemaRegistry`] when the registry is unreachable or rejects the
    /// request, and [`KafkaError::InvalidOptions`] when the subject does not hold this schema.
    pub async fn lookup_id(
        &self,
        subject: &str,
        schema_type: SchemaType,
        definition: impl Into<String>,
    ) -> Result<u32, KafkaError> {
        self.inner
            .client
            .lookup_id(subject, schema_type, definition.into())
            .await
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
        self.latest(subject).await?.ok_or_else(|| {
            KafkaError::SchemaRegistry(format!("the registry has no subject {subject:?}").into())
        })
    }

    /// Like [`warm`](Self::warm), but a missing subject resolves to `None` instead of an
    /// error - the framing middleware treats it as "this topic is not registry-backed".
    pub(crate) async fn latest(
        &self,
        subject: &str,
    ) -> Result<Option<Arc<RegisteredSchema>>, KafkaError> {
        let Some(schema) = self.inner.client.latest(subject).await? else {
            return Ok(None);
        };
        self.remember(Some(subject), &schema);
        Ok(Some(schema))
    }

    /// Drops the parsed forms of schemas the id cache has just evicted: they are derived from
    /// the definitions that went with them, so keeping them would outlive their source.
    #[cfg(any(feature = "avro", feature = "protobuf"))]
    fn purge_parsed(&self, evicted: &[u32]) {
        for id in evicted {
            #[cfg(feature = "avro")]
            self.inner
                .parsed_avro
                .lock()
                .expect("parsed schema cache mutex poisoned")
                .remove(id);
            #[cfg(feature = "protobuf")]
            self.inner
                .parsed_proto
                .lock()
                .expect("descriptor cache mutex poisoned")
                .remove(id);
        }
    }

    /// No parsed forms are kept without a format feature, so there is nothing to purge.
    #[cfg(not(any(feature = "avro", feature = "protobuf")))]
    #[allow(clippy::unused_self)]
    fn purge_parsed(&self, _evicted: &[u32]) {}

    /// The cached schema for `id`, when an earlier lookup resolved it (the subscription's
    /// transcoding keeps ids it has seen warm).
    ///
    /// # Panics
    ///
    /// Panics when the internal cache mutex is poisoned, which requires a prior panic inside
    /// the client (an invariant violation, not an operational failure).
    #[must_use]
    pub fn cached_schema(&self, id: u32) -> Option<Arc<RegisteredSchema>> {
        self.inner.cache.schema(id)
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
        self.inner.cache.subject(subject)
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
    /// The full schema JSON is registered, not its Parsing Canonical Form: canonicalization
    /// keeps only what makes two schemas *the same* and drops field defaults, aliases, docs and
    /// logical types - and a field default is precisely what lets a later reader resolve an
    /// earlier writer's datum, so a subject registered canonically can never carry an evolution.
    ///
    /// # Errors
    ///
    /// As [`register`](Self::register), plus [`KafkaError::WireFormat`] when the derived schema
    /// cannot be serialized.
    #[cfg(feature = "avro")]
    pub async fn register_avro<T: apache_avro::AvroSchema>(
        &self,
        subject: &str,
    ) -> Result<u32, KafkaError> {
        let definition = crate::avro::schema_json(&T::get_schema())?;
        self.register(subject, SchemaType::Avro, definition).await
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

/// The async half of a registry-backed codec: it resolves, on the broker's async edges, every
/// schema the synchronous codec will read.
///
/// A [`Codec`] is synchronous on both ends and a registry lookup is
/// not, and no amount of arranging makes those meet inside `encode` or `decode`: blocking a
/// runtime worker from a sync function is not an option and guessing a schema is corruption. So
/// the lookups move to the two places that are already `async` and already know when they have to
/// happen - the broker's `connect`, for the subjects a codec publishes under, and each
/// subscription's delivery path, for the writer schema an arriving envelope names. By the time a
/// codec runs, what it needs is in the shared cache, and its own work is pure computation.
///
/// This is a distinct attachment from
/// [`KafkaBroker::schema_registry`](crate::KafkaBroker::schema_registry) on purpose. That one
/// *rewrites* deliveries into JSON, which is the transcoding compatibility path; this one leaves
/// every payload exactly as it arrived and only fills a cache. Attaching both to one broker is
/// contradictory - the transcode would hand a JSON document to a codec expecting the Confluent
/// wire format - and the prefetch runs first so it still sees the envelope.
///
/// # Examples
///
/// ```no_run
/// use ruststream_rdkafka::{KafkaBroker, SchemaPrefetch, SchemaRegistry};
/// use ruststream_rdkafka::avro::AvroCodec;
///
/// let prefetch = SchemaPrefetch::new(SchemaRegistry::new("http://localhost:8081"));
/// // Every type a codec registers records its subject here, so `connect` resolves it and a
/// // subject that is missing is settled by the prefetch's policy rather than at first publish.
/// let codec = AvroCodec::registry(&prefetch);
/// let broker = KafkaBroker::new(["localhost:9092"]).schema_prefetch(prefetch);
/// # let _ = (codec, broker);
/// ```
#[derive(Clone)]
pub struct SchemaPrefetch {
    registry: SchemaRegistry,
    /// What the codecs built from this prefetch publish, and the schema each was registered
    /// with. Shared with those codecs' clones, so registering a type once puts it on this list.
    registrations: Arc<Mutex<Vec<Registration>>>,
    on_missing: MissingSubject,
    check_compatibility: bool,
}

/// One `register::<T>(subject)` call: what to resolve at connect, and what to put back if the
/// registry no longer holds it.
#[derive(Debug, Clone)]
pub(crate) struct Registration {
    pub(crate) subject: String,
    pub(crate) schema_type: SchemaType,
    /// The schema the registering type carries. Kept so
    /// [`MissingSubject::AutoRegister`] has something to register, and never used to encode:
    /// what a datum is written with is the schema the framed id names.
    pub(crate) definition: String,
}

/// What a registered subject's absence from the registry means, and what to do about it.
///
/// The vocabulary is Confluent's, because an operator who knows Kafka already has these settings
/// in their head. Their serializers cover this ground with three: `auto.register.schemas`
/// (register the local schema when the subject does not have it), `use.latest.version` (only
/// when auto-registration is off - serialize against the subject's latest registered version
/// rather than the local schema), and `latest.compatibility.strict` (check the local schema
/// against that latest version).
///
/// Two of those describe this crate's *normal* path rather than this enum. Encoding always
/// writes with the schema the framed id names, which is `use.latest.version` semantics, and
/// [`SchemaPrefetch::check_compatibility`] is `latest.compatibility.strict`. What is left for
/// this enum is the one question those settings answer between them: when the subject is not
/// there at all, does the producer create it, refuse, or go without.
///
/// It is an enum rather than the three booleans Confluent uses because the booleans are not
/// independent - `use.latest.version` means nothing while auto-registration is on - and a
/// combination that means nothing is exactly what an enum keeps unrepresentable.
///
/// # What a deletion actually does
///
/// Confluent's two deletions differ, and this was checked against a live registry rather than
/// assumed. A **soft** delete (`DELETE /subjects/{s}`) hides the subject - its `versions/latest`
/// answers 404 - while `GET /schemas/ids/{id}` still returns the schema, so **consumers keep
/// working** and only the producer is stuck. A **permanent** delete (`?permanent=true`) removes
/// the id too, and then nothing can decode a record that names it: no policy here helps a
/// consumer, because the schema is gone for everyone. Re-registering the same schema afterwards
/// mints a *new* id, so records already on the topic stay unreadable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum MissingSubject {
    /// Fail, and say which subject is gone. The default.
    ///
    /// Confluent's `auto.register.schemas=false` with `use.latest.version=true`: the producer
    /// writes only what the registry already holds, so a subject that is not there stops it.
    /// Creating subjects in someone else's registry as a side effect of starting up is worse
    /// than not starting: a schema registry is a shared contract, and a producer that quietly
    /// writes into it takes a decision that belongs to whoever owns the topic.
    #[default]
    Refuse,
    /// Register the schema the type carries under the subject, and warn.
    ///
    /// Confluent's `auto.register.schemas=true`, which is their own producer default, for a
    /// deployment where the producer does own its subjects and an absent one is an accident to
    /// repair rather than a signal to stop.
    AutoRegister,
    /// Publish the datum with no envelope, and warn.
    ///
    /// No Confluent counterpart: their serializers have no "carry on without the registry" mode.
    /// For a topic that is expected to survive its registry being unavailable, at the cost of
    /// records that no registry-backed consumer can decode.
    PublishUnframed,
}

impl fmt::Debug for SchemaPrefetch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaPrefetch")
            .field("registry", &self.registry)
            .field("on_missing", &self.on_missing)
            .finish_non_exhaustive()
    }
}

impl SchemaPrefetch {
    /// Builds the prefetch over `registry`. Construction is synchronous and does no I/O; the
    /// resolving happens on the broker's async edges.
    #[must_use]
    pub fn new(registry: SchemaRegistry) -> Self {
        Self {
            registry,
            registrations: Arc::new(Mutex::new(Vec::new())),
            on_missing: MissingSubject::default(),
            check_compatibility: true,
        }
    }

    /// Whether `connect` checks each registered type's schema against the version its subject
    /// already holds, and refuses to start when the registry says they are incompatible.
    ///
    /// This is Confluent's `latest.compatibility.strict`, and on for the same reason it is on by
    /// default there: a model that has drifted from its subject is a fact worth learning while
    /// the app is starting rather than from a consumer that cannot read what it wrote. The
    /// registry's own account of what differs, down to the field, travels in the error.
    ///
    /// Turn it off for a registry whose compatibility level is deliberately `NONE`, or a client
    /// that cannot answer the question - a [`RegistryClient`] that does not implement
    /// `is_compatible` skips it regardless.
    #[must_use]
    pub fn check_compatibility(mut self, check: bool) -> Self {
        self.check_compatibility = check;
        self
    }

    /// What to do about a registered subject the registry does not hold; see
    /// [`MissingSubject`]. Defaults to [`Refuse`](MissingSubject::Refuse).
    ///
    /// Configure before building codecs from it: each copies the policy as it is built.
    #[must_use]
    pub fn on_missing_subject(mut self, policy: MissingSubject) -> Self {
        self.on_missing = policy;
        self
    }

    /// The client the codecs share a cache with.
    #[must_use]
    pub fn registry(&self) -> &SchemaRegistry {
        &self.registry
    }

    /// The policy codecs built from this prefetch carry.
    pub(crate) fn missing_subject(&self) -> MissingSubject {
        self.on_missing
    }

    /// Records what a `register::<T>(subject)` call published, so
    /// [`warm_subjects`](Self::warm_subjects) resolves it.
    ///
    /// # Panics
    ///
    /// Panics when the internal mutex is poisoned, which requires a prior panic inside this
    /// type (an invariant violation, not an operational failure).
    pub(crate) fn record(&self, registration: Registration) {
        self.registrations
            .lock()
            .expect("prefetch registrations mutex poisoned")
            .push(registration);
    }

    /// Resolves every registered subject. Called once, by the broker's `connect`, so an absent
    /// subject is settled while the app is still starting rather than at its first publish.
    pub(crate) async fn warm_subjects(&self) -> Result<(), KafkaError> {
        let registrations: Vec<Registration> = self
            .registrations
            .lock()
            .expect("prefetch registrations mutex poisoned")
            .clone();
        for registration in registrations {
            let subject = &registration.subject;
            if self.registry.latest(subject).await?.is_some() {
                if self.check_compatibility {
                    self.registry
                        .is_compatible(
                            subject,
                            registration.schema_type,
                            registration.definition.clone(),
                        )
                        .await?;
                }
                continue;
            }
            match self.on_missing {
                MissingSubject::Refuse => {
                    return Err(KafkaError::SchemaRegistry(
                        format!(
                            "subject {subject:?} is registered by a codec in this app but the \
                             registry does not hold it, so there is no id to frame its records \
                             with. It was most likely deleted; re-register it, point the codec \
                             at the subject that carries this schema, or choose a \
                             MissingSubject policy that repairs or bypasses it."
                        )
                        .into(),
                    ));
                }
                MissingSubject::AutoRegister => {
                    let id = self
                        .registry
                        .register(
                            subject,
                            registration.schema_type,
                            registration.definition.clone(),
                        )
                        .await?;
                    tracing::warn!(
                        target: "ruststream_rdkafka",
                        subject = %subject,
                        schema_id = id,
                        schema_type = ?registration.schema_type,
                        definition = %registration.definition,
                        "the registry did not hold a subject this app publishes under, so it \
                         was registered again from the schema the message type carries; records \
                         written before the deletion keep the id they were framed with, which a \
                         permanent delete has already made unreadable",
                    );
                }
                MissingSubject::PublishUnframed => {
                    tracing::warn!(
                        target: "ruststream_rdkafka",
                        subject = %subject,
                        schema_type = ?registration.schema_type,
                        definition = %registration.definition,
                        "the registry did not hold a subject this app publishes under, so its \
                         records go out with no Confluent envelope; a registry-backed consumer \
                         cannot decode them",
                    );
                }
            }
        }
        Ok(())
    }

    /// Resolves the writer schema an arriving envelope names, so the delivery's own decode finds
    /// it in the cache. The payload is not touched.
    ///
    /// A payload carrying no envelope needs nothing, and a lookup that fails is only logged: the
    /// decode that follows reports it as a decode failure, which the subscription's failure
    /// policy settles - one delivery's fate decided in one place, rather than half of it here.
    pub(crate) async fn warm_delivery(&self, payload: &[u8]) {
        let Some((id, _)) = parse_envelope(payload) else {
            return;
        };
        if self.registry.cached_schema(id).is_some() {
            return;
        }
        if let Err(err) = self.registry.schema_by_id(id).await {
            tracing::warn!(
                target: "ruststream_rdkafka",
                schema_id = id,
                error = %err,
                "the writer schema of an arriving delivery could not be resolved; its decode \
                 will report the miss and the subscription's failure policy settles it",
            );
        }
    }
}

/// The Confluent envelope around another codec, for a payload that does not need its schema to
/// be read.
///
/// The envelope is separable exactly when the datum inside it is self-describing. A JSON
/// document is: the id says which schema it claims to conform to, and the document parses
/// without it, so framing decomposes cleanly into "put the envelope on, take the envelope off"
/// with an inner codec doing the rest. An Avro datum is not: the id names the schema the datum
/// *cannot be read without*, and no wrapper can hand that per-delivery schema through a
/// [`Codec`] method - which is why [`AvroCodec`](crate::avro::AvroCodec) owns its envelope
/// instead of riding this one. That line is the design rule, not an accident of the two formats.
///
/// The wrapper frames and does not validate. A registry's JSON Schema is a compatibility
/// contract the registry itself enforces between versions, and checking every message against it
/// would mean carrying a JSON Schema validator and paying it per delivery, which Confluent's own
/// serializer makes optional for the same reason. Validation is the inner codec's business, and
/// the inner codec is named right here at the call site: a service that wants it passes a codec
/// that validates instead of a plain one.
///
/// Only encoding needs the registry, for the subject's id; decoding strips the envelope and
/// hands the bytes on, so a consumer needs no [`SchemaPrefetch`] resolved for it at all.
///
/// # Examples
///
/// ```no_run
/// use ruststream::codec::JsonCodec;
/// use ruststream_rdkafka::{SchemaFramed, SchemaPrefetch, SchemaRegistry};
///
/// let prefetch = SchemaPrefetch::new(SchemaRegistry::new("http://localhost:8081"));
/// // Confluent-framed JSON: the core's own codec, under the envelope of a registered subject.
/// # #[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
/// # struct Order { id: i64 }
/// let codec = SchemaFramed::new(&prefetch, JsonCodec).register::<Order>("orders-value");
/// # let _ = codec;
/// ```
#[derive(Debug, Clone)]
pub struct SchemaFramed<C> {
    registry: SchemaRegistry,
    /// Subject by message type, keyed by the name serde gives it - the same shape and the same
    /// key the Avro codec uses, and publish-only for the same reason: decoding strips the
    /// envelope and hands the bytes on, so it needs to know no type at all.
    subjects: Arc<HashMap<&'static str, String>>,
    on_missing: MissingSubject,
    prefetch: SchemaPrefetch,
    inner: C,
}

impl<C> SchemaFramed<C> {
    /// Frames what [`register`](Self::register) records, with `inner` producing the payload.
    ///
    /// Construction is synchronous and does no I/O; the subjects are resolved when the broker
    /// connects.
    #[must_use]
    pub fn new(prefetch: &SchemaPrefetch, inner: C) -> Self {
        Self {
            registry: prefetch.registry().clone(),
            subjects: Arc::new(HashMap::new()),
            on_missing: prefetch.missing_subject(),
            prefetch: prefetch.clone(),
            inner,
        }
    }

    /// Records that values of `T` publish under `subject`.
    ///
    /// The JSON Schema of `T` is captured with it, via `schemars`, for the one thing that needs
    /// it: putting the subject back under [`MissingSubject::AutoRegister`]. A self-describing
    /// payload under this envelope is a JSON Schema subject in practice, which is why that is
    /// the bound; a wrapper over some other self-describing codec registers its subject
    /// separately and this one still frames it.
    ///
    /// # Panics
    ///
    /// Panics when two registered types share a serde name, which would make the subject of a
    /// publish ambiguous - a startup failure by design, since the alternative is writing records
    /// under the wrong subject. Rename one with `#[serde(rename = "..")]`.
    #[must_use]
    pub fn register<T: serde::Serialize + JsonSchema>(
        mut self,
        subject: impl Into<String>,
    ) -> Self {
        let subject = subject.into();
        let name: &'static str = String::leak(T::schema_name().to_string());
        let definition = serde_json::to_string(
            &schemars::SchemaGenerator::default().into_root_schema_for::<T>(),
        )
        .unwrap_or_else(|err| panic!("the JSON Schema of the type under {subject:?}: {err}"));

        let table = Arc::get_mut(&mut self.subjects).expect("not yet shared with a mount");
        if let Some(existing) = table.insert(name, subject.clone())
            && existing != subject
        {
            panic!(
                "two types registered on this codec are both called {name:?} to serde, so a \
                 publish could not tell {existing:?} from {subject:?}; give one of them a \
                 distinct `#[serde(rename = \"..\")]`"
            );
        }
        self.prefetch.record(Registration {
            subject,
            schema_type: SchemaType::Json,
            definition,
        });
        self
    }

    /// The codec whose payloads travel inside the envelope.
    pub fn inner(&self) -> &C {
        &self.inner
    }
}

impl<C: Codec> Codec for SchemaFramed<C> {
    fn encode<T: serde::Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        let name = serde_name(value).ok_or_else(|| {
            CodecError::Encode(Box::new(KafkaError::malformed(
                "this codec frames by the name serde gives a value's type, and this value has \
                 none - a bare number, a sequence or a map. Publish a named struct, or use a \
                 codec that needs no subject"
                    .to_owned(),
            )))
        })?;
        let subject = self.subjects.get(name).ok_or_else(|| {
            CodecError::Encode(Box::new(KafkaError::malformed(format!(
                "no subject is registered for messages named {name:?}, so this codec has no id \
                 to frame them with; add `.register::<{name}>(\"..\")` to it. The types it does \
                 carry are {:?}",
                self.subjects.keys().collect::<Vec<_>>(),
            ))))
        })?;
        let datum = self.inner.encode(value)?;
        let Some(schema) = self.registry.cached_subject(subject) else {
            if self.on_missing == MissingSubject::PublishUnframed {
                return Ok(datum);
            }
            return Err(CodecError::Encode(Box::new(KafkaError::malformed(
                format!(
                    "no schema is cached for subject {subject:?}, which messages named {name:?} \
                 publish under. Attach the SchemaPrefetch this codec was built from to the \
                 broker (KafkaBroker::schema_prefetch) so connect resolves it; if it was \
                 resolved and has since gone, the subject was deleted from the registry and \
                 MissingSubject says what to do about that"
                ),
            ))));
        };
        let mut framed = BytesMut::with_capacity(1 + 4 + datum.len());
        framed.extend_from_slice(&[WIRE_MAGIC]);
        framed.extend_from_slice(&schema.id().to_be_bytes());
        framed.extend_from_slice(&datum);
        Ok(framed)
    }

    fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        // A payload with no envelope is refused rather than passed through: on a registry-backed
        // topic an unframed record is a producer that did not frame, and reading it anyway would
        // make that look like it worked.
        let (_, datum) = parse_envelope(bytes).ok_or_else(|| {
            CodecError::Decode(Box::new(KafkaError::malformed(format!(
                "the delivery does not carry the Confluent wire format (a zero magic byte and a \
                 4-byte schema id) that a registry-backed topic publishes under; its first bytes \
                 are {:02x?}",
                &bytes[..bytes.len().min(8)],
            ))))
        })?;
        self.inner.decode(datum)
    }
}

/// The topic-to-subject half a publish-side framing layer needs: the naming strategy, the
/// per-topic pins, and the memory of which subjects the registry does not hold.
///
/// Every framing layer has to answer the same question - "what schema does this topic publish
/// under, and is there one at all" - and only differs in what it then does with the answer, so
/// the question is answered in one place. The miss in particular is remembered here, which is
/// what keeps a topic that is not registry-backed from paying a round-trip per publish.
#[derive(Clone)]
pub(crate) struct SubjectMap {
    registry: SchemaRegistry,
    strategy: SubjectStrategy,
    /// Subjects pinned per topic, overriding the strategy.
    pins: HashMap<String, String>,
    /// Subjects the registry answered 404 for: their topics publish un-framed without
    /// re-querying.
    skipped: Arc<Mutex<HashSet<String>>>,
}

impl SubjectMap {
    pub(crate) fn new(registry: SchemaRegistry) -> Self {
        Self {
            registry,
            strategy: SubjectStrategy::default(),
            pins: HashMap::new(),
            skipped: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub(crate) fn registry(&self) -> &SchemaRegistry {
        &self.registry
    }

    pub(crate) fn strategy(&self) -> SubjectStrategy {
        self.strategy
    }

    pub(crate) fn set_strategy(&mut self, strategy: SubjectStrategy) {
        self.strategy = strategy;
    }

    pub(crate) fn pins(&self) -> &HashMap<String, String> {
        &self.pins
    }

    pub(crate) fn pin(&mut self, topic: String, subject: String) {
        self.pins.insert(topic, subject);
    }

    /// The subject `topic` publishes under.
    pub(crate) fn subject_for(&self, topic: &str) -> String {
        self.pins
            .get(topic)
            .cloned()
            .unwrap_or_else(|| self.strategy.subject(topic, ""))
    }

    /// The schema `topic` publishes under, or `None` when the registry has no such subject.
    ///
    /// A miss is remembered (and logged once), so a topic that is not registry-backed publishes
    /// untouched without re-querying; a subject registered later is picked up after a restart or
    /// an explicit [`SchemaRegistry::warm`].
    ///
    /// # Panics
    ///
    /// Panics when the internal mutex is poisoned, which requires a prior panic inside this
    /// type (an invariant violation, not an operational failure).
    pub(crate) async fn schema_for(
        &self,
        topic: &str,
    ) -> Result<Option<Arc<RegisteredSchema>>, KafkaError> {
        let subject = self.subject_for(topic);
        if let Some(schema) = self.registry.cached_subject(&subject) {
            return Ok(Some(schema));
        }
        if self.is_skipped(&subject) {
            return Ok(None);
        }
        let Some(schema) = self.registry.latest(&subject).await? else {
            self.skip(topic, &subject);
            return Ok(None);
        };
        Ok(Some(schema))
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
/// let registry = SchemaRegistry::new("http://localhost:8081");
/// let app = RustStream::new(AppInfo::new("orders", "1.0.0"))
///     .publish_layer(SchemaFrame::new(registry.clone()));
/// # let _ = app;
/// ```
#[derive(Clone)]
pub struct SchemaFrame {
    subjects: SubjectMap,
    /// Per-topic Protobuf message names (fully qualified), for multi-message schemas.
    messages: HashMap<String, String>,
}

impl fmt::Debug for SchemaFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaFrame")
            .field("strategy", &self.subjects.strategy())
            .field("subjects", self.subjects.pins())
            .finish_non_exhaustive()
    }
}

impl SchemaFrame {
    /// Builds the framing middleware over `registry`. Subjects default to the Confluent
    /// `TopicName` strategy (`{topic}-value`).
    #[must_use]
    pub fn new(registry: SchemaRegistry) -> Self {
        Self {
            subjects: SubjectMap::new(registry),
            messages: HashMap::new(),
        }
    }

    /// How destination topics map onto subjects (default:
    /// [`SubjectStrategy::TopicName`]). The `RecordName` strategies need the record's name,
    /// which the publish path does not know; pin those subjects per topic with
    /// [`subject`](Self::subject) instead.
    #[must_use]
    pub fn subject_strategy(mut self, strategy: SubjectStrategy) -> Self {
        self.subjects.set_strategy(strategy);
        self
    }

    /// Pins `topic`'s subject explicitly, overriding the strategy.
    #[must_use]
    pub fn subject(mut self, topic: impl Into<String>, subject: impl Into<String>) -> Self {
        self.subjects.pin(topic.into(), subject.into());
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

    /// Frames `out`'s payload in place when its topic's subject is registered.
    async fn frame(&self, out: &mut Outgoing<'_>) -> Result<(), KafkaError> {
        let topic = out.name().to_owned();
        let Some(schema) = self.subjects.schema_for(&topic).await? else {
            return Ok(());
        };
        let message = self.messages.get(&topic).map(String::as_str);
        let datum =
            outgoing_json_to_datum(self.subjects.registry(), &schema, message, out.payload())?;
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
