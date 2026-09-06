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

use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error as StdError;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ruststream::codec::{Codec, CodecError};
use ruststream::runtime::{Outgoing, PublishLayer, PublishNext, PublishPipeline};
use ruststream::{BytesMut, Publisher};
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

#[derive(Clone)]
enum Auth {
    None,
    Basic { user: String, password: String },
    Bearer(String),
}

/// How a [`SchemaRegistry`] remembers what it has resolved.
///
/// The two halves of the registry's answer expire differently, and that is not a preference but
/// a property of the service: **a schema id is immutable**. The registry assigns an id per
/// distinct schema definition, globally and by content - registering a new version of a subject
/// mints a *new* id and leaves the old one resolving to the old schema for ever, and registering
/// an identical schema under a different subject hands back the id it already had. A subject's
/// *latest version*, by contrast, moves whenever someone registers one.
///
/// So an id-keyed entry can never go stale and needs no expiry - a TTL over it could only cause
/// a refetch that returns the identical bytes - while a subject-keyed entry can, and is the only
/// thing [`subject_ttl`](Self::subject_ttl) governs. Confluent's own clients scope their
/// `latest.cache.ttl.sec` to exactly the latest-version caches for the same reason.
///
/// What ids still need is a bound. A consumer meets one id per writer version it is sent, which
/// is small in a healthy topology and unbounded in a broken one (a producer registering a schema
/// per message), so the cache is capped and evicts in insertion order. Evicting an id that is
/// still in use costs one refetch and never a wrong answer, which is what makes plain insertion
/// order enough here and a recency policy not worth its bookkeeping.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroUsize;
/// use std::time::Duration;
///
/// use ruststream_rdkafka::{SchemaCachePolicy, SchemaRegistry};
///
/// // The default: ids kept, up to a bound, and subjects never re-resolved.
/// let sr = SchemaRegistry::new("http://localhost:8081");
///
/// // A producer that should notice a newly registered version without a restart.
/// let refreshing = SchemaRegistry::new("http://localhost:8081").cache_policy(
///     SchemaCachePolicy::Cached {
///         capacity: NonZeroUsize::new(256).expect("non-zero"),
///         subject_ttl: Some(Duration::from_secs(300)),
///     },
/// );
///
/// // Nothing remembered at all: every lookup reaches the registry.
/// let uncached = SchemaRegistry::new("http://localhost:8081")
///     .cache_policy(SchemaCachePolicy::Disabled);
/// # let _ = (sr, refreshing, uncached);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaCachePolicy {
    /// Nothing is remembered: every lookup reaches the registry.
    ///
    /// This is a real configuration, not a zero TTL wearing a disguise - for a process that must
    /// hold no schema state, or a test that wants every call observable. It has one consequence
    /// worth knowing before choosing it: a synchronous [`Codec`] reads the cache and cannot
    /// await a miss, so [`AvroCodec::registry`](crate::avro::AvroCodec::registry) and
    /// [`SchemaFramed`] cannot work under it. The transcoding path, whose lookups are all
    /// `async`, works unchanged.
    Disabled,
    /// Ids are remembered up to `capacity`, and a subject's resolved version for `subject_ttl`.
    Cached {
        /// The most id-keyed schemas held at once; the oldest is dropped past it.
        capacity: NonZeroUsize,
        /// How long a subject's resolved version is trusted before the next `async` lookup
        /// re-resolves it. `None` never re-resolves, which is Confluent's own default.
        ///
        /// This governs the `async` lookups only ([`warm`](SchemaRegistry::warm) and the
        /// publish-side transcode). A codec's subject is resolved once, when the broker
        /// connects, and stays put: its `encode` is synchronous and cannot re-resolve, and a
        /// producer changing the schema it writes mid-process is a deliberate act, not something
        /// to absorb silently between two messages.
        subject_ttl: Option<Duration>,
    },
}

impl SchemaCachePolicy {
    /// The bound Confluent's own clients default to.
    const DEFAULT_CAPACITY: usize = 1000;
}

impl Default for SchemaCachePolicy {
    /// Ids kept up to 1000 (Confluent's own default bound), subjects never re-resolved
    /// (Confluent's own default TTL).
    fn default() -> Self {
        Self::Cached {
            capacity: NonZeroUsize::new(Self::DEFAULT_CAPACITY).expect("a non-zero constant"),
            subject_ttl: None,
        }
    }
}

/// A subject's resolved id, with the moment it was resolved, so a TTL can be applied to it.
struct SubjectEntry {
    id: u32,
    resolved: Instant,
}

struct RegistryInner {
    base_url: String,
    http: reqwest::Client,
    auth: Auth,
    policy: SchemaCachePolicy,
    by_id: Mutex<HashMap<u32, Arc<RegisteredSchema>>>,
    /// The ids in `by_id`, in the order they were inserted, so the cap can drop the oldest.
    id_order: Mutex<VecDeque<u32>>,
    by_subject: Mutex<HashMap<String, SubjectEntry>>,
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
                policy: SchemaCachePolicy::default(),
                by_id: Mutex::new(HashMap::new()),
                id_order: Mutex::new(VecDeque::new()),
                by_subject: Mutex::new(HashMap::new()),
                #[cfg(feature = "avro")]
                parsed_avro: Mutex::new(HashMap::new()),
                #[cfg(feature = "protobuf")]
                parsed_proto: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// How much of what it resolves this client remembers; see [`SchemaCachePolicy`].
    ///
    /// Configure before handing the client out: clones share one cache and the configuration it
    /// was made with.
    #[must_use]
    pub fn cache_policy(self, policy: SchemaCachePolicy) -> Self {
        let auth = self.inner.auth.clone();
        self.rebuilt(auth, policy)
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
        let policy = self.inner.policy;
        self.rebuilt(auth, policy)
    }

    /// A client with the same endpoint and a new configuration, and empty caches.
    ///
    /// The caches are not carried over: what the credentials could see and what the policy would
    /// have kept are both part of how an entry got there, so an entry from the previous
    /// configuration is not an answer this one may give.
    fn rebuilt(self, auth: Auth, policy: SchemaCachePolicy) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                base_url: self.inner.base_url.clone(),
                http: self.inner.http.clone(),
                auth,
                policy,
                by_id: Mutex::new(HashMap::new()),
                id_order: Mutex::new(VecDeque::new()),
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
        self.cache_by_id(&schema);
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
        let body = serde_json::json!({
            "schema": definition.into(),
            "schemaType": schema_type.as_api(),
        });
        let response = self
            .request(reqwest::Method::POST, &format!("/subjects/{subject}"))
            .json(&body)
            .send()
            .await
            .map_err(KafkaError::schema_registry)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(KafkaError::InvalidOptions(format!(
                "the registry holds no such schema under subject {subject:?}; register it there \
                 first (the typed shorthands do it in one call) or point the producer at the \
                 subject that carries this schema",
            )));
        }
        let response = response
            .error_for_status()
            .map_err(KafkaError::schema_registry)?;
        let found: RegisterResponse = response.json().await.map_err(KafkaError::schema_registry)?;
        Ok(found.id)
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

    /// The bound on id-keyed entries, or `None` when nothing is remembered at all.
    fn capacity(&self) -> Option<NonZeroUsize> {
        match self.inner.policy {
            SchemaCachePolicy::Disabled => None,
            SchemaCachePolicy::Cached { capacity, .. } => Some(capacity),
        }
    }

    fn cache(&self, subject: &str, schema: &Arc<RegisteredSchema>) {
        self.cache_by_id(schema);
        if self.capacity().is_none() {
            return;
        }
        self.inner
            .by_subject
            .lock()
            .expect("subject cache mutex poisoned")
            .insert(
                subject.to_owned(),
                SubjectEntry {
                    id: schema.id,
                    resolved: Instant::now(),
                },
            );
    }

    /// Remembers one schema by its id, dropping the oldest once the bound is reached.
    fn cache_by_id(&self, schema: &Arc<RegisteredSchema>) {
        let Some(capacity) = self.capacity() else {
            return;
        };
        // The two schema maps are taken together and released before the parsed-schema maps are
        // touched, so no two of these locks are ever held at once.
        let mut by_id = self
            .inner
            .by_id
            .lock()
            .expect("schema cache mutex poisoned");
        let mut order = self
            .inner
            .id_order
            .lock()
            .expect("schema cache order mutex poisoned");
        if by_id.insert(schema.id, Arc::clone(schema)).is_none() {
            order.push_back(schema.id);
        }
        let mut evicted = Vec::new();
        while order.len() > capacity.get() {
            let Some(oldest) = order.pop_front() else {
                break;
            };
            by_id.remove(&oldest);
            evicted.push(oldest);
        }
        drop(order);
        drop(by_id);
        self.purge_parsed(&evicted);
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
        let ttl = match self.inner.policy {
            SchemaCachePolicy::Disabled => return None,
            SchemaCachePolicy::Cached { subject_ttl, .. } => subject_ttl,
        };
        // A subject's latest version moves, so an entry past its time is not an answer. The id it
        // named stays valid for ever and stays in its own cache; what expired is the claim that
        // this subject still points at it. The guard is released before the id cache is taken.
        let by_subject = self
            .inner
            .by_subject
            .lock()
            .expect("subject cache mutex poisoned");
        let fresh = by_subject.get(subject).and_then(|entry| {
            ttl.is_none_or(|ttl| entry.resolved.elapsed() < ttl)
                .then_some(entry.id)
        });
        drop(by_subject);
        self.cached_schema(fresh?)
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
/// A [`Codec`](ruststream::codec::Codec) is synchronous on both ends and a registry lookup is
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
/// // Every codec built here records its subject, so `connect` resolves it and a subject that
/// // is missing fails the app's startup rather than its first publish.
/// let codec = AvroCodec::registry(&prefetch, "confirmations-value");
/// let broker = KafkaBroker::new(["localhost:9092"]).schema_prefetch(prefetch);
/// # let _ = (codec, broker);
/// ```
#[derive(Clone)]
pub struct SchemaPrefetch {
    registry: SchemaRegistry,
    /// The subjects registry codecs were built against. Shared with the codecs' own clones, so
    /// naming a subject once - at the codec - is what puts it on this list.
    subjects: Arc<Mutex<HashSet<String>>>,
}

impl fmt::Debug for SchemaPrefetch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaPrefetch")
            .field("registry", &self.registry)
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
            subjects: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// The client the codecs share a cache with.
    #[must_use]
    pub fn registry(&self) -> &SchemaRegistry {
        &self.registry
    }

    /// Records a subject a codec will publish under, so [`warm_subjects`](Self::warm_subjects)
    /// resolves it.
    ///
    /// # Panics
    ///
    /// Panics when the internal mutex is poisoned, which requires a prior panic inside this
    /// type (an invariant violation, not an operational failure).
    pub(crate) fn record_subject(&self, subject: &str) {
        self.subjects
            .lock()
            .expect("prefetch subjects mutex poisoned")
            .insert(subject.to_owned());
    }

    /// Resolves every recorded subject. Called once, by the broker's `connect`, so a subject
    /// that does not exist fails the app's startup instead of its first publish.
    pub(crate) async fn warm_subjects(&self) -> Result<(), KafkaError> {
        let subjects: Vec<String> = self
            .subjects
            .lock()
            .expect("prefetch subjects mutex poisoned")
            .iter()
            .cloned()
            .collect();
        for subject in subjects {
            self.registry.warm(&subject).await.map_err(|err| {
                KafkaError::SchemaRegistry(
                    format!(
                        "the schema of subject {subject:?} could not be resolved at startup, so \
                         a codec publishing under it would have no id to frame with: {err}"
                    )
                    .into(),
                )
            })?;
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
/// let codec = SchemaFramed::new(&prefetch, "orders-value", JsonCodec);
/// # let _ = codec;
/// ```
#[derive(Debug, Clone)]
pub struct SchemaFramed<C> {
    registry: SchemaRegistry,
    subject: String,
    inner: C,
}

impl<C> SchemaFramed<C> {
    /// Frames `inner`'s payloads under the envelope of `subject`.
    ///
    /// Construction is synchronous and does no I/O: `subject` is recorded on `prefetch`, which
    /// resolves it when the broker connects.
    ///
    /// # Panics
    ///
    /// Panics when the prefetch's internal mutex is poisoned, which requires a prior panic
    /// inside it (an invariant violation, not an operational failure).
    #[must_use]
    pub fn new(prefetch: &SchemaPrefetch, subject: impl Into<String>, inner: C) -> Self {
        let subject = subject.into();
        prefetch.record_subject(&subject);
        Self {
            registry: prefetch.registry().clone(),
            subject,
            inner,
        }
    }

    /// The codec whose payloads travel inside the envelope.
    pub fn inner(&self) -> &C {
        &self.inner
    }
}

impl<C: Codec> Codec for SchemaFramed<C> {
    fn encode<T: serde::Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        let schema = self.registry.cached_subject(&self.subject).ok_or_else(|| {
            CodecError::Encode(Box::new(KafkaError::malformed(format!(
                "no schema is cached for subject {:?}: it was not resolved at startup. Attach \
                 the SchemaPrefetch this codec was built from to the broker \
                 (KafkaBroker::schema_prefetch), so connect resolves it",
                self.subject,
            ))))
        })?;
        let datum = self.inner.encode(value)?;
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
                 4-byte schema id) that subject {:?} publishes under; its first bytes are {:02x?}",
                self.subject,
                &bytes[..bytes.len().min(8)],
            ))))
        })?;
        self.inner.decode(datum)
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
