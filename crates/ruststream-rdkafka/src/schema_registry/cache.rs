//! What a registry client remembers: the seam, its policy, and the default implementation.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::RegisteredSchema;

/// Where a [`SchemaRegistry`](super::SchemaRegistry) keeps what it has resolved.
///
/// The trait is deliberately synchronous on every method. A [`Codec`](ruststream::codec::Codec)
/// reads this cache while encoding or decoding a message, and a codec is synchronous, so an
/// implementation that had to await here would have nowhere to do it and would end up blocking a
/// runtime worker. Fetching is the other trait's job
/// ([`RegistryClient`](super::RegistryClient), which is async); this one only remembers. An
/// implementation backed by a remote store therefore belongs behind `RegistryClient`, not here.
///
/// Implementations are shared across threads and across clones of the client, so they take
/// `&self` and do their own interior locking.
///
/// # Examples
///
/// A cache that remembers ids and forgets subjects, in the three methods the trait asks for:
///
/// ```
/// use std::collections::HashMap;
/// use std::sync::{Arc, Mutex};
///
/// use ruststream_rdkafka::schema_registry::{RegisteredSchema, SchemaCache};
///
/// #[derive(Default)]
/// struct IdsOnly(Mutex<HashMap<u32, Arc<RegisteredSchema>>>);
///
/// impl SchemaCache for IdsOnly {
///     fn schema(&self, id: u32) -> Option<Arc<RegisteredSchema>> {
///         self.0.lock().expect("not poisoned").get(&id).cloned()
///     }
///
///     fn subject(&self, _subject: &str) -> Option<Arc<RegisteredSchema>> {
///         None
///     }
///
///     fn store(&self, _subject: Option<&str>, schema: &Arc<RegisteredSchema>) {
///         self.0
///             .lock()
///             .expect("not poisoned")
///             .insert(schema.id(), Arc::clone(schema));
///     }
/// }
///
/// # fn check() {
/// let cache = IdsOnly::default();
/// assert!(cache.schema(7).is_none());
/// # }
/// # check();
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a schema cache",
    note = "a cache answers `schema(id)` and `subject(name)` synchronously and takes new entries \
            through `store`; fetching is `RegistryClient`'s job, so nothing here may await"
)]
pub trait SchemaCache: Send + Sync + 'static {
    /// The schema remembered under `id`, if any.
    ///
    /// A schema id is immutable, so an entry here can only be absent, never wrong.
    fn schema(&self, id: u32) -> Option<Arc<RegisteredSchema>>;

    /// The schema remembered as `subject`'s current version, if any and still current.
    ///
    /// A subject's latest version moves, so an implementation that ages entries returns `None`
    /// once its own policy says the answer is no longer trustworthy.
    fn subject(&self, subject: &str) -> Option<Arc<RegisteredSchema>>;

    /// Takes a freshly resolved schema, and the subject it was resolved through when it was one.
    ///
    /// `subject` is `None` for a lookup by id, which says nothing about any subject.
    fn store(&self, subject: Option<&str>, schema: &Arc<RegisteredSchema>);

    /// The ids this cache has dropped since the last call, so forms derived from them (a parsed
    /// Avro schema, a compiled descriptor pool) can be dropped with them.
    ///
    /// The default reports none, which is right for a cache that never drops anything.
    fn evicted_ids(&self) -> Vec<u32> {
        Vec::new()
    }
}

/// How the default cache ages and bounds what it holds.
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
/// thing the `subject_ttl` of [`Cached`](Self::Cached) governs. Confluent's own clients scope
/// their `latest.cache.ttl.sec` to exactly the latest-version caches for the same reason.
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
    /// worth knowing before choosing it: a synchronous codec reads the cache and cannot await a
    /// miss, so [`AvroCodec::registry`](crate::avro::AvroCodec::registry) and
    /// [`SchemaFramed`](super::SchemaFramed) cannot work under it. The transcoding path, whose
    /// lookups are all `async`, works unchanged.
    Disabled,
    /// Ids are remembered up to `capacity`, and a subject's resolved version for `subject_ttl`.
    Cached {
        /// The most id-keyed schemas held at once; the oldest is dropped past it.
        capacity: NonZeroUsize,
        /// How long a subject's resolved version is trusted before the next `async` lookup
        /// re-resolves it. `None` never re-resolves, which is Confluent's own default.
        ///
        /// This governs the `async` lookups only ([`warm`](super::SchemaRegistry::warm) and the
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

/// The default [`SchemaCache`]: in-process maps governed by a [`SchemaCachePolicy`].
pub struct MemorySchemaCache {
    policy: SchemaCachePolicy,
    by_id: Mutex<HashMap<u32, Arc<RegisteredSchema>>>,
    /// The ids in `by_id`, in the order they were inserted, so the cap can drop the oldest.
    id_order: Mutex<VecDeque<u32>>,
    by_subject: Mutex<HashMap<String, SubjectEntry>>,
    /// Called for every id this cache drops, so the client can forget its parsed form too.
    on_evict: Mutex<Vec<u32>>,
}

impl fmt::Debug for MemorySchemaCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemorySchemaCache")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl MemorySchemaCache {
    /// A cache under `policy`.
    #[must_use]
    pub fn new(policy: SchemaCachePolicy) -> Self {
        Self {
            policy,
            by_id: Mutex::new(HashMap::new()),
            id_order: Mutex::new(VecDeque::new()),
            by_subject: Mutex::new(HashMap::new()),
            on_evict: Mutex::new(Vec::new()),
        }
    }

    /// The policy this cache runs under.
    #[must_use]
    pub fn policy(&self) -> SchemaCachePolicy {
        self.policy
    }

    /// The bound on id-keyed entries, or `None` when nothing is remembered at all.
    fn capacity(&self) -> Option<NonZeroUsize> {
        match self.policy {
            SchemaCachePolicy::Disabled => None,
            SchemaCachePolicy::Cached { capacity, .. } => Some(capacity),
        }
    }
}

impl SchemaCache for MemorySchemaCache {
    fn schema(&self, id: u32) -> Option<Arc<RegisteredSchema>> {
        self.by_id
            .lock()
            .expect("schema cache mutex poisoned")
            .get(&id)
            .cloned()
    }

    fn subject(&self, subject: &str) -> Option<Arc<RegisteredSchema>> {
        let ttl = match self.policy {
            SchemaCachePolicy::Disabled => return None,
            SchemaCachePolicy::Cached { subject_ttl, .. } => subject_ttl,
        };
        // A subject's latest version moves, so an entry past its time is not an answer. The id it
        // named stays valid for ever and stays in its own map; what expired is the claim that
        // this subject still points at it. The guard is released before the id map is taken.
        let by_subject = self
            .by_subject
            .lock()
            .expect("subject cache mutex poisoned");
        let fresh = by_subject.get(subject).and_then(|entry| {
            ttl.is_none_or(|ttl| entry.resolved.elapsed() < ttl)
                .then_some(entry.id)
        });
        drop(by_subject);
        self.schema(fresh?)
    }

    fn store(&self, subject: Option<&str>, schema: &Arc<RegisteredSchema>) {
        let Some(capacity) = self.capacity() else {
            return;
        };
        // The two id maps are taken together and released before the subject map, so no two of
        // these locks are ever held at once.
        let mut by_id = self.by_id.lock().expect("schema cache mutex poisoned");
        let mut order = self
            .id_order
            .lock()
            .expect("schema cache order mutex poisoned");
        if by_id.insert(schema.id(), Arc::clone(schema)).is_none() {
            order.push_back(schema.id());
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
        if !evicted.is_empty() {
            self.on_evict
                .lock()
                .expect("evicted list mutex poisoned")
                .extend(evicted);
        }
        if let Some(subject) = subject {
            self.by_subject
                .lock()
                .expect("subject cache mutex poisoned")
                .insert(
                    subject.to_owned(),
                    SubjectEntry {
                        id: schema.id(),
                        resolved: Instant::now(),
                    },
                );
        }
    }

    fn evicted_ids(&self) -> Vec<u32> {
        std::mem::take(&mut *self.on_evict.lock().expect("evicted list mutex poisoned"))
    }
}
