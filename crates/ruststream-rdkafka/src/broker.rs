//! The broker ladder: the unconnected handle, the connected form, and the terminal witness.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::producer::{BaseProducer, FutureProducer, Producer as _};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, DescribeServer, ServerSpec, Str, Subscribe,
    SubscriptionSource,
};
use tokio::task;

use crate::eos::EosSource;
use crate::error::KafkaError;
use crate::publisher::{KafkaPublish, KafkaPublisher};
use crate::subscriber::{DeliveredTopic, KafkaSubscriber};
use crate::subscription::{Commit, KafkaTopic, Reader, StartOffset, SubscriptionPlan};
use crate::tracker::{CommitTracker, TrackingContext};

/// The live client state behind [`ConnectedKafkaBroker`]: the shared producer every publisher
/// clones from, the resolved configurations subscriptions and transactional producers derive
/// from, and the registry of exactly-once sources.
pub(crate) struct ConnState {
    /// The shared producer, opened by the first publish. A service that only consumes leaves it
    /// empty and runs no producer client at all.
    producer: OnceLock<FutureProducer>,
    producer_config: ClientConfig,
    base_config: ClientConfig,
    default_group: Option<String>,
    flush_timeout: Duration,
    /// Subscriptions in `Commit::Transactional` mode, keyed by their pipeline id (the
    /// transactional id of the `EosPipeline` that commits their offsets).
    eos_sources: Mutex<HashMap<String, Vec<EosSource>>>,
    /// Flipped by `shutdown`. The ladder makes owner-side misuse a compile error, but handles
    /// that alias the connection (publishers paired earlier, clones of the connected form) are
    /// still reachable, so their liveness is the one part of the contract that stays dynamic.
    closed: AtomicBool,
    #[cfg(feature = "schema-registry")]
    schema_registry: Option<crate::schema_registry::SchemaRegistry>,
    #[cfg(feature = "schema-registry")]
    schema_prefetch: Option<crate::schema_registry::SchemaPrefetch>,
}

impl ConnState {
    /// The shared producer, opening it on the first ask.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Publish`] when librdkafka refuses to open the producer. The
    /// configuration itself was accepted at connect, so what is left here is a resource the
    /// client could not take.
    pub(crate) fn producer(&self) -> Result<&FutureProducer, KafkaError> {
        if let Some(producer) = self.producer.get() {
            return Ok(producer);
        }
        let opened: FutureProducer = self
            .producer_config
            .create()
            .map_err(|err| KafkaError::Publish(Box::new(err)))?;
        // A concurrent first publish may have won the cell; its producer is the one everything
        // uses from here, and the loser's is dropped unused.
        let _ = self.producer.set(opened);
        Ok(self.producer.get().expect("the cell holds a producer"))
    }

    pub(crate) fn producer_config(&self) -> &ClientConfig {
        &self.producer_config
    }

    /// Errors once the connection this handle aliases has been shut down, naming the topic the
    /// operation could not reach.
    pub(crate) fn ensure_open(&self, topic: &str) -> Result<(), KafkaError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(KafkaError::Closed {
                topic: topic.to_owned(),
            });
        }
        Ok(())
    }

    fn register_eos(&self, pipeline: &str, source: EosSource) {
        let mut sources = self
            .eos_sources
            .lock()
            .expect("eos source registry mutex poisoned");
        sources.entry(pipeline.to_owned()).or_default().push(source);
    }

    pub(crate) fn eos_sources(&self, pipeline: &str) -> Vec<EosSource> {
        let mut sources = self
            .eos_sources
            .lock()
            .expect("eos source registry mutex poisoned");
        // Prune entries whose subscriber is gone, so the registry does not grow with
        // re-subscriptions.
        sources
            .get_mut(pipeline)
            .map_or_else(Vec::new, |registered| {
                registered.retain(EosSource::alive);
                registered.clone()
            })
    }
}

impl fmt::Debug for ConnState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnState")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

/// An Apache Kafka broker backed by [`rdkafka`](https://docs.rs/rdkafka) / librdkafka.
///
/// This is the unconnected form: [`new`](Self::new) is synchronous and does no I/O, so a service
/// composes with the synchronous `#[ruststream::app]` builder. All network work happens in
/// [`Broker::connect`], which consumes this handle and yields the
/// [`ConnectedKafkaBroker`] witness - subscriptions and publishers exist only from there.
///
/// Configuration philosophy: options not set here mean the librdkafka defaults - this crate does
/// not impose its own. Anything not surfaced as a typed option is reachable through the raw
/// [`config`](Self::config) / [`producer_config`](Self::producer_config) /
/// [`KafkaTopic::config`](crate::KafkaTopic::config) passthroughs.
///
/// # Examples
///
/// ```no_run
/// use ruststream_rdkafka::KafkaBroker;
///
/// let broker = KafkaBroker::new(["localhost:9092"])
///     .default_group("orders-svc")
///     .config("client.id", "orders-svc");
/// # let _ = broker;
/// ```
#[derive(Debug, Clone)]
pub struct KafkaBroker {
    servers: Vec<String>,
    default_group: Option<String>,
    client_config: Vec<(String, String)>,
    producer_config: Vec<(String, String)>,
    connect_timeout: Duration,
    flush_timeout: Duration,
    #[cfg(feature = "schema-registry")]
    schema_registry: Option<crate::schema_registry::SchemaRegistry>,
    #[cfg(feature = "schema-registry")]
    schema_prefetch: Option<crate::schema_registry::SchemaPrefetch>,
}

impl KafkaBroker {
    /// Records the bootstrap servers; no I/O happens until [`Broker::connect`].
    ///
    /// Each entry is a `host` or `host:port` seed the client bootstraps from.
    #[must_use]
    pub fn new<I, S>(servers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            servers: servers.into_iter().map(Into::into).collect(),
            default_group: None,
            client_config: Vec::new(),
            producer_config: Vec::new(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            flush_timeout: DEFAULT_FLUSH_TIMEOUT,
            #[cfg(feature = "schema-registry")]
            schema_registry: None,
            #[cfg(feature = "schema-registry")]
            schema_prefetch: None,
        }
    }

    /// The consumer group used by subscriptions that do not set one themselves
    /// ([`KafkaTopic::group`](crate::KafkaTopic::group) overrides it per subscription).
    ///
    /// Joining a group is what the bare-string `#[subscriber("orders")]` form does, so it needs
    /// this; a subscription that ends up with no group at all is a startup error. A subscription
    /// that names its partitions with [`KafkaPartitions`](crate::KafkaPartitions) joins no group
    /// and needs none.
    #[must_use]
    pub fn default_group(mut self, group: impl Into<String>) -> Self {
        self.default_group = Some(group.into());
        self
    }

    /// Raw librdkafka property passthrough applied to every client this broker creates
    /// (consumers and the producer). Keys that only apply to one side are ignored by the other,
    /// exactly as librdkafka does.
    #[must_use]
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.client_config.push((key.into(), value.into()));
        self
    }

    /// Raw librdkafka property passthrough applied to the producer only, on top of
    /// [`config`](Self::config) (for example `acks` or `message.timeout.ms`).
    #[must_use]
    pub fn producer_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.producer_config.push((key.into(), value.into()));
        self
    }

    /// How long [`Broker::connect`] waits for the cluster-reachability probe (a metadata fetch)
    /// before failing startup. Defaults to 30 seconds. This is this crate's own fail-fast
    /// window, not a librdkafka property.
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// How long [`ConnectedBroker::shutdown`] waits for in-flight publishes to flush before
    /// reporting failure. Defaults to 30 seconds.
    #[must_use]
    pub fn flush_timeout(mut self, timeout: Duration) -> Self {
        self.flush_timeout = timeout;
        self
    }

    /// Attaches a [`SchemaRegistry`](crate::schema_registry::SchemaRegistry) client to the
    /// consume edge: every subscription transcodes Confluent-framed deliveries to plain JSON
    /// on its (async) delivery path, before they reach the synchronous codec - so handlers
    /// stay ordinary serde types on the default `json` codec, streams and batches alike.
    /// Non-framed payloads pass through untouched. The client is shared; clones see one
    /// cache. The publish-side counterpart is the
    /// [`SchemaFrame`](crate::schema_registry::SchemaFrame) publish middleware, added
    /// app-wide with `RustStream::publish_layer`.
    #[cfg(feature = "schema-registry")]
    #[must_use]
    pub fn schema_registry(mut self, registry: crate::schema_registry::SchemaRegistry) -> Self {
        self.schema_registry = Some(registry);
        self
    }

    /// Attaches a [`SchemaPrefetch`](crate::schema_registry::SchemaPrefetch), the async half of
    /// a registry-backed codec: [`connect`](ruststream::Broker::connect) resolves the subjects
    /// its codecs publish under, and every subscription resolves the writer schema an arriving
    /// envelope names - both before the synchronous codec runs, which is the only way a sync
    /// `encode` / `decode` can reach an async registry without blocking a runtime worker.
    ///
    /// Deliveries are not touched: this attachment fills a cache and nothing else. It is
    /// therefore the opposite of [`schema_registry`](Self::schema_registry), which rewrites
    /// framed deliveries into JSON for the transcoding compatibility path. The two are
    /// alternatives, not layers: with both attached the transcode would hand a JSON document to
    /// a codec expecting the wire format, so the prefetch runs first and still sees the
    /// envelope, but the pairing is a configuration mistake either way.
    #[cfg(feature = "schema-registry")]
    #[must_use]
    pub fn schema_prefetch(mut self, prefetch: crate::schema_registry::SchemaPrefetch) -> Self {
        self.schema_prefetch = Some(prefetch);
        self
    }

    fn base_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", self.servers.join(","));
        for (key, value) in &self.client_config {
            config.set(key, value);
        }
        config
    }
}

impl Broker for KafkaBroker {
    type Error = KafkaError;
    type Connected = ConnectedKafkaBroker;

    /// Creates the shared producer and probes the cluster with a metadata fetch, so an
    /// unreachable or misconfigured cluster fails startup instead of the first publish.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] when no bootstrap server was given and
    /// [`KafkaError::Connect`] when the client cannot be created or the probe fails within
    /// [`connect_timeout`](Self::connect_timeout).
    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        if self.servers.is_empty() {
            return Err(KafkaError::InvalidOptions(
                "at least one bootstrap server is required".to_owned(),
            ));
        }
        let base_config = self.base_config();
        let mut producer_config = base_config.clone();
        for (key, value) in &self.producer_config {
            producer_config.set(key, value);
        }
        // The probe is a client of its own, built from the producer's configuration and dropped
        // with this call: it answers whether the cluster is reachable and, on the way, whether
        // librdkafka accepts the publish settings, so a refused property still fails here rather
        // than at some publish later in the service's life. What a consume-only service must not
        // do is keep a producer open for the life of the connection; that one is opened by the
        // first publish.
        let probe: BaseProducer = producer_config.create().map_err(KafkaError::connect)?;

        // fetch_metadata blocks, so it runs on the blocking pool.
        let timeout = self.connect_timeout;
        task::spawn_blocking(move || {
            let reached = probe.client().fetch_metadata(None, timeout);
            drop(probe);
            reached
        })
        .await
        .map_err(|err| KafkaError::Connect(Box::new(err)))?
        .map_err(KafkaError::connect)?;

        // Every subject a registry codec publishes under, resolved here rather than on the first
        // publish: the codec's own encode is synchronous, and a subject that does not exist
        // should stop the app coming up rather than surface as one failed message later.
        #[cfg(feature = "schema-registry")]
        if let Some(prefetch) = &self.schema_prefetch {
            prefetch.warm_subjects().await?;
        }

        let state = Arc::new(ConnState {
            producer: OnceLock::new(),
            producer_config,
            base_config,
            default_group: self.default_group,
            flush_timeout: self.flush_timeout,
            eos_sources: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            #[cfg(feature = "schema-registry")]
            schema_registry: self.schema_registry,
            #[cfg(feature = "schema-registry")]
            schema_prefetch: self.schema_prefetch,
        });
        Ok(ConnectedKafkaBroker { state })
    }
}

impl DescribeServer for KafkaBroker {
    /// The bootstrap coordinate clients connect to, one `host:port` per configured address.
    ///
    /// Each address goes through [`ServerSpec::host_from_url`], so a `PLAINTEXT://` or
    /// `SASL_SSL://` prefix, and any userinfo an address carries, stay out of the generated
    /// document: it is published, and a credential that reaches it has left the service.
    fn describe_server(&self) -> ServerSpec {
        let hosts: Vec<String> = self
            .servers
            .iter()
            .map(|server| ServerSpec::host_from_url(server))
            .collect();
        let spec = ServerSpec::new(hosts.join(","), "kafka");
        // `protocol_version` stays unset: the Kafka protocol negotiates its version per API key
        // between the client and the cluster, so no one number describes what clients speak here.
        #[cfg(feature = "asyncapi")]
        let spec = spec.bindings(crate::bindings::server(self.registry_url()));
        spec
    }
}

#[cfg(feature = "asyncapi")]
impl KafkaBroker {
    /// The schema registry's coordinate, for the server binding. `None` without a registry, and
    /// without the feature that speaks to one.
    fn registry_url(&self) -> Option<&str> {
        #[cfg(feature = "schema-registry")]
        {
            self.schema_registry
                .as_ref()
                .and_then(crate::schema_registry::SchemaRegistry::base_url)
        }
        #[cfg(not(feature = "schema-registry"))]
        {
            None
        }
    }
}

/// The connected form of [`KafkaBroker`]: the typed witness that [`Broker::connect`] succeeded.
///
/// Everything connection-bound hangs off this handle - subscriptions
/// ([`KafkaTopic`](crate::KafkaTopic), the [`Subscribe`] capability) and live publishers (paired
/// from a [`KafkaPublish`] policy).
///
/// [`ConnectedBroker::shutdown`] consumes the handle, so publishing or subscribing afterwards is
/// a compile error for its owner; handles that alias the connection (publishers paired earlier,
/// subscribers still open) report [`KafkaError::Closed`] instead of succeeding against a dead
/// connection.
///
/// # Examples
///
/// ```no_run
/// use ruststream::{Broker, ConnectedBroker};
/// use ruststream_rdkafka::{KafkaBroker, KafkaPublish};
///
/// # async fn demo() -> Result<(), ruststream_rdkafka::KafkaError> {
/// let connected = KafkaBroker::new(["localhost:9092"]).connect().await?;
/// let publisher = connected.publisher(KafkaPublish::default());
/// let _closed = connected.shutdown().await?;
/// # let _ = publisher;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ConnectedKafkaBroker {
    state: Arc<ConnState>,
}

impl ConnectedKafkaBroker {
    /// A live publisher on the shared producer, configured by `policy`.
    ///
    /// The declaration-side counterpart of `policy.pair(&connected)`; use the policy form at
    /// include sites and `after_startup` hooks, this one when you already hold the connected
    /// broker.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream::Broker;
    /// use ruststream_rdkafka::{KafkaBroker, KafkaPublish};
    ///
    /// # async fn demo() -> Result<(), ruststream_rdkafka::KafkaError> {
    /// let connected = KafkaBroker::new(["localhost:9092"]).connect().await?;
    /// let publisher = connected.publisher(KafkaPublish::default());
    /// # let _ = publisher;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn publisher(&self, policy: KafkaPublish) -> KafkaPublisher {
        KafkaPublisher::new(Arc::clone(&self.state), policy.queue_timeout_setting())
    }

    pub(crate) fn state(&self) -> &Arc<ConnState> {
        &self.state
    }

    /// Opens a subscription for `def`, whichever of this crate's descriptors it is.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Closed`] once the connection this handle aliases has been shut
    /// down, [`KafkaError::InvalidOptions`] when neither the descriptor nor the broker names a
    /// consumer group (or the descriptor's own options do not hold together), and
    /// [`KafkaError::Subscribe`] when the consumer cannot be created or the subscription is
    /// rejected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream::Broker;
    /// use ruststream_rdkafka::{KafkaBroker, KafkaTopic};
    ///
    /// # async fn demo() -> Result<(), ruststream_rdkafka::KafkaError> {
    /// let connected = KafkaBroker::new(["localhost:9092"]).connect().await?;
    /// let subscriber = connected
    ///     .subscribe_with(KafkaTopic::new("orders").group("orders-svc"))
    ///     .await?;
    /// # let _ = subscriber;
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscribe_with<Source>(
        &self,
        def: Source,
    ) -> impl Future<Output = Result<KafkaSubscriber, KafkaError>>
    where
        Source: SubscriptionSource<Self, Subscriber = KafkaSubscriber>,
    {
        def.subscribe(self)
    }

    /// Opens the consumer a resolved descriptor asks for. The descriptors validate themselves
    /// before they get here, so what is left is what needs the broker: the default group, the
    /// base config, and the combinations manual assignment cannot honor.
    pub(crate) fn open(&self, plan: SubscriptionPlan) -> Result<KafkaSubscriber, KafkaError> {
        self.state.ensure_open(&plan.name)?;
        let manual = matches!(plan.reader, Reader::Assigned { .. });
        let group = plan
            .settings
            .group
            .clone()
            .or_else(|| self.state.default_group.clone());
        let group = match group {
            Some(group) => Some(group),
            // Manual assignment needs no group membership; everything else does.
            None if manual => None,
            None => {
                return Err(KafkaError::InvalidOptions(format!(
                    "subscription to {:?} has no consumer group: name one on the descriptor or \
                     set `KafkaBroker::default_group`",
                    plan.name,
                )));
            }
        };
        if manual {
            validate_manual_assignment(&plan, group.as_deref())?;
        }

        let def = &plan.settings;
        let mut config = self.state.base_config.clone();
        if let Some(group) = &group {
            config.set("group.id", group);
        } else {
            // librdkafka requires a group.id even for assign(); an assign-only consumer
            // never joins the group protocol and, with auto-commit off, never commits, so
            // this placeholder id stays inert broker-side.
            config.set("group.id", "ruststream.standalone");
            config.set("enable.auto.commit", "false");
        }
        match def.start {
            StartOffset::Committed => {}
            StartOffset::Earliest => {
                config.set("auto.offset.reset", "earliest");
            }
            StartOffset::Latest => {
                config.set("auto.offset.reset", "latest");
            }
        }
        if let Some(assignment) = def.assignment {
            config.set(
                "partition.assignment.strategy",
                assignment.as_config_value(),
            );
        }
        match &def.commit {
            Commit::Auto => {}
            Commit::Tracked => {
                config.set("enable.auto.offset.store", "false");
            }
            Commit::Transactional(_) => {
                // The pipeline's producer transaction owns the offsets: the consumer must
                // neither store nor commit them on its own.
                config.set("enable.auto.offset.store", "false");
                config.set("enable.auto.commit", "false");
            }
        }
        // The raw passthrough is applied last on purpose: it wins over the typed options.
        for (key, value) in &def.config {
            config.set(key, value);
        }

        let tracker = Arc::new(CommitTracker::default());
        let context = TrackingContext::new(Arc::clone(&tracker), &plan.name);
        let consumer: StreamConsumer<TrackingContext> = config
            .create_with_context(context)
            .map_err(KafkaError::subscribe)?;
        // Decided here, where the reader still says what was subscribed: a delivery of a
        // subscription that reads one literal topic needs no name off the record.
        let delivered_topic = match &plan.reader {
            Reader::Assigned { topic, partitions } => {
                assign_partitions(&consumer, topic, partitions, def.start)?;
                DeliveredTopic::One(Str::from(topic.as_str()))
            }
            Reader::Subscribed(names) => {
                let literal = match names.as_slice() {
                    [only] if !only.starts_with('^') => Some(Str::from(only.as_str())),
                    _ => None,
                };
                let borrowed: Vec<&str> = names.iter().map(String::as_str).collect();
                consumer
                    .subscribe(&borrowed)
                    .map_err(KafkaError::subscribe)?;
                literal.map_or(DeliveredTopic::PerRecord(None), DeliveredTopic::One)
            }
        };

        let consumer = Arc::new(consumer);
        if let Commit::Transactional(pipeline) = &def.commit {
            self.state
                .register_eos(pipeline, EosSource::new(&tracker, &consumer));
        }
        let settings = plan.settings;
        let subscriber = KafkaSubscriber::new(
            consumer,
            plan.name,
            delivered_topic,
            settings.commit,
            tracker,
            settings.lane_key,
        );
        #[cfg(feature = "schema-registry")]
        let subscriber = subscriber
            .with_schema_registry(self.state.schema_registry.clone())
            .with_schema_prefetch(self.state.schema_prefetch.clone());
        Ok(subscriber)
    }
}

impl ConnectedBroker for ConnectedKafkaBroker {
    type Error = KafkaError;
    type Closed = ClosedKafkaBroker;

    /// Flushes in-flight publishes and closes the connection; consumers close when their
    /// subscribers drop.
    ///
    /// Consuming `self` makes any further use of this handle a compile error. Handles that
    /// alias the connection report [`KafkaError::Closed`] from here on.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Publish`] when in-flight records were not delivered within
    /// [`KafkaBroker::flush_timeout`].
    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        // Closed before the flush: a publish racing the teardown must not enter a queue nobody
        // will drain afterwards.
        self.state.closed.store(true, Ordering::Release);
        // Nothing published through this connection, so there is nothing to flush.
        let Some(producer) = self.state.producer.get().cloned() else {
            return Ok(ClosedKafkaBroker { unflushed: 0 });
        };
        let timeout = self.state.flush_timeout;
        // flush blocks (it polls the producer), so it runs on the blocking pool.
        let unflushed = task::spawn_blocking(move || {
            producer.flush(timeout)?;
            Ok::<_, rdkafka::error::KafkaError>(producer.in_flight_count())
        })
        .await
        .map_err(|err| KafkaError::Publish(Box::new(err)))?
        .map_err(KafkaError::publish)?;
        Ok(ClosedKafkaBroker { unflushed })
    }
}

/// The terminal witness returned by [`ConnectedBroker::shutdown`].
///
/// Has no publish or subscribe surface; it carries the teardown diagnostics as plain data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedKafkaBroker {
    unflushed: i32,
}

impl ClosedKafkaBroker {
    /// How many records librdkafka still held when the connection closed. Zero after a clean
    /// flush; a non-zero count means the producer queue outlived the flush call.
    #[must_use]
    pub fn unflushed_records(&self) -> i32 {
        self.unflushed
    }
}

impl Subscribe for ConnectedKafkaBroker {
    type Subscriber = KafkaSubscriber;

    /// A bare name is one topic, and a record produced to a topic reaches every group reading
    /// it, so `#[subscriber("orders")]` addresses its own deferred `retry_after` copies. A
    /// `^`-anchored name is a librdkafka topic regex and is refused here; subscribe to a pattern
    /// with [`KafkaTopics::pattern`](crate::KafkaTopics::pattern).
    type Copies = AddressedCopies;

    /// Subscribes to the topic `name` with descriptor defaults; requires
    /// [`KafkaBroker::default_group`].
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_with(KafkaTopic::new(name)).await
    }
}

/// The option combinations manual assignment cannot honor, failed at subscribe time.
fn validate_manual_assignment(
    plan: &SubscriptionPlan,
    group: Option<&str>,
) -> Result<(), KafkaError> {
    let def = &plan.settings;
    if matches!(def.commit, Commit::Transactional(_)) {
        return Err(KafkaError::InvalidOptions(
            "manual partition assignment does not compose with `Commit::Transactional`: an \
             EOS pipeline commits through the consumer group protocol"
                .to_owned(),
        ));
    }
    if group.is_none() {
        if def.commit == Commit::Tracked {
            return Err(KafkaError::InvalidOptions(
                "`Commit::Tracked` needs a group to commit into; name one with \
                 `KafkaPartitions::group` or drop the commit mode for a group-less reader"
                    .to_owned(),
            ));
        }
        if def.start == StartOffset::Committed {
            return Err(KafkaError::InvalidOptions(
                "a group-less manual assignment has no committed offsets to start from; set \
                 `start(StartOffset::Earliest)` or `Latest`, or name a group"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

/// `assign()`s the descriptor's exact partitions with their start offsets: `Stored` resumes
/// from the group's committed positions (falling back to `auto.offset.reset`);
/// `Beginning`/`End` are the explicit group-less starts.
fn assign_partitions(
    consumer: &StreamConsumer<TrackingContext>,
    topic: &str,
    partitions: &[i32],
    start: StartOffset,
) -> Result<(), KafkaError> {
    let offset = match start {
        StartOffset::Committed => Offset::Stored,
        StartOffset::Earliest => Offset::Beginning,
        StartOffset::Latest => Offset::End,
    };
    let mut assignment = TopicPartitionList::new();
    for partition in partitions {
        assignment
            .add_partition_offset(topic, *partition, offset)
            .map_err(KafkaError::subscribe)?;
    }
    consumer.assign(&assignment).map_err(KafkaError::subscribe)
}

#[cfg(test)]
mod tests {
    use ruststream::{OutgoingMessage, Publisher as _};

    use super::*;

    #[test]
    fn construction_is_synchronous_and_io_free() {
        let broker = KafkaBroker::new(["a:9092", "b:9092"]).default_group("g");
        assert_eq!(
            broker.describe_server().host.as_deref(),
            Some("a:9092,b:9092")
        );
        assert_eq!(broker.describe_server().protocol, "kafka");
    }

    /// The stand's address, or `None` to skip - the gate the live suites in `tests/` use, in the
    /// three lines it takes here: `just test-brokers` sets both variables, and a job that stood a
    /// cluster up and then lost its address would otherwise report a skip as a pass.
    fn live_url() -> Option<String> {
        match std::env::var("KAFKA_TEST_URL") {
            Ok(url) if !url.is_empty() => Some(url),
            _ => {
                assert!(
                    !std::env::var("RUSTSTREAM_REQUIRE_LIVE").is_ok_and(|flag| !flag.is_empty()),
                    "RUSTSTREAM_REQUIRE_LIVE is set, so this test must run, but KAFKA_TEST_URL \
                     is unset or empty",
                );
                None
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_connection_opens_no_producer_until_something_publishes() {
        let Some(url) = live_url() else { return };
        let broker = KafkaBroker::new([url]).connect().await.expect("connect");
        assert!(
            broker.state.producer.get().is_none(),
            "connecting must open no producer: a service that only consumes never publishes, and \
             a producer is a client, its threads and a connection to the cluster",
        );

        let publisher = broker.publisher(KafkaPublish::default());
        publisher
            .publish(
                OutgoingMessage::new("ruststream-lazy-producer", b"one".as_slice()),
                None,
            )
            .await
            .expect("publish");
        assert!(
            broker.state.producer.get().is_some(),
            "the first publish opens the producer",
        );

        broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn connect_with_no_servers_fails_fast() {
        let err = KafkaBroker::new(Vec::<String>::new())
            .connect()
            .await
            .unwrap_err();
        assert!(matches!(err, KafkaError::InvalidOptions(_)));
    }
}
