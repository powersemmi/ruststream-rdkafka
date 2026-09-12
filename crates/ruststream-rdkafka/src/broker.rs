//! The broker ladder: the unconnected handle, the connected form, and the terminal witness.

use std::collections::HashMap;
use std::fmt;
use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::producer::{FutureProducer, Producer as _};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use ruststream::{Broker, ConnectedBroker, DescribeServer, ServerSpec, Subscribe};
use tokio::task;

use crate::eos::EosSource;
use crate::error::KafkaError;
use crate::publisher::{KafkaPublish, KafkaPublisher, KafkaRetryPublisher};
use crate::retry::RetryContext;
use crate::subscriber::KafkaSubscriber;
use crate::topic::{Commit, KafkaTopic, StartOffset};
use crate::tracker::{CommitTracker, TrackingContext};

/// The live client state behind [`ConnectedKafkaBroker`]: the shared producer every publisher
/// clones from, the resolved configurations subscriptions and transactional producers derive
/// from, and the registry of exactly-once sources.
pub(crate) struct ConnState {
    producer: FutureProducer,
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
    pub(crate) fn producer(&self) -> &FutureProducer {
        &self.producer
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

/// The cell an unconnected broker hands to its [`KafkaRetryPublisher`], filled once
/// [`Broker::connect`] has the live state. See that type for why this one path is cell-backed.
pub(crate) type EarlyConn = Arc<OnceLock<Arc<ConnState>>>;

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
    /// Filled by [`Broker::connect`], read only by [`KafkaRetryPublisher`]; clones of this
    /// broker share it, so the publisher a clone handed out still comes alive.
    early_conn: EarlyConn,
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
            early_conn: Arc::new(OnceLock::new()),
            #[cfg(feature = "schema-registry")]
            schema_registry: None,
            #[cfg(feature = "schema-registry")]
            schema_prefetch: None,
        }
    }

    /// A publisher for builder-time wiring that needs a live
    /// [`Publisher`](ruststream::Publisher) before the broker is connected - today that is
    /// [`BrokerScope::retry_via`](ruststream::runtime::BrokerScope::retry_via), the deferred
    /// republish behind `retry_after` on a broker without native delayed redelivery, which
    /// Kafka is.
    ///
    /// It exists for exactly that: a publish policy cannot be used there, because `retry_via`
    /// takes a publisher and a publisher may not exist before its connection does. This handle
    /// is the narrow exception, backed by a cell [`Broker::connect`] fills; the regular publish
    /// path stays policy-first and never sees a cell. Records go out through the broker's
    /// shared producer, on the broker's producer configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream_rdkafka::KafkaBroker;
    ///
    /// # fn demo() -> impl App {
    /// let broker = KafkaBroker::new(["localhost:9092"]).default_group("payments-svc");
    /// RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
    ///     let retries = b.broker().retry_publisher();
    ///     b.retry_via(retries);
    /// })
    /// # }
    /// ```
    #[must_use]
    pub fn retry_publisher(&self) -> KafkaRetryPublisher {
        KafkaRetryPublisher::new(Arc::clone(&self.early_conn))
    }

    /// The consumer group used by subscriptions that do not set one themselves
    /// ([`KafkaTopic::group`](crate::KafkaTopic::group) overrides it per subscription).
    ///
    /// Kafka requires a group to subscribe, so the bare-string `#[subscriber("orders")]` form
    /// needs this; a subscription that ends up with no group at all is a startup error.
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
        let producer: FutureProducer = producer_config.create().map_err(KafkaError::connect)?;

        // fetch_metadata blocks, so it runs on the blocking pool.
        let probe = producer.clone();
        let timeout = self.connect_timeout;
        task::spawn_blocking(move || probe.client().fetch_metadata(None, timeout))
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
            producer,
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
        // Brings any early publisher handed out before this call alive. A second connect of a
        // clone lineage leaves the first connection in the cell rather than swapping it, so an
        // early publisher keeps publishing through the connection it was minted against.
        let _ = self.early_conn.set(Arc::clone(&state));
        Ok(ConnectedKafkaBroker { state })
    }
}

impl DescribeServer for KafkaBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new(self.servers.join(","), "kafka")
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

    /// Opens a subscription for `def`: one consumer joining `def`'s group on `def`'s topic(s)
    /// or pattern.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Closed`] once the connection this handle aliases has been shut
    /// down, [`KafkaError::InvalidOptions`] when neither the descriptor nor the broker names a
    /// consumer group (or the descriptor's pattern is not `^`-anchored), and
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
    pub fn subscribe_with(
        &self,
        def: KafkaTopic,
    ) -> impl Future<Output = Result<KafkaSubscriber, KafkaError>> {
        // librdkafka joins the group in the background, so opening a subscription never awaits;
        // the async surface is what the SubscriptionSource contract and the call sites expect.
        ready(self.open_subscription(def))
    }

    /// The synchronous body behind [`subscribe_with`](Self::subscribe_with), kept apart so the
    /// error paths stay `?` rather than a chain of early `ready(Err(..))` returns.
    fn open_subscription(&self, def: KafkaTopic) -> Result<KafkaSubscriber, KafkaError> {
        self.state.ensure_open(def.topic())?;
        def.validate()?;
        let manual = !def.assigned_partitions().is_empty();
        let group = def
            .group_or(self.state.default_group.as_deref())
            .map(str::to_owned);
        let group = match group {
            Some(group) => Some(group),
            // Manual assignment needs no group membership; everything else does.
            None if manual => None,
            None => {
                return Err(KafkaError::InvalidOptions(format!(
                    "subscription to {:?} has no consumer group: set `KafkaTopic::group` or \
                     `KafkaBroker::default_group`",
                    def.topic(),
                )));
            }
        };
        if manual {
            validate_manual_assignment(&def, group.as_deref())?;
        }

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
        match def.start_offset() {
            StartOffset::Committed => {}
            StartOffset::Earliest => {
                config.set("auto.offset.reset", "earliest");
            }
            StartOffset::Latest => {
                config.set("auto.offset.reset", "latest");
            }
        }
        if let Some(assignment) = def.assignment_strategy() {
            config.set(
                "partition.assignment.strategy",
                assignment.as_config_value(),
            );
        }
        match def.commit_mode() {
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
        for (key, value) in def.config_entries() {
            config.set(key, value);
        }

        let tracker = Arc::new(CommitTracker::default());
        let context = TrackingContext::new(Arc::clone(&tracker));
        let consumer: StreamConsumer<TrackingContext> = config
            .create_with_context(context)
            .map_err(KafkaError::subscribe)?;
        if manual {
            assign_partitions(&consumer, &def)?;
        } else {
            let names: Vec<&str> = def.subscribed_topics().iter().map(String::as_str).collect();
            consumer.subscribe(&names).map_err(KafkaError::subscribe)?;
        }

        let consumer = Arc::new(consumer);
        if let Commit::Transactional(pipeline) = def.commit_mode() {
            self.state
                .register_eos(pipeline, EosSource::new(&tracker, &consumer));
        }
        // The descriptor's configuration fields are spent by now, so the rest of it moves into
        // the subscription rather than being cloned back out of a borrow.
        let parts = def.into_parts();
        let retry = (parts.retry.is_some() || parts.dead_letter.is_some()).then(|| {
            Arc::new(RetryContext::new(
                parts.retry,
                parts.max_deliveries,
                parts.dead_letter,
                Arc::clone(&self.state),
                Arc::clone(&consumer),
                Arc::clone(&tracker),
            ))
        });
        let subscriber = KafkaSubscriber::new(
            consumer,
            parts.name,
            parts.commit,
            tracker,
            parts.lane_key,
            retry,
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
        let producer = self.state.producer.clone();
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

    /// Subscribes to the topic `name` with descriptor defaults; requires
    /// [`KafkaBroker::default_group`].
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_with(KafkaTopic::new(name)).await
    }
}

/// The option combinations manual assignment cannot honor, failed at subscribe time.
fn validate_manual_assignment(def: &KafkaTopic, group: Option<&str>) -> Result<(), KafkaError> {
    if matches!(def.commit_mode(), Commit::Transactional(_)) {
        return Err(KafkaError::InvalidOptions(
            "manual partition assignment does not compose with `Commit::Transactional`: an \
             EOS pipeline commits through the consumer group protocol"
                .to_owned(),
        ));
    }
    if group.is_none() {
        if def.commit_mode() == &Commit::Tracked {
            return Err(KafkaError::InvalidOptions(
                "`Commit::Tracked` needs a group to commit into; name one with \
                 `KafkaTopic::group` or drop the commit mode for a group-less reader"
                    .to_owned(),
            ));
        }
        if def.start_offset() == StartOffset::Committed {
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
    def: &KafkaTopic,
) -> Result<(), KafkaError> {
    let offset = match def.start_offset() {
        StartOffset::Committed => Offset::Stored,
        StartOffset::Earliest => Offset::Beginning,
        StartOffset::Latest => Offset::End,
    };
    let mut assignment = TopicPartitionList::new();
    for partition in def.assigned_partitions() {
        assignment
            .add_partition_offset(def.topic(), *partition, offset)
            .map_err(KafkaError::subscribe)?;
    }
    consumer.assign(&assignment).map_err(KafkaError::subscribe)
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn descriptor_group_resolution_prefers_the_descriptor() {
        let def = KafkaTopic::new("orders");
        assert!(def.group_or(None).is_none());
        assert_eq!(def.group_or(Some("fallback")), Some("fallback"));
        assert_eq!(
            KafkaTopic::new("orders")
                .group("own")
                .group_or(Some("fallback")),
            Some("own"),
        );
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
