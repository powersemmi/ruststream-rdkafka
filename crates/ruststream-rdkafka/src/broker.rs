//! The broker handle: connection lifecycle, subscriptions, and the publisher constructor.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::producer::{FutureProducer, Producer as _};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use ruststream::{Broker, DescribeServer, ServerSpec, Subscribe};
use tokio::sync::OnceCell;
use tokio::task;

use crate::eos::EosSource;
use crate::error::KafkaError;
use crate::publisher::KafkaPublisher;
use crate::retry::RetryContext;
use crate::subscriber::KafkaSubscriber;
use crate::topic::{Commit, KafkaTopic, StartOffset};
use crate::tracker::{CommitTracker, TrackingContext};

/// The live client state: the shared producer every publisher clones from, plus the resolved
/// producer configuration so transactional publishers can derive their own producers from it.
pub(crate) struct ConnState {
    producer: FutureProducer,
    producer_config: ClientConfig,
    /// Subscriptions in `Commit::Transactional` mode, keyed by their pipeline id (the
    /// transactional id of the `EosPipeline` that commits their offsets).
    eos_sources: Mutex<HashMap<String, Vec<EosSource>>>,
}

impl ConnState {
    pub(crate) fn producer(&self) -> &FutureProducer {
        &self.producer
    }

    pub(crate) fn producer_config(&self) -> &ClientConfig {
        &self.producer_config
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
        f.debug_struct("ConnState").finish_non_exhaustive()
    }
}

/// The connection cell shared by the broker and everything it hands out, so publishers obtained
/// before `Broker::connect` resolve the connection on first use.
pub(crate) type SharedConn = Arc<OnceCell<ConnState>>;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

/// An Apache Kafka broker backed by [`rdkafka`](https://docs.rs/rdkafka) / librdkafka.
///
/// Follows the `RustStream` lazy startup contract: [`new`](Self::new) is synchronous and does no
/// I/O; the network work happens in the idempotent async `Broker::connect`, which the runtime
/// calls once at startup. Publishers handed out earlier share the connection cell and resolve it
/// on first use.
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
    conn: SharedConn,
    servers: Vec<String>,
    default_group: Option<String>,
    client_config: Vec<(String, String)>,
    producer_config: Vec<(String, String)>,
    connect_timeout: Duration,
    flush_timeout: Duration,
    #[cfg(feature = "schema-registry")]
    schema_registry: Option<crate::schema_registry::SchemaRegistry>,
}

impl KafkaBroker {
    /// Records the bootstrap servers; no I/O happens until `Broker::connect`.
    ///
    /// Each entry is a `host` or `host:port` seed the client bootstraps from.
    #[must_use]
    pub fn new<I, S>(servers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            conn: Arc::new(OnceCell::new()),
            servers: servers.into_iter().map(Into::into).collect(),
            default_group: None,
            client_config: Vec::new(),
            producer_config: Vec::new(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            flush_timeout: DEFAULT_FLUSH_TIMEOUT,
            #[cfg(feature = "schema-registry")]
            schema_registry: None,
        }
    }

    /// Connects eagerly: [`new`](Self::new) followed by `Broker::connect`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Connect`] when the client cannot be created or the cluster is
    /// unreachable.
    pub async fn connect<I, S>(servers: I) -> Result<Self, KafkaError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let broker = Self::new(servers);
        Broker::connect(&broker).await?;
        Ok(broker)
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

    /// How long `Broker::connect` waits for the cluster-reachability probe (a metadata fetch)
    /// before failing startup. Defaults to 30 seconds. This is this crate's own fail-fast
    /// window, not a librdkafka property.
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// How long `Broker::shutdown` waits for in-flight publishes to flush before reporting
    /// failure. Defaults to 30 seconds.
    #[must_use]
    pub fn flush_timeout(mut self, timeout: Duration) -> Self {
        self.flush_timeout = timeout;
        self
    }

    /// A publisher on the shared producer.
    #[must_use]
    pub fn publisher(&self) -> KafkaPublisher {
        let publisher = KafkaPublisher::new(Arc::clone(&self.conn));
        #[cfg(feature = "schema-registry")]
        let publisher = publisher.with_registry(self.schema_registry.clone());
        publisher
    }

    fn connected(&self) -> Result<&ConnState, KafkaError> {
        self.conn.get().ok_or(KafkaError::NotConnected)
    }

    fn base_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", self.servers.join(","));
        for (key, value) in &self.client_config {
            config.set(key, value);
        }
        config
    }

    /// Attaches a [`SchemaRegistry`](crate::schema_registry::SchemaRegistry) client: every
    /// subscription prefetches the schema a Confluent-framed delivery references before the
    /// delivery reaches the (synchronous) codec, so registry-backed decoding never blocks the
    /// hot path on I/O. The client is shared; clones see one cache.
    #[cfg(feature = "schema-registry")]
    #[must_use]
    pub fn schema_registry(mut self, registry: crate::schema_registry::SchemaRegistry) -> Self {
        self.schema_registry = Some(registry);
        self
    }

    /// Opens a subscription for `def`: one consumer joining `def`'s group on `def`'s topic(s)
    /// or pattern.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::NotConnected`] before `Broker::connect`,
    /// [`KafkaError::InvalidOptions`] when neither the descriptor nor the broker names a
    /// consumer group (or the descriptor's pattern is not `^`-anchored), and
    /// [`KafkaError::Subscribe`] when the consumer cannot be created or the subscription is
    /// rejected.
    // Async without an await on purpose: librdkafka joins the group in the background, and the
    // descriptor contract (`SubscriptionSource::subscribe`) is async either way.
    #[allow(clippy::unused_async)]
    pub async fn subscribe(&self, def: KafkaTopic) -> Result<KafkaSubscriber, KafkaError> {
        self.connected()?;
        def.validate()?;
        let manual = !def.assigned_partitions().is_empty();
        let group = def
            .group_or(self.default_group.as_deref())
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

        let mut config = self.base_config();
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
            let state = self.conn.get().ok_or(KafkaError::NotConnected)?;
            state.register_eos(pipeline, EosSource::new(&tracker, &consumer));
        }
        let retry =
            (def.retry_policy().is_some() || def.dead_letter_topic().is_some()).then(|| {
                Arc::new(RetryContext::new(
                    def.retry_policy().cloned(),
                    def.max_deliveries_cap(),
                    def.dead_letter_topic().map(str::to_owned),
                    Arc::clone(&self.conn),
                    Arc::clone(&consumer),
                ))
            });
        let subscriber = KafkaSubscriber::new(
            consumer,
            def.topic().to_owned(),
            def.commit_mode().clone(),
            tracker,
            def.lane_key_choice(),
            retry,
        );
        #[cfg(feature = "schema-registry")]
        let subscriber = subscriber.with_schema_registry(self.schema_registry.clone());
        Ok(subscriber)
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

impl Broker for KafkaBroker {
    type Error = KafkaError;

    /// Creates the shared producer and probes the cluster with a metadata fetch, so an
    /// unreachable or misconfigured cluster fails startup instead of the first publish;
    /// idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] when no bootstrap server was given and
    /// [`KafkaError::Connect`] when the client cannot be created or the probe fails within
    /// [`connect_timeout`](Self::connect_timeout).
    async fn connect(&self) -> Result<(), Self::Error> {
        self.conn
            .get_or_try_init(|| async {
                if self.servers.is_empty() {
                    return Err(KafkaError::InvalidOptions(
                        "at least one bootstrap server is required".to_owned(),
                    ));
                }
                let mut config = self.base_config();
                for (key, value) in &self.producer_config {
                    config.set(key, value);
                }
                let producer: FutureProducer = config.create().map_err(KafkaError::connect)?;

                // fetch_metadata blocks, so it runs on the blocking pool.
                let probe = producer.clone();
                let timeout = self.connect_timeout;
                task::spawn_blocking(move || probe.client().fetch_metadata(None, timeout))
                    .await
                    .map_err(|err| KafkaError::Connect(Box::new(err)))?
                    .map_err(KafkaError::connect)?;

                Ok(ConnState {
                    producer,
                    producer_config: config,
                    eos_sources: Mutex::new(HashMap::new()),
                })
            })
            .await?;
        Ok(())
    }

    /// Flushes in-flight publishes; consumers close when their subscribers drop. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Publish`] when in-flight records were not delivered within
    /// [`flush_timeout`](Self::flush_timeout).
    async fn shutdown(&self) -> Result<(), Self::Error> {
        if let Some(state) = self.conn.get() {
            // flush blocks (it polls the producer), so it runs on the blocking pool.
            let producer = state.producer.clone();
            let timeout = self.flush_timeout;
            task::spawn_blocking(move || producer.flush(timeout))
                .await
                .map_err(|err| KafkaError::Publish(Box::new(err)))?
                .map_err(KafkaError::publish)?;
        }
        Ok(())
    }
}

// `Self::subscribe` inside this impl would resolve to the trait method and recurse; the type
// name is the only way to reach the inherent one.
#[allow(clippy::use_self)]
impl Subscribe for KafkaBroker {
    type Subscriber = KafkaSubscriber;

    /// Subscribes to the topic `name` with descriptor defaults; requires
    /// [`default_group`](Self::default_group).
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        KafkaBroker::subscribe(self, KafkaTopic::new(name)).await
    }
}

impl DescribeServer for KafkaBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new(self.servers.join(","), "kafka")
    }
}

#[cfg(test)]
mod tests {
    use ruststream::DescribeServer as _;

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

    #[tokio::test]
    async fn operations_before_connect_report_not_connected() {
        let broker = KafkaBroker::new(["localhost:9092"]).default_group("g");
        let err = broker
            .subscribe(KafkaTopic::new("orders"))
            .await
            .unwrap_err();
        assert!(matches!(err, KafkaError::NotConnected));
    }

    #[tokio::test]
    async fn missing_group_is_a_clear_startup_error() {
        let broker = KafkaBroker::new(["localhost:9092"]);
        // Force the connected state check to pass is not possible without I/O; the group check
        // runs after it, so assert on the error of the not-connected path elsewhere and the
        // group resolution logic directly here.
        let def = KafkaTopic::new("orders");
        assert!(def.group_or(None).is_none());
        assert_eq!(def.group_or(Some("fallback")), Some("fallback"));
        let _ = broker;
    }

    #[tokio::test]
    async fn connect_with_no_servers_fails_fast() {
        let broker = KafkaBroker::new(Vec::<String>::new());
        let err = Broker::connect(&broker).await.unwrap_err();
        assert!(matches!(err, KafkaError::InvalidOptions(_)));
    }
}
