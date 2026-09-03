//! The publish policies and the live publishers they pair into, transactions included.

use std::collections::HashMap;
use std::fmt;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::TopicPartitionList;
use rdkafka::consumer::ConsumerGroupMetadata;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer as _};
use rdkafka::util::Timeout;
use ruststream::runtime::Slot;
use ruststream::{
    DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher, TransactionalPublisher,
};
use tokio::sync::OnceCell;
use tokio::task;

use crate::broker::{ConnState, ConnectedKafkaBroker, EarlyConn};
use crate::convert;
use crate::error::KafkaError;

const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(30);

/// The publish policy of [`KafkaPublisher`]: pure declaration, no connection, no publish
/// surface.
///
/// Constructible anywhere - in a router definition, in configuration, before startup - because
/// it holds nothing but options. The runtime pairs it with the connected broker at startup (or
/// [`ConnectedKafkaBroker::publisher`] does it by hand), and only the resulting
/// [`KafkaPublisher`] can publish.
///
/// [`transactional_id`](Self::transactional_id) is a type transition, not a flag: it yields a
/// [`KafkaTransactionalPublish`], so a plain publisher carries no transactional surface at all.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use ruststream_rdkafka::KafkaPublish;
///
/// let policy = KafkaPublish::default().queue_timeout(Duration::from_secs(5));
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct KafkaPublish {
    queue_timeout: Option<Duration>,
}

impl KafkaPublish {
    /// How long a publish may wait for space when librdkafka's local queue is full, before
    /// failing with a queue-full error. Without it a publish waits for space indefinitely,
    /// which is the natural back-pressure behavior.
    pub const fn queue_timeout(mut self, timeout: Duration) -> Self {
        self.queue_timeout = Some(timeout);
        self
    }

    /// Turns this into a transactional publish policy fenced by `id` (Kafka's
    /// `transactional.id`).
    ///
    /// The id must be stable and unique per concurrent producer: Kafka uses it to fence
    /// zombies, so two live producers sharing an id abort each other. Pair distinct policies
    /// for concurrent transactional flows, or take one publisher per source partition with
    /// [`KafkaTransactionalPublish::per_partition`].
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rdkafka::KafkaPublish;
    ///
    /// let policy = KafkaPublish::default().transactional_id("orders-svc-1");
    /// # let _ = policy;
    /// ```
    pub fn transactional_id(self, id: impl Into<String>) -> KafkaTransactionalPublish {
        KafkaTransactionalPublish {
            queue_timeout: self.queue_timeout,
            id: id.into(),
            transaction_timeout: DEFAULT_TRANSACTION_TIMEOUT,
        }
    }

    pub(crate) const fn queue_timeout_setting(self) -> Option<Duration> {
        self.queue_timeout
    }
}

impl PublishPolicy<ConnectedKafkaBroker> for KafkaPublish {
    type Live = KafkaPublisher;

    fn pair(
        self,
        connected: &ConnectedKafkaBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher(self)))
    }
}

impl DefaultPublish for ConnectedKafkaBroker {
    type Policy = KafkaPublish;
}

/// A live producer handle on the broker's shared producer.
///
/// [`OutgoingMessage::name`] is the destination topic. A
/// [`PARTITION_KEY_HEADER`](crate::PARTITION_KEY_HEADER) header becomes the record's native key,
/// so Kafka routes messages that share a key to the same partition; without it the configured
/// partitioner picks one.
///
/// Each publish awaits the broker's delivery report, so an `Ok` means the cluster accepted the
/// record (durability then depends on the producer's `acks` setting, configurable through
/// [`KafkaBroker::producer_config`](crate::KafkaBroker::producer_config)).
///
/// Exists only from a connected broker, so it never sees a "not connected" state; it does alias
/// that connection and may outlive it, so after the broker shuts down every publish reports
/// [`KafkaError::Closed`]. Cheap to clone.
#[derive(Clone)]
pub struct KafkaPublisher {
    state: Arc<ConnState>,
    queue_timeout: Option<Duration>,
}

impl fmt::Debug for KafkaPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaPublisher")
            .field("queue_timeout", &self.queue_timeout)
            .finish_non_exhaustive()
    }
}

impl KafkaPublisher {
    pub(crate) const fn new(state: Arc<ConnState>, queue_timeout: Option<Duration>) -> Self {
        Self {
            state,
            queue_timeout,
        }
    }
}

/// Sends one record through `producer` and awaits its delivery report.
async fn send_via(
    producer: &FutureProducer,
    queue_timeout: Option<Duration>,
    msg: OutgoingMessage<'_>,
) -> Result<(), KafkaError> {
    let parts = convert::headers_for_publish(msg.headers())?;
    let mut record = FutureRecord::<[u8], [u8]>::to(msg.name()).payload(msg.payload());
    if let Some(key) = &parts.key {
        record = record.key(key.as_ref());
    }
    if let Some(partition) = parts.partition {
        // An explicit partition wins over the partitioner and the record key.
        record = record.partition(partition);
    }
    if let Some(headers) = parts.headers {
        record = record.headers(headers);
    }
    let queue_timeout = queue_timeout.map_or(Timeout::Never, Timeout::After);
    producer
        .send(record, queue_timeout)
        .await
        .map(|_delivery| ())
        .map_err(|(err, _record)| KafkaError::publish(err))
}

impl Publisher for KafkaPublisher {
    type Error = KafkaError;

    /// Publishes `msg` to the topic named by [`OutgoingMessage::name`] and awaits the delivery
    /// report.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Closed`] once the connection this handle aliases has been shut
    /// down, and [`KafkaError::Publish`] when the cluster rejects the record or the delivery
    /// times out (librdkafka's `message.timeout.ms`).
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the record in flight, delivered or not.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.state.ensure_open(msg.name())?;
        send_via(self.state.producer(), self.queue_timeout, msg).await
    }
}

/// A publisher minted from an unconnected [`KafkaBroker`](crate::KafkaBroker), for the one
/// wiring that cannot take a policy.
///
/// [`BrokerScope::retry_via`](ruststream::runtime::BrokerScope::retry_via) - the deferred
/// republish behind `retry_after`, which Kafka relies on because it has no native delayed
/// redelivery - is configured while the app builder runs and takes a live [`Publisher`], since
/// a publisher that cannot send would be a lie. This type is the sanctioned exception that
/// makes the pair work on a lazy-connect broker: it holds the cell
/// [`Broker::connect`](ruststream::Broker::connect) fills, not a connection. The policy path
/// ([`KafkaPublish`] and its transitions) is untouched and stays connection-free by
/// construction.
///
/// Its two runtime checks are the aliasing rule the broker contract deliberately keeps
/// dynamic: a handle that predates the connection, or outlives it, must surface an error rather
/// than silently succeed. Publishing before `connect` reports [`KafkaError::NotConnected`];
/// publishing after the connected broker shut down reports [`KafkaError::Closed`].
///
/// # Examples
///
/// ```no_run
/// use ruststream_rdkafka::KafkaBroker;
///
/// let broker = KafkaBroker::new(["localhost:9092"]);
/// let retries = broker.retry_publisher();
/// // ... `b.retry_via(retries)` while the app builder runs.
/// # let _ = retries;
/// ```
#[derive(Clone)]
pub struct KafkaRetryPublisher {
    conn: EarlyConn,
}

impl fmt::Debug for KafkaRetryPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaRetryPublisher")
            .field("connected", &self.conn.get().is_some())
            .finish_non_exhaustive()
    }
}

impl KafkaRetryPublisher {
    pub(crate) const fn new(conn: EarlyConn) -> Self {
        Self { conn }
    }
}

impl Publisher for KafkaRetryPublisher {
    type Error = KafkaError;

    /// Publishes `msg` through the broker's shared producer and awaits the delivery report.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::NotConnected`] before the broker connects,
    /// [`KafkaError::Closed`] once it has shut down, and [`KafkaError::Publish`] when the
    /// cluster rejects the record or the delivery times out.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the record in flight, delivered or not.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let state = self.conn.get().ok_or_else(|| KafkaError::NotConnected {
            topic: msg.name().to_owned(),
        })?;
        state.ensure_open(msg.name())?;
        send_via(state.producer(), None, msg).await
    }
}

/// The publish policy of [`KafkaTransactionalPublisher`]: the transactional mode as its own
/// type, reached from [`KafkaPublish::transactional_id`].
///
/// Pairing it is where Kafka does the real work: the transactional producer is created and its
/// transactions initialized (the call that fences earlier producers with the same id), so a
/// misconfigured transactional id fails at startup rather than at the first transaction.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use ruststream_rdkafka::KafkaPublish;
///
/// let policy = KafkaPublish::default()
///     .transactional_id("orders-svc-1")
///     .transaction_timeout(Duration::from_secs(10));
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct KafkaTransactionalPublish {
    queue_timeout: Option<Duration>,
    id: String,
    transaction_timeout: Duration,
}

impl KafkaTransactionalPublish {
    /// See [`KafkaPublish::queue_timeout`].
    pub const fn queue_timeout(mut self, timeout: Duration) -> Self {
        self.queue_timeout = Some(timeout);
        self
    }

    /// How long transaction control calls (`init`, `commit`, `abort`) may block before
    /// reporting failure. Defaults to 30 seconds; this is the call deadline handed to
    /// librdkafka, not its `transaction.timeout.ms` (reachable through
    /// [`KafkaBroker::producer_config`](crate::KafkaBroker::producer_config)).
    pub const fn transaction_timeout(mut self, timeout: Duration) -> Self {
        self.transaction_timeout = timeout;
        self
    }

    /// Turns this into a per-partition policy: the id becomes the base of one transactional id
    /// per source partition (`"{base}-p{partition}"`), pairing into
    /// [`TransactionalPartitions`].
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rdkafka::KafkaPublish;
    ///
    /// let policy = KafkaPublish::default()
    ///     .transactional_id("billing-svc-1")
    ///     .per_partition();
    /// # let _ = policy;
    /// ```
    pub fn per_partition(self) -> KafkaPartitionedPublish {
        KafkaPartitionedPublish { template: self }
    }

    /// The transactional id this policy fences with.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    fn with_id(&self, id: String) -> Self {
        Self {
            queue_timeout: self.queue_timeout,
            id,
            transaction_timeout: self.transaction_timeout,
        }
    }
}

impl PublishPolicy<ConnectedKafkaBroker> for KafkaTransactionalPublish {
    type Live = KafkaTransactionalPublisher;

    async fn pair(self, connected: &ConnectedKafkaBroker) -> Result<Self::Live, PairError> {
        connected
            .transactional_publisher(self)
            .await
            .map_err(PairError::new)
    }
}

impl ConnectedKafkaBroker {
    /// A live transactional publisher: creates the transactional producer from the broker's
    /// resolved producer configuration and initializes its transactions.
    ///
    /// Async because the initialization is real work (it fences earlier producers holding the
    /// same transactional id); it runs once, when the publisher comes alive.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Closed`] once the connection has been shut down and
    /// [`KafkaError::Publish`] when the producer cannot be created or the initialization fails
    /// within the policy's [`transaction_timeout`](KafkaTransactionalPublish::transaction_timeout).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream::Broker;
    /// use ruststream_rdkafka::{KafkaBroker, KafkaPublish};
    ///
    /// # async fn demo() -> Result<(), ruststream_rdkafka::KafkaError> {
    /// let connected = KafkaBroker::new(["localhost:9092"]).connect().await?;
    /// let publisher = connected
    ///     .transactional_publisher(KafkaPublish::default().transactional_id("orders-svc-1"))
    ///     .await?;
    /// # let _ = publisher;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn transactional_publisher(
        &self,
        policy: KafkaTransactionalPublish,
    ) -> Result<KafkaTransactionalPublisher, KafkaError> {
        open_transactional(self.state(), &policy).await
    }
}

/// Creates and initializes one transactional producer for `policy`.
pub(crate) async fn open_transactional(
    state: &Arc<ConnState>,
    policy: &KafkaTransactionalPublish,
) -> Result<KafkaTransactionalPublisher, KafkaError> {
    state.ensure_open(policy.id())?;
    let mut config = state.producer_config().clone();
    config.set("transactional.id", policy.id());
    let producer: FutureProducer = config.create().map_err(KafkaError::publish)?;
    // init_transactions blocks (it fences earlier producers with this id), so it runs on the
    // blocking pool.
    let init = producer.clone();
    let timeout = policy.transaction_timeout;
    task::spawn_blocking(move || init.init_transactions(timeout))
        .await
        .map_err(|err| KafkaError::Publish(Box::new(err)))?
        .map_err(KafkaError::publish)?;
    Ok(KafkaTransactionalPublisher {
        inner: Arc::new(TxInner {
            state: Arc::clone(state),
            producer,
            queue_timeout: policy.queue_timeout,
            timeout,
            id: policy.id.clone(),
            open: Mutex::new(false),
        }),
    })
}

struct TxInner {
    state: Arc<ConnState>,
    producer: FutureProducer,
    queue_timeout: Option<Duration>,
    timeout: Duration,
    id: String,
    /// Whether a transaction is currently open. Interleaving `publish` with
    /// `begin_transaction`/`commit` from concurrent tasks is not supported: which side of the
    /// transaction boundary a concurrent publish lands on would be a race either way.
    open: Mutex<bool>,
}

/// A live publisher that produces inside Kafka transactions.
///
/// Records between `begin_transaction` and `commit` become visible atomically (readers on
/// Kafka's default `read_committed` isolation see all of them or none); `abort` discards them
/// broker-side.
///
/// Its transactional producer is created and initialized when the
/// [`KafkaTransactionalPublish`] policy pairs, so nothing is lazy here: the handle is fenced
/// from the moment it exists. Clones share one producer and one transaction state.
#[derive(Clone)]
pub struct KafkaTransactionalPublisher {
    inner: Arc<TxInner>,
}

impl fmt::Debug for KafkaTransactionalPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaTransactionalPublisher")
            .field("id", &self.inner.id)
            .field("timeout", &self.inner.timeout)
            .finish_non_exhaustive()
    }
}

impl KafkaTransactionalPublisher {
    /// The transactional id fencing this publisher.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    pub(crate) fn deadline(&self) -> Duration {
        self.inner.timeout
    }

    pub(crate) fn state(&self) -> &Arc<ConnState> {
        &self.inner.state
    }

    fn is_open(&self) -> bool {
        *self
            .inner
            .open
            .lock()
            .expect("transaction state mutex poisoned")
    }

    fn set_open(&self, open: bool) {
        *self
            .inner
            .open
            .lock()
            .expect("transaction state mutex poisoned") = open;
    }

    fn no_transaction(&self) -> KafkaError {
        KafkaError::NoTransaction {
            id: self.inner.id.clone(),
        }
    }

    /// Adds consumed source offsets (and their group's metadata) to the open transaction, so
    /// they commit atomically with the records published into it. The EOS pipeline's commit
    /// path; must run between `begin_transaction` and `commit`.
    pub(crate) async fn send_offsets(
        &self,
        offsets: TopicPartitionList,
        metadata: ConsumerGroupMetadata,
    ) -> Result<(), KafkaError> {
        self.inner.state.ensure_open(&self.inner.id)?;
        if !self.is_open() {
            return Err(self.no_transaction());
        }
        let producer = self.inner.producer.clone();
        let timeout = self.inner.timeout;
        task::spawn_blocking(move || {
            producer.send_offsets_to_transaction(&offsets, &metadata, timeout)
        })
        .await
        .map_err(|err| KafkaError::Publish(Box::new(err)))?
        .map_err(KafkaError::publish)
    }
}

impl Publisher for KafkaTransactionalPublisher {
    type Error = KafkaError;

    /// Publishes `msg` to the topic named by [`OutgoingMessage::name`]. Inside an open
    /// transaction the record joins it; otherwise it goes out through the broker's shared plain
    /// producer.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Closed`] once the connection this handle aliases has been shut
    /// down and [`KafkaError::Publish`] when the cluster rejects the record or the delivery
    /// times out.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the record in flight, delivered or not.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.inner.state.ensure_open(msg.name())?;
        if self.is_open() {
            return send_via(&self.inner.producer, self.inner.queue_timeout, msg).await;
        }
        send_via(self.inner.state.producer(), self.inner.queue_timeout, msg).await
    }
}

impl TransactionalPublisher for KafkaTransactionalPublisher {
    /// Begins a Kafka transaction.
    ///
    /// One producer runs one transaction at a time, so beginning while one is open is an
    /// error, not a queue: a second begin means two flows share one publisher, and silently
    /// merging their messages into one transaction would commit one flow's records with the
    /// other's. Concurrent transactional flows use distinct publishers (see
    /// [`TransactionalPartitions`]).
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::TransactionBusy`] when a transaction is already open on this
    /// publisher (or a clone sharing it), [`KafkaError::Closed`] after the broker shut down,
    /// and [`KafkaError::Publish`] when the begin call fails.
    // The guard intentionally spans the begin call: check-and-begin must be atomic so two
    // concurrent begins cannot both pass the check.
    fn begin_transaction(&self) -> impl Future<Output = Result<(), Self::Error>> {
        if let Err(err) = self.inner.state.ensure_open(&self.inner.id) {
            return ready(Err(err));
        }
        {
            let mut open = self
                .inner
                .open
                .lock()
                .expect("transaction state mutex poisoned");
            if *open {
                return ready(Err(KafkaError::TransactionBusy {
                    id: self.inner.id.clone(),
                }));
            }
            // A rejected begin leaves the open transaction untouched, per the trait contract.
            if let Err(err) = self.inner.producer.begin_transaction() {
                return ready(Err(KafkaError::publish(err)));
            }
            *open = true;
        }
        ready(Ok(()))
    }

    /// Commits the open transaction, making its records visible atomically.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::NoTransaction`] when no transaction is open on this publisher,
    /// [`KafkaError::Closed`] after the broker shut down, and [`KafkaError::Publish`] when the
    /// commit fails. librdkafka distinguishes retriable failures from ones requiring an abort;
    /// after an error the transaction's state is unresolved, so treat the publisher as needing
    /// an [`abort`](TransactionalPublisher::abort) or replacement.
    async fn commit(&self) -> Result<(), Self::Error> {
        self.inner.state.ensure_open(&self.inner.id)?;
        if !self.is_open() {
            return Err(self.no_transaction());
        }
        let producer = self.inner.producer.clone();
        let timeout = self.inner.timeout;
        task::spawn_blocking(move || producer.commit_transaction(timeout))
            .await
            .map_err(|err| KafkaError::Publish(Box::new(err)))?
            .map_err(KafkaError::publish)?;
        self.set_open(false);
        Ok(())
    }

    /// Aborts the open transaction, discarding its records broker-side.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::NoTransaction`] when no transaction is open on this publisher,
    /// [`KafkaError::Closed`] after the broker shut down, and [`KafkaError::Publish`] when the
    /// abort fails.
    async fn abort(&self) -> Result<(), Self::Error> {
        self.inner.state.ensure_open(&self.inner.id)?;
        if !self.is_open() {
            return Err(self.no_transaction());
        }
        let producer = self.inner.producer.clone();
        let timeout = self.inner.timeout;
        let aborted = task::spawn_blocking(move || producer.abort_transaction(timeout))
            .await
            .map_err(|err| KafkaError::Publish(Box::new(err)))?;
        // The transaction is over either way: a failed abort resolves broker-side by its own
        // timeout, and leaving the handle "open" would wedge it permanently.
        self.set_open(false);
        aborted.map_err(KafkaError::publish)
    }
}

/// The publish policy of [`TransactionalPartitions`], reached from
/// [`KafkaTransactionalPublish::per_partition`].
///
/// # Examples
///
/// ```
/// use ruststream_rdkafka::KafkaPublish;
///
/// let policy = KafkaPublish::default()
///     .transactional_id("billing-svc-1")
///     .per_partition();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct KafkaPartitionedPublish {
    template: KafkaTransactionalPublish,
}

impl PublishPolicy<ConnectedKafkaBroker> for KafkaPartitionedPublish {
    type Live = TransactionalPartitions;

    fn pair(
        self,
        connected: &ConnectedKafkaBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(TransactionalPartitions {
            inner: Arc::new(PartitionsInner {
                state: Arc::clone(connected.state()),
                template: self.template,
                publishers: Mutex::new(HashMap::new()),
            }),
        }))
    }
}

/// Transactional publishers, one per source partition, materialized on first use.
///
/// Kafka permits one open transaction per producer and one live producer per transactional id
/// (initializing a second fences the first), so concurrent transactional handlers need one
/// producer each. The source partition is the natural scope: under the default
/// [`LaneKey::Partition`](crate::LaneKey::Partition) worker pool a partition's deliveries
/// process serially on one lane, so a publisher per partition gives every lane an independent
/// transaction with no coordination. The id set (`"{base}-p{partition}"`) follows the topic's
/// partitions rather than the worker count: changing `workers(n)` neither changes the ids nor
/// weakens zombie fencing - the scheme Kafka Streams uses for its per-task producers.
///
/// Not for [`LaneKey::RecordKey`](crate::LaneKey::RecordKey) pools: record-key lanes spread
/// one partition across lanes, so two lanes would share a partition's publisher and collide
/// on its single transaction ([`KafkaError::TransactionBusy`]).
///
/// Clones share the cache, so one injected handle serves every handler invocation.
#[derive(Clone)]
pub struct TransactionalPartitions {
    inner: Arc<PartitionsInner>,
}

struct PartitionsInner {
    state: Arc<ConnState>,
    template: KafkaTransactionalPublish,
    /// The per-partition publishers. The set of partitions is only known as deliveries arrive,
    /// so materialization stays lazy here (a cell per partition, so two lanes racing the same
    /// partition initialize one producer, not two).
    publishers: Mutex<HashMap<i32, Arc<OnceCell<KafkaTransactionalPublisher>>>>,
}

impl fmt::Debug for TransactionalPartitions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionalPartitions")
            .field("id_base", &self.inner.template.id())
            .finish_non_exhaustive()
    }
}

impl TransactionalPartitions {
    /// The publisher owning `partition`'s transactional id, created and initialized on first
    /// use.
    ///
    /// `partition` is the delivery's source partition (`KafkaContext`'s `Partition` field in a
    /// handler); passing anything else still works but forfeits the serialization argument
    /// that makes the per-partition scope safe.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Closed`] once the broker has shut down and
    /// [`KafkaError::Publish`] when the partition's producer cannot be created or its
    /// transactions cannot be initialized.
    ///
    /// # Panics
    ///
    /// Panics when the internal cache mutex is poisoned, which requires a prior panic while
    /// materializing a publisher (an invariant violation, not an operational failure).
    pub async fn for_partition(
        &self,
        partition: i32,
    ) -> Result<KafkaTransactionalPublisher, KafkaError> {
        let cell = {
            let mut publishers = self
                .inner
                .publishers
                .lock()
                .expect("partition publisher cache mutex poisoned");
            Arc::clone(publishers.entry(partition).or_default())
        };
        let policy = self
            .inner
            .template
            .with_id(format!("{}-p{partition}", self.inner.template.id()));
        cell.get_or_try_init(|| open_transactional(&self.inner.state, &policy))
            .await
            .cloned()
    }
}

/// The capability of handing out one transactional publisher per source partition.
///
/// A handler that drives per-partition transactions names this in its `Out` slot
/// (`Out(lanes): Out<impl PartitionLanes>`) instead of a publisher type: the concrete value is
/// [`TransactionalPartitions`], inferred from the [`KafkaTransactionalPublish::per_partition`]
/// policy attached at the include site.
///
/// # Test capture
///
/// This is a router, not a publisher: it hands out a publisher of its own rather than sending a
/// message. What a lane then publishes leaves through that publisher, so it lands in the
/// broker's publish log and not in the slot's test record - the same boundary a settled owned
/// transaction's buffer has. Assert on the publish log for lane traffic, and keep the slot
/// record for handlers that publish through the slot itself.
///
/// # Examples
///
/// ```
/// use ruststream::TransactionalPublisher;
/// use ruststream_rdkafka::{KafkaError, PartitionLanes};
///
/// async fn ping<L: PartitionLanes>(lanes: &L, partition: i32) -> Result<(), KafkaError> {
///     let publisher = lanes.for_partition(partition).await?;
///     publisher.begin_transaction().await
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not hand out per-partition transactional publishers",
    note = "for an `Out<impl PartitionLanes, _>` slot, attach a \
            `KafkaPublish::default().transactional_id(..).per_partition()` policy"
)]
pub trait PartitionLanes: Send + Sync {
    /// The publisher owning `partition`'s transactional id.
    ///
    /// # Errors
    ///
    /// See [`TransactionalPartitions::for_partition`].
    fn for_partition(
        &self,
        partition: i32,
    ) -> impl Future<Output = Result<KafkaTransactionalPublisher, KafkaError>> + Send;
}

impl PartitionLanes for TransactionalPartitions {
    fn for_partition(
        &self,
        partition: i32,
    ) -> impl Future<Output = Result<KafkaTransactionalPublisher, KafkaError>> + Send {
        Self::for_partition(self, partition)
    }
}

// The arena entry a handler's `Out` parameter binds. `Slot`'s `Deref` already routes method
// calls to the wired value, but only this impl lets a body hand the entry to a function generic
// over the capability (`fn issue<L: PartitionLanes>(lanes: &L, ..)`), which is the whole point of
// bounding the slot with a trait instead of a concrete type. Both paths reach the same unwrapped
// value, which is what puts a lane's publishes outside the slot's test capture (see the trait's
// documentation).
impl<M, L, EncodeCodec, Body> PartitionLanes for Slot<M, L, EncodeCodec, Body>
where
    L: PartitionLanes,
    EncodeCodec: Send + Sync,
{
    fn for_partition(
        &self,
        partition: i32,
    ) -> impl Future<Output = Result<KafkaTransactionalPublisher, KafkaError>> + Send {
        (**self).for_partition(partition)
    }
}

#[cfg(test)]
mod tests {
    use crate::broker::KafkaBroker;

    use super::*;

    #[tokio::test]
    async fn the_early_publisher_errors_before_connect() {
        // No I/O anywhere: the cell is simply still empty.
        let publisher = KafkaBroker::new(["localhost:9092"]).retry_publisher();
        let err = publisher
            .publish(OutgoingMessage::new("orders", b"deferred".as_slice()))
            .await
            .expect_err("publishing before connect must error");
        assert!(
            matches!(&err, KafkaError::NotConnected { topic } if topic == "orders"),
            "the error must name the topic it could not reach, got: {err}",
        );
    }

    #[test]
    fn transactional_id_is_a_type_transition() {
        let plain = KafkaPublish::default().queue_timeout(Duration::from_secs(1));
        let transactional = plain.transactional_id("svc-1");
        assert_eq!(transactional.id(), "svc-1");
        assert_eq!(transactional.queue_timeout, plain.queue_timeout_setting());
        assert_eq!(
            transactional.transaction_timeout,
            DEFAULT_TRANSACTION_TIMEOUT
        );
    }

    #[test]
    fn per_partition_derives_ids_from_the_base() {
        let policy = KafkaPublish::default()
            .transactional_id("svc-1")
            .transaction_timeout(Duration::from_secs(5));
        let derived = policy.with_id(format!("{}-p3", policy.id()));
        assert_eq!(derived.id(), "svc-1-p3");
        assert_eq!(derived.transaction_timeout, Duration::from_secs(5));
    }
}
