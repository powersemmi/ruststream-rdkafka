//! In-process transactions: the stand-ins the crate's transactional publish policies pair into.
//!
//! What lives here is the client-visible half of a Kafka transaction and nothing else: publishes
//! held back between `begin_transaction` and `commit`, released together on commit, discarded on
//! abort, with the same misuse errors the real publisher reports. The guarantees a Kafka
//! transaction actually rests on are broker-side and are not reproduced; each type names its own
//! gaps, and `docs/testing.md` collects them next to the live suite that covers them.

use std::collections::HashMap;
use std::fmt;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ruststream::{
    HeaderMap, OutgoingMessage, PairError, PublishPolicy, Publisher, TransactionalPublisher,
};

use super::broker::{ConnectedKafkaTestBroker, TestBrokerState};
use crate::error::KafkaError;
use crate::publisher::{KafkaPartitionedPublish, KafkaTransactionalPublish, PartitionLanes};

/// One publish held back while a transaction is open: topic, payload, headers.
type Buffered = (String, Bytes, HeaderMap);

/// The in-process stand-in for
/// [`KafkaTransactionalPublisher`](crate::KafkaTransactionalPublisher): the live form a
/// [`KafkaTransactionalPublish`] policy pairs into against the test broker.
///
/// Reached the way production reaches it - by attaching the real
/// `Publish::default().transactional_id(..)` policy at the include site - so a handler bounded
/// `Out<impl TransactionalPublisher>` mounts on the stand-in with the routes file unchanged.
///
/// # What it reproduces
///
/// The client-visible shape of a transaction, exactly. Publishes made between
/// [`begin_transaction`](TransactionalPublisher::begin_transaction) and
/// [`commit`](TransactionalPublisher::commit) are buffered and reach the router only when the
/// commit runs, in publish order; [`abort`](TransactionalPublisher::abort) drops them without
/// routing any; a publish with no transaction open routes straight away, as it does through the
/// real publisher's plain producer. The misuse contract holds too - a second
/// `begin_transaction` reports [`KafkaError::TransactionBusy`], a `commit` or `abort` with
/// nothing open reports [`KafkaError::NoTransaction`] - and clones share one buffer, as clones
/// of the real publisher share one producer and one transaction.
///
/// # What it does not reproduce
///
/// Everything a Kafka transaction's guarantee actually rests on lives on the cluster, and none
/// of it is representable in a channel. Naming the gaps, because a test that assumes them would
/// be green for no reason:
///
/// * **Atomic visibility.** A commit routes the buffer message by message, so a subscriber can
///   observe a prefix of it. Real readers at `read_committed` isolation see the whole
///   transaction or none of it, and there is no isolation level here to read at. An assertion
///   that a reader never sees a partial commit is unsound in process.
/// * **Zombie fencing.** The transactional id is carried and reported but fences nothing: two
///   publishers on the same id coexist here, where `init_transactions` would have fenced the
///   older producer's epoch. An assertion about a fenced-out replica is unsound in process.
/// * **Broker-held timeouts.** A transaction left open stays open until the process ends; Kafka
///   aborts one that outlives its `transaction.timeout.ms`. The policy's
///   [`transaction_timeout`](KafkaTransactionalPublish::transaction_timeout) and
///   [`queue_timeout`](KafkaTransactionalPublish::queue_timeout) are accepted and carry no
///   behaviour.
/// * **The work pairing does.** Pairing the real policy creates the producer and initializes its
///   transactions, so a bad transactional id fails at startup; pairing here allocates a buffer
///   and cannot fail, so a startup-time misconfiguration is invisible.
/// * **Exactly-once.** Coupling consumed offsets into the transaction
///   (`send_offsets_to_transaction`) needs a consumer group and its metadata, neither of which
///   the in-process transport has. [`KafkaEosPublish`](crate::KafkaEosPublish) therefore does
///   not pair against the test broker at all: mounting an exactly-once route on it is a compile
///   error rather than a green test that proves nothing.
///
/// The live suite covers all of it - `partition_scoped_transactions_run_independently`,
/// `eos_pipeline_commits_offsets_with_records` and
/// `eos_aborted_window_replays_without_output_duplicates` in `tests/integration_rdkafka.rs`,
/// which run only with `KAFKA_TEST_URL` set.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, Publisher, TransactionalPublisher};
/// use ruststream_rdkafka::KafkaPublish;
/// use ruststream_rdkafka::testing::KafkaTestBroker;
///
/// # async fn demo() -> Result<(), ruststream_rdkafka::KafkaError> {
/// let broker = KafkaTestBroker::new().connect().await?;
/// let publisher = broker
///     .transactional_publisher(KafkaPublish::default().transactional_id("orders-svc-1"))
///     .await?;
///
/// publisher.begin_transaction().await?;
/// publisher
///     .publish(OutgoingMessage::new("shipments", b"{}".as_slice()))
///     .await?;
/// publisher.commit().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct KafkaTestTransactionalPublisher {
    state: Arc<TestBrokerState>,
    id: String,
    /// The open transaction's buffer, or `None` when none is open. Shared by clones, mirroring
    /// the real publisher, whose clones share one producer and therefore one transaction.
    open: Arc<Mutex<Option<Vec<Buffered>>>>,
}

impl fmt::Debug for KafkaTestTransactionalPublisher {
    // The transaction state is deliberately left out: reading it means taking the lock, and a
    // `Debug` that can block or panic on a poisoned mutex is a trap in exactly the diagnostic
    // paths it exists for.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaTestTransactionalPublisher")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl KafkaTestTransactionalPublisher {
    pub(crate) fn new(state: &Arc<TestBrokerState>, policy: KafkaTransactionalPublish) -> Self {
        Self {
            state: Arc::clone(state),
            id: policy.into_id(),
            open: Arc::new(Mutex::new(None)),
        }
    }

    /// The transactional id this publisher was declared with.
    ///
    /// Reported for parity with
    /// [`KafkaTransactionalPublisher::id`](crate::KafkaTransactionalPublisher::id); in process it
    /// identifies the handle and fences nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Broker;
    /// use ruststream_rdkafka::KafkaPublish;
    /// use ruststream_rdkafka::testing::KafkaTestBroker;
    ///
    /// # async fn demo() -> Result<(), ruststream_rdkafka::KafkaError> {
    /// let broker = KafkaTestBroker::new().connect().await?;
    /// let publisher = broker
    ///     .transactional_publisher(KafkaPublish::default().transactional_id("orders-svc-1"))
    ///     .await?;
    /// assert_eq!(publisher.id(), "orders-svc-1");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    fn no_transaction(&self) -> KafkaError {
        KafkaError::NoTransaction {
            id: self.id.clone(),
        }
    }

    /// Routes `entry` into the in-process transport.
    fn route(&self, entry: &Buffered) {
        let (topic, payload, headers) = entry;
        self.state
            .router
            .publish(topic, payload, headers, self.state.coordinator().as_ref());
    }

    /// Buffers `msg` when a transaction is open, routes it otherwise. Synchronous, so no lock
    /// guard is ever held across an await point.
    fn send(&self, msg: &OutgoingMessage<'_>) -> Result<(), KafkaError> {
        if msg.name().is_empty() {
            return Err(KafkaError::InvalidOptions(
                "topic name must not be empty; the outgoing message name is the destination topic"
                    .to_owned(),
            ));
        }
        self.state.ensure_open(msg.name())?;
        let entry: Buffered = (
            msg.name().to_owned(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        {
            let mut open = self.open.lock().expect("test transaction mutex poisoned");
            if let Some(buffer) = open.as_mut() {
                buffer.push(entry);
                return Ok(());
            }
        }
        self.route(&entry);
        Ok(())
    }

    /// Claims the handle's single transaction. The check and the claim happen under one guard,
    /// so two concurrent begins cannot both pass the check.
    fn begin(&self) -> Result<(), KafkaError> {
        self.state.ensure_open(&self.id)?;
        let mut open = self.open.lock().expect("test transaction mutex poisoned");
        if open.is_some() {
            return Err(KafkaError::TransactionBusy {
                id: self.id.clone(),
            });
        }
        *open = Some(Vec::new());
        drop(open);
        Ok(())
    }

    /// Takes the open transaction's buffer, leaving the handle free for the next one.
    fn settle(&self) -> Result<Vec<Buffered>, KafkaError> {
        self.open
            .lock()
            .expect("test transaction mutex poisoned")
            .take()
            .ok_or_else(|| self.no_transaction())
    }
}

impl Publisher for KafkaTestTransactionalPublisher {
    type Error = KafkaError;

    /// Buffers `msg` into the open transaction, or routes it to subscribers of the topic named
    /// by [`OutgoingMessage::name`] when none is open.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] when the topic name is empty, and
    /// [`KafkaError::Closed`] once the transport this handle aliases has been shut down.
    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.send(&msg))
    }
}

impl TransactionalPublisher for KafkaTestTransactionalPublisher {
    /// Opens the buffering transaction.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::TransactionBusy`] when one is already open on this handle (or a
    /// clone sharing it), leaving that transaction untouched, and [`KafkaError::Closed`] once
    /// the transport has shut down.
    fn begin_transaction(&self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.begin())
    }

    /// Releases the buffered publishes to the router, in publish order.
    ///
    /// Ordering is honoured; atomicity is not: the router sees them one at a time, so a
    /// subscriber can observe a prefix. See the type's documentation.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::NoTransaction`] when none is open on this handle, and
    /// [`KafkaError::Closed`] once the transport has shut down - the transaction is consumed
    /// either way, as a failed commit consumes the real one.
    fn commit(&self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.settle().and_then(|buffered| {
            self.state.ensure_open(&self.id)?;
            for entry in &buffered {
                self.route(entry);
            }
            Ok(())
        }))
    }

    /// Discards the buffered publishes.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::NoTransaction`] when none is open on this handle. Aborting after
    /// the transport shut down still succeeds: nothing has to reach it.
    fn abort(&self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.settle().map(drop))
    }
}

/// The in-process broker pairs the real [`KafkaTransactionalPublish`] policy, so a service's
/// transactional include sites compile and mount unchanged against either broker.
impl PublishPolicy<ConnectedKafkaTestBroker> for KafkaTransactionalPublish {
    type Live = KafkaTestTransactionalPublisher;

    fn pair(
        self,
        connected: &ConnectedKafkaTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        // Infallible, unlike the real pairing: there is no producer to create and no
        // `init_transactions` to fence with, so a misconfigured transactional id cannot be
        // caught here. That gap is documented on `KafkaTestTransactionalPublisher`.
        ready(Ok(KafkaTestTransactionalPublisher::new(
            connected.state(),
            self,
        )))
    }
}

/// The in-process stand-in for [`TransactionalPartitions`](crate::TransactionalPartitions).
///
/// One independent [`KafkaTestTransactionalPublisher`] per source partition, so lanes that would
/// collide on a single transaction do not.
///
/// # What it reproduces
///
/// The lane independence a handler depends on: [`for_partition`](PartitionLanes::for_partition)
/// hands every partition its own publisher with its own buffer, so two lanes can hold open
/// transactions at once and neither sees the other's messages. Repeat calls for one partition
/// return the same handle, as the real cache does, and the derived ids follow the same
/// `"{base}-p{partition}"` scheme.
///
/// # What it does not reproduce
///
/// The reason the scheme exists. Per-partition ids are how Kafka fences a zombie lane, and in
/// process an id fences nothing (see [`KafkaTestTransactionalPublisher`] for the full list of
/// gaps, which apply to every lane's publisher). Materializing a lane also does no work here,
/// where the real path creates a producer and initializes its transactions, so a lane whose id
/// the cluster would refuse comes up clean. `partition_scoped_transactions_run_independently`
/// and `a_lanes_slot_publishes_through_its_partition_transaction` in
/// `tests/integration_rdkafka.rs` cover the real behaviour.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, PublishPolicy, Publisher, TransactionalPublisher};
/// use ruststream_rdkafka::{KafkaPublish, PartitionLanes};
/// use ruststream_rdkafka::testing::KafkaTestBroker;
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let broker = KafkaTestBroker::new().connect().await?;
/// let lanes = KafkaPublish::default()
///     .transactional_id("billing-svc-1")
///     .per_partition()
///     .pair(&broker)
///     .await?;
///
/// let lane = lanes.for_partition(3).await?;
/// assert_eq!(lane.id(), "billing-svc-1-p3");
/// lane.begin_transaction().await?;
/// lane.publish(OutgoingMessage::new("invoice-lines", b"{}".as_slice())).await?;
/// lane.commit().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct KafkaTestPartitions {
    state: Arc<TestBrokerState>,
    template: KafkaTransactionalPublish,
    /// One publisher per partition. A plain map rather than the real cache's per-partition
    /// `OnceCell`: materializing a lane here is a synchronous constructor, so there is no
    /// initialization for two lanes to race.
    lanes: Arc<Mutex<HashMap<i32, KafkaTestTransactionalPublisher>>>,
}

impl fmt::Debug for KafkaTestPartitions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaTestPartitions")
            .field("id_base", &self.template.id())
            .finish_non_exhaustive()
    }
}

impl KafkaTestPartitions {
    /// The publisher owning `partition`'s derived transactional id, created on first use.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Closed`] once the transport has shut down.
    ///
    /// # Panics
    ///
    /// Panics when the internal lane cache mutex is poisoned, which requires a prior panic while
    /// holding it (an invariant violation, not an operational failure).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{Broker, PublishPolicy};
    /// use ruststream_rdkafka::KafkaPublish;
    /// use ruststream_rdkafka::testing::KafkaTestBroker;
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let broker = KafkaTestBroker::new().connect().await?;
    /// let lanes = KafkaPublish::default()
    ///     .transactional_id("billing-svc-1")
    ///     .per_partition()
    ///     .pair(&broker)
    ///     .await?;
    /// let lane = lanes.for_partition(0).await?;
    /// # let _ = lane;
    /// # Ok(())
    /// # }
    /// ```
    pub fn for_partition(
        &self,
        partition: i32,
    ) -> impl Future<Output = Result<KafkaTestTransactionalPublisher, KafkaError>> {
        ready(self.lane(partition))
    }

    fn lane(&self, partition: i32) -> Result<KafkaTestTransactionalPublisher, KafkaError> {
        self.state.ensure_open(self.template.id())?;
        let mut lanes = self.lanes.lock().expect("test lane cache mutex poisoned");
        Ok(lanes
            .entry(partition)
            .or_insert_with(|| {
                let policy = self
                    .template
                    .with_id(format!("{}-p{partition}", self.template.id()));
                KafkaTestTransactionalPublisher::new(&self.state, policy)
            })
            .clone())
    }
}

impl PartitionLanes for KafkaTestPartitions {
    type Publisher = KafkaTestTransactionalPublisher;

    fn for_partition(
        &self,
        partition: i32,
    ) -> impl Future<Output = Result<Self::Publisher, KafkaError>> + Send {
        Self::for_partition(self, partition)
    }
}

/// The in-process broker pairs the real [`KafkaPartitionedPublish`] policy, so a handler bounded
/// `Out<impl PartitionLanes>` mounts on the stand-in with the routes file unchanged.
impl PublishPolicy<ConnectedKafkaTestBroker> for KafkaPartitionedPublish {
    type Live = KafkaTestPartitions;

    fn pair(
        self,
        connected: &ConnectedKafkaTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(KafkaTestPartitions {
            state: Arc::clone(connected.state()),
            template: self.template().clone(),
            lanes: Arc::new(Mutex::new(HashMap::new())),
        }))
    }
}
