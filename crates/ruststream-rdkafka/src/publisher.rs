//! The publishers: fire-and-confirm production, plus Kafka transactions.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::TopicPartitionList;
use rdkafka::consumer::ConsumerGroupMetadata;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer as _};
use rdkafka::util::Timeout;
use ruststream::{OutgoingMessage, Publisher, TransactionalPublisher};
use tokio::sync::OnceCell;
use tokio::task;

use crate::broker::SharedConn;
use crate::convert;
use crate::error::KafkaError;

const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(30);

/// The lazily-created transactional producer shared by clones of one publisher.
struct TxState {
    id: String,
    timeout: Duration,
    producer: OnceCell<FutureProducer>,
    /// Whether a transaction is currently open. Interleaving `publish` with
    /// `begin_transaction`/`commit` from concurrent tasks is not supported: which side of the
    /// transaction boundary a concurrent publish lands on would be a race either way.
    open: Mutex<bool>,
}

impl fmt::Debug for TxState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxState")
            .field("id", &self.id)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// A producer handle sharing the broker's connection.
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
/// [`transactional_id`](Self::transactional_id) upgrades the handle to a transactional one
/// implementing [`TransactionalPublisher`]: publishes between `begin_transaction` and `commit`
/// become visible atomically (readers on Kafka's default `read_committed` isolation see all of
/// them or none), and `abort` discards them broker-side.
///
/// Obtained from [`KafkaBroker::publisher`](crate::KafkaBroker::publisher); usable before
/// `Broker::connect` resolves the connection (publishing earlier returns
/// [`KafkaError::NotConnected`]).
#[derive(Debug, Clone)]
pub struct KafkaPublisher {
    conn: SharedConn,
    queue_timeout: Option<Duration>,
    tx: Option<Arc<TxState>>,
}

impl KafkaPublisher {
    pub(crate) fn new(conn: SharedConn) -> Self {
        Self {
            conn,
            queue_timeout: None,
            tx: None,
        }
    }

    /// How long a publish may wait for space when librdkafka's local queue is full, before
    /// failing with a queue-full error. Without it a publish waits for space indefinitely,
    /// which is the natural back-pressure behavior.
    #[must_use]
    pub fn queue_timeout(mut self, timeout: Duration) -> Self {
        self.queue_timeout = Some(timeout);
        self
    }

    /// Upgrades to a transactional publisher fenced by `id` (Kafka's `transactional.id`).
    ///
    /// The id must be stable and unique per concurrent producer: Kafka uses it to fence
    /// zombies, so two live producers sharing an id abort each other. Create several
    /// publishers with distinct ids for concurrent transactional flows. The transactional
    /// producer itself is created (and its transactions initialized) on first use, from the
    /// broker's resolved producer configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream_rdkafka::KafkaBroker;
    ///
    /// let broker = KafkaBroker::new(["localhost:9092"]);
    /// let replies = broker.publisher().transactional_id("orders-svc-1");
    /// # let _ = replies;
    /// ```
    #[must_use]
    pub fn transactional_id(mut self, id: impl Into<String>) -> Self {
        self.tx = Some(Arc::new(TxState {
            id: id.into(),
            timeout: DEFAULT_TRANSACTION_TIMEOUT,
            producer: OnceCell::new(),
            open: Mutex::new(false),
        }));
        self
    }

    /// How long transaction control calls (`init`, `commit`, `abort`) may block before
    /// reporting failure. Defaults to 30 seconds; this is the call deadline handed to
    /// librdkafka, not its `transaction.timeout.ms` (reachable through
    /// [`KafkaBroker::producer_config`](crate::KafkaBroker::producer_config)).
    ///
    /// Only meaningful after [`transactional_id`](Self::transactional_id).
    #[must_use]
    pub fn transaction_timeout(mut self, timeout: Duration) -> Self {
        if let Some(tx) = &self.tx {
            self.tx = Some(Arc::new(TxState {
                id: tx.id.clone(),
                timeout,
                producer: OnceCell::new(),
                open: Mutex::new(false),
            }));
        }
        self
    }

    fn tx_or_invalid(&self) -> Result<&Arc<TxState>, KafkaError> {
        self.tx.as_ref().ok_or_else(|| {
            KafkaError::InvalidOptions(
                "transactional publishing needs `KafkaPublisher::transactional_id`; a plain \
                 publisher cannot begin, commit, or abort transactions"
                    .to_owned(),
            )
        })
    }

    /// Resolves (creating and initializing on first use) the transactional producer.
    async fn tx_producer(&self, tx: &Arc<TxState>) -> Result<FutureProducer, KafkaError> {
        let producer = tx
            .producer
            .get_or_try_init(|| async {
                let state = self.conn.get().ok_or(KafkaError::NotConnected)?;
                let mut config = state.producer_config().clone();
                config.set("transactional.id", &tx.id);
                let producer: FutureProducer = config.create().map_err(KafkaError::publish)?;
                // init_transactions blocks (it fences earlier producers with this id), so it
                // runs on the blocking pool.
                let init = producer.clone();
                let timeout = tx.timeout;
                task::spawn_blocking(move || init.init_transactions(timeout))
                    .await
                    .map_err(|err| KafkaError::Publish(Box::new(err)))?
                    .map_err(KafkaError::publish)?;
                Ok(producer)
            })
            .await?;
        Ok(producer.clone())
    }

    pub(crate) fn shared_conn(&self) -> SharedConn {
        Arc::clone(&self.conn)
    }

    pub(crate) fn transactional_id_str(&self) -> Option<&str> {
        self.tx.as_ref().map(|tx| tx.id.as_str())
    }

    pub(crate) fn transaction_deadline(&self) -> Duration {
        self.tx
            .as_ref()
            .map_or(DEFAULT_TRANSACTION_TIMEOUT, |tx| tx.timeout)
    }

    /// Adds consumed source offsets (and their group's metadata) to the open transaction, so
    /// they commit atomically with the records published into it. The EOS pipeline's commit
    /// path; must run between `begin_transaction` and `commit`.
    pub(crate) async fn send_offsets(
        &self,
        offsets: TopicPartitionList,
        metadata: ConsumerGroupMetadata,
    ) -> Result<(), KafkaError> {
        let tx = self.tx_or_invalid()?.clone();
        let producer = self.tx_producer(&tx).await?;
        let timeout = tx.timeout;
        task::spawn_blocking(move || {
            producer.send_offsets_to_transaction(&offsets, &metadata, timeout)
        })
        .await
        .map_err(|err| KafkaError::Publish(Box::new(err)))?
        .map_err(KafkaError::publish)
    }

    fn is_open(tx: &TxState) -> bool {
        *tx.open.lock().expect("transaction state mutex poisoned")
    }

    fn set_open(tx: &TxState, open: bool) {
        *tx.open.lock().expect("transaction state mutex poisoned") = open;
    }

    async fn send_via(
        &self,
        producer: &FutureProducer,
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
        let queue_timeout = self.queue_timeout.map_or(Timeout::Never, Timeout::After);
        producer
            .send(record, queue_timeout)
            .await
            .map(|_delivery| ())
            .map_err(|(err, _record)| KafkaError::publish(err))
    }
}

impl Publisher for KafkaPublisher {
    type Error = KafkaError;

    /// Publishes `msg` to the topic named by [`OutgoingMessage::name`] and awaits the delivery
    /// report. Inside an open transaction the record joins it; otherwise it goes out through
    /// the broker's shared plain producer, transactional id or not.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::NotConnected`] before `Broker::connect` resolves the connection and
    /// [`KafkaError::Publish`] when the cluster rejects the record or the delivery times out
    /// (librdkafka's `message.timeout.ms`).
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the record in flight, delivered or not.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        if let Some(tx) = &self.tx
            && Self::is_open(tx)
        {
            let producer = self.tx_producer(tx).await?;
            return self.send_via(&producer, msg).await;
        }
        let state = self.conn.get().ok_or(KafkaError::NotConnected)?;
        self.send_via(state.producer(), msg).await
    }
}

impl TransactionalPublisher for KafkaPublisher {
    /// Begins a Kafka transaction (creating and initializing the transactional producer on
    /// first use).
    ///
    /// One producer runs one transaction at a time, so beginning while one is open is an
    /// error, not a queue: a second begin means two flows share one publisher, and silently
    /// merging their messages into one transaction would commit one flow's records with the
    /// other's. Concurrent transactional flows use distinct publishers (see
    /// [`TransactionalPartitions`](crate::TransactionalPartitions)).
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] without a
    /// [`transactional_id`](Self::transactional_id), [`KafkaError::TransactionBusy`] when a
    /// transaction is already open on this publisher (or a clone sharing its id),
    /// [`KafkaError::NotConnected`] before `Broker::connect`, and [`KafkaError::Publish`] when
    /// initialization or the begin call fails.
    // The guard intentionally spans the begin call: check-and-begin must be atomic so two
    // concurrent begins cannot both pass the check.
    #[allow(clippy::significant_drop_tightening)]
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        let tx = self.tx_or_invalid()?.clone();
        let producer = self.tx_producer(&tx).await?;
        let mut open = tx.open.lock().expect("transaction state mutex poisoned");
        if *open {
            return Err(KafkaError::TransactionBusy);
        }
        producer.begin_transaction().map_err(KafkaError::publish)?;
        *open = true;
        Ok(())
    }

    /// Commits the open transaction, making its records visible atomically; a no-op when none
    /// is open.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Publish`] when the commit fails. librdkafka distinguishes
    /// retriable failures from ones requiring an abort; after an error the transaction's state
    /// is unresolved, so treat the publisher as needing an
    /// [`abort`](TransactionalPublisher::abort) or replacement.
    async fn commit(&self) -> Result<(), Self::Error> {
        let tx = self.tx_or_invalid()?.clone();
        if !Self::is_open(&tx) {
            return Ok(());
        }
        let producer = self.tx_producer(&tx).await?;
        let timeout = tx.timeout;
        task::spawn_blocking(move || producer.commit_transaction(timeout))
            .await
            .map_err(|err| KafkaError::Publish(Box::new(err)))?
            .map_err(KafkaError::publish)?;
        Self::set_open(&tx, false);
        Ok(())
    }

    /// Aborts the open transaction, discarding its records broker-side; a no-op when none is
    /// open.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Publish`] when the abort fails.
    async fn abort(&self) -> Result<(), Self::Error> {
        let tx = self.tx_or_invalid()?.clone();
        if !Self::is_open(&tx) {
            return Ok(());
        }
        let producer = self.tx_producer(&tx).await?;
        let timeout = tx.timeout;
        task::spawn_blocking(move || producer.abort_transaction(timeout))
            .await
            .map_err(|err| KafkaError::Publish(Box::new(err)))?
            .map_err(KafkaError::publish)?;
        Self::set_open(&tx, false);
        Ok(())
    }
}

/// Lazily materialized transactional publishers, one per source partition.
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
/// Clones share the cache, so one instance in the application state serves every handler
/// invocation.
///
/// # Examples
///
/// ```no_run
/// use ruststream_rdkafka::{KafkaBroker, TransactionalPartitions};
///
/// let broker = KafkaBroker::new(["localhost:9092"]);
/// let publishers = TransactionalPartitions::new(broker.publisher(), "billing-svc-1");
/// // In a handler: the delivery's source partition picks the publisher.
/// let publisher = publishers.for_partition(3); // transactional id "billing-svc-1-p3"
/// # let _ = publisher;
/// ```
#[derive(Debug, Clone)]
pub struct TransactionalPartitions {
    inner: Arc<PartitionsInner>,
}

#[derive(Debug)]
struct PartitionsInner {
    template: KafkaPublisher,
    id_base: String,
    timeout: Option<Duration>,
    publishers: Mutex<HashMap<i32, KafkaPublisher>>,
}

impl TransactionalPartitions {
    /// Creates the per-partition publisher set over `template` (which carries the broker
    /// connection and any [`queue_timeout`](KafkaPublisher::queue_timeout)); each partition's
    /// publisher gets the transactional id `"{id_base}-p{partition}"`. A transactional id
    /// already set on the template is ignored.
    ///
    /// `id_base` must be stable across restarts and unique per service instance - it is what
    /// scopes zombie fencing.
    #[must_use]
    pub fn new(template: KafkaPublisher, id_base: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(PartitionsInner {
                template,
                id_base: id_base.into(),
                timeout: None,
                publishers: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The control-call deadline ([`KafkaPublisher::transaction_timeout`]) applied to each
    /// partition's publisher. Configure before handing the set out: publishers already
    /// materialized keep their deadline.
    #[must_use]
    pub fn transaction_timeout(self, timeout: Duration) -> Self {
        Self {
            inner: Arc::new(PartitionsInner {
                template: self.inner.template.clone(),
                id_base: self.inner.id_base.clone(),
                timeout: Some(timeout),
                publishers: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The publisher owning `partition`'s transactional id, created on first use.
    ///
    /// `partition` is the delivery's source partition (`KafkaContext`'s `Partition` field in a
    /// handler); passing anything else still works but forfeits the serialization argument
    /// that makes the per-partition scope safe.
    ///
    /// # Panics
    ///
    /// Panics when the internal cache mutex is poisoned, which requires a prior panic while
    /// materializing a publisher (an invariant violation, not an operational failure).
    #[must_use]
    pub fn for_partition(&self, partition: i32) -> KafkaPublisher {
        let mut publishers = self
            .inner
            .publishers
            .lock()
            .expect("partition publisher cache mutex poisoned");
        publishers
            .entry(partition)
            .or_insert_with(|| {
                let id = format!("{}-p{partition}", self.inner.id_base);
                let publisher = self.inner.template.clone().transactional_id(id);
                match self.inner.timeout {
                    Some(timeout) => publisher.transaction_timeout(timeout),
                    None => publisher,
                }
            })
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use ruststream::TransactionalPublisher as _;

    use super::*;

    fn tx_id(publisher: &KafkaPublisher) -> Option<String> {
        publisher.tx.as_ref().map(|tx| tx.id.clone())
    }

    #[tokio::test]
    async fn transactions_without_an_id_fail_clearly() {
        let publisher = KafkaPublisher::new(Arc::default());
        let err = publisher
            .begin_transaction()
            .await
            .expect_err("begin without transactional_id must fail");
        assert!(matches!(err, KafkaError::InvalidOptions(_)));
        assert!(err.to_string().contains("transactional_id"));
    }

    #[test]
    fn partitions_derive_ids_and_share_the_cache() {
        let set = TransactionalPartitions::new(KafkaPublisher::new(Arc::default()), "svc-1");
        let three = set.for_partition(3);
        assert_eq!(tx_id(&three).as_deref(), Some("svc-1-p3"));
        assert_eq!(tx_id(&set.for_partition(0)).as_deref(), Some("svc-1-p0"));

        // The same partition resolves to the same producer state, through clones too: the
        // clone shares the cache, so both handles are fenced (and serialized) together. The
        // clone is the point of the assertion, not an artifact.
        #[allow(clippy::redundant_clone)]
        let cloned = set.clone();
        let again = cloned.for_partition(3);
        let (left, right) = (
            three.tx.expect("transactional"),
            again.tx.expect("transactional"),
        );
        assert!(Arc::ptr_eq(&left, &right));
    }

    #[test]
    fn partitions_template_id_is_replaced() {
        let template = KafkaPublisher::new(Arc::default()).transactional_id("ignored");
        let set = TransactionalPartitions::new(template, "svc-1");
        assert_eq!(tx_id(&set.for_partition(7)).as_deref(), Some("svc-1-p7"));
    }
}
