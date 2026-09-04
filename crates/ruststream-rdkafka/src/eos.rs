//! Exactly-once pipelines: group-committed producer transactions that carry the consumed
//! offsets (`send_offsets_to_transaction`, the KIP-447 consume-transform-produce shape).
//!
//! An [`EosPipeline`] shares one transactional producer across every handler of one or more
//! `Commit::Transactional` subscriptions. Handlers publish into the pipeline's open window;
//! on the commit interval the pipeline waits for the window's participants to settle, adds
//! their source positions to the transaction, and commits - records and offsets become
//! visible atomically, so a crash or an abort rewinds both and the output topic never sees a
//! duplicate. Concurrent publishes may join an open transaction (only the control calls are
//! exclusive), which is what lets a `workers(n, by_key)` pool share one transactional id.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures::future::select_all;
use rdkafka::consumer::{Consumer as _, ConsumerGroupMetadata, StreamConsumer};
use rdkafka::{Offset, TopicPartitionList};
use ruststream::runtime::{Outgoing, PublishContext, PublishTransform};
use ruststream::{
    OutgoingMessage, PairError, PublishPolicy, Publisher, TransactionalPublisher as _,
};
use tracing::{debug, error};

use crate::broker::ConnectedKafkaBroker;
use crate::error::KafkaError;
use crate::publisher::{KafkaPublish, KafkaTransactionalPublish, KafkaTransactionalPublisher};
use crate::tracker::{CommitTracker, TrackingContext};

/// The Kafka Streams default for exactly-once commit intervals.
const DEFAULT_COMMIT_INTERVAL: Duration = Duration::from_millis(100);

/// One `Commit::Transactional` subscription registered under a pipeline id: the watermark
/// tracker deciding which positions are settled, and the consumer whose group metadata
/// fences the offset commit (and which seeks back on an abort).
///
/// Held weakly: the registry must not keep a dropped subscriber's consumer alive (it would
/// silently stay in its group and stall rebalances). A dead entry is pruned on lookup.
#[derive(Clone)]
pub(crate) struct EosSource {
    tracker: Weak<CommitTracker>,
    consumer: Weak<StreamConsumer<TrackingContext>>,
}

impl EosSource {
    pub(crate) fn new(
        tracker: &Arc<CommitTracker>,
        consumer: &Arc<StreamConsumer<TrackingContext>>,
    ) -> Self {
        Self {
            tracker: Arc::downgrade(tracker),
            consumer: Arc::downgrade(consumer),
        }
    }

    pub(crate) fn alive(&self) -> bool {
        self.tracker.strong_count() > 0 && self.consumer.strong_count() > 0
    }

    fn upgrade(&self) -> Option<LiveSource> {
        Some(LiveSource {
            tracker: self.tracker.upgrade()?,
            consumer: self.consumer.upgrade()?,
        })
    }
}

/// An upgraded [`EosSource`] pinned for the duration of one window commit.
struct LiveSource {
    tracker: Arc<CommitTracker>,
    consumer: Arc<StreamConsumer<TrackingContext>>,
}

/// The source coordinates of one delivery, as [`EosPipeline::publish`] needs them.
///
/// In a handler, take them as a `Ctx(source): Ctx<Source>` extractor parameter, or read the
/// [`keys::Source`](crate::context::keys::Source) field off a declared ctx parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceOffset {
    topic: String,
    partition: i32,
    offset: i64,
}

impl SourceOffset {
    /// Builds the coordinates by hand; in a handler prefer the
    /// [`keys::Source`](crate::context::keys::Source) key.
    #[must_use]
    pub fn new(topic: impl Into<String>, partition: i32, offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
        }
    }

    fn key(&self) -> (String, i32) {
        (self.topic.clone(), self.partition)
    }
}

/// Where the pipeline's current window stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// No transaction open; the next publish opens one.
    Idle,
    /// A publish is opening the transaction; others wait for the outcome.
    Opening,
    /// The transaction is open and admitting publishes.
    Open,
    /// The commit interval elapsed: only participants already enrolled may still publish,
    /// new deliveries wait for the next window.
    Committing,
}

/// The open window's state.
#[derive(Debug)]
struct Window {
    phase: Phase,
    /// Highest enrolled source offset per (topic, partition).
    enrolled: HashMap<(String, i32), i64>,
    /// A publish into this window failed: the transaction is poisoned and must abort.
    failed: bool,
    /// Distinguishes windows across commits, so a stale window task cannot touch its
    /// successor.
    epoch: u64,
}

struct PipelineInner {
    publisher: KafkaTransactionalPublisher,
    /// The pipeline id: the publisher's transactional id, which `Commit::Transactional`
    /// subscriptions name to register their offsets here.
    id: String,
    interval: Duration,
    window: Mutex<Window>,
    /// Woken on every phase transition; publishers waiting for admission re-check then.
    phase_changed: tokio::sync::Notify,
    /// The sources registered under this pipeline id, refreshed whenever a window opens. The
    /// publish path reads them to notice a reposition without touching the broker-wide registry
    /// per message.
    sources: Mutex<Vec<EosSource>>,
    /// Offsets committed by this pipeline per (topic, partition) ("next to consume"), the
    /// seek target when a window aborts.
    committed: Mutex<HashMap<(String, i32), i64>>,
    /// The first offset ever enrolled per (topic, partition): the abort seek target before
    /// anything committed.
    session_low: Mutex<HashMap<(String, i32), i64>>,
}

/// The publish policy of [`EosPipeline`]: the pipeline id (the producer's transactional id)
/// plus the commit interval, declared anywhere and paired with the connected broker at startup.
///
/// Wiring, all three naming the same id:
///
/// 1. The policy: `KafkaEosPublish::new("pipeline-1")`, attached at the include site
///    (`b.include(handler).publisher(policy)`) or bound for an `after_startup` hook.
/// 2. Each source subscription: `Commit::Transactional("pipeline-1".into())` - its consumer
///    stops committing offsets on its own and registers with the pipeline instead.
/// 3. The handler: it receives the paired [`EosPipeline`] and calls
///    [`publish`](EosPipeline::publish) with the delivery's [`SourceOffset`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use ruststream_rdkafka::KafkaEosPublish;
///
/// let policy = KafkaEosPublish::new("enrich-1").commit_interval(Duration::from_millis(50));
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct KafkaEosPublish {
    transactional: KafkaTransactionalPublish,
    interval: Duration,
}

impl KafkaEosPublish {
    /// Declares a pipeline fenced by `id`, which doubles as the pipeline id that
    /// `Commit::Transactional` subscriptions register under.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            transactional: KafkaPublish::default().transactional_id(id),
            interval: DEFAULT_COMMIT_INTERVAL,
        }
    }

    /// How long a window stays open before committing; defaults to 100ms (the Kafka Streams
    /// exactly-once default). Longer intervals amortize the commit over more records at the
    /// cost of end-to-end latency (records become visible only at the commit).
    pub const fn commit_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// See [`KafkaTransactionalPublish::transaction_timeout`]; it doubles as the deadline a
    /// window waits for its participants to settle.
    pub fn transaction_timeout(mut self, timeout: Duration) -> Self {
        self.transactional = self.transactional.transaction_timeout(timeout);
        self
    }

    /// See [`KafkaPublish::queue_timeout`].
    pub fn queue_timeout(mut self, timeout: Duration) -> Self {
        self.transactional = self.transactional.queue_timeout(timeout);
        self
    }

    /// The pipeline id.
    #[must_use]
    pub fn id(&self) -> &str {
        self.transactional.id()
    }
}

impl PublishPolicy<ConnectedKafkaBroker> for KafkaEosPublish {
    type Live = EosPipeline;

    async fn pair(self, connected: &ConnectedKafkaBroker) -> Result<Self::Live, PairError> {
        let interval = self.interval;
        let publisher = self.transactional.pair(connected).await?;
        Ok(EosPipeline::new(publisher, interval))
    }
}

/// An exactly-once pipeline over one transactional producer, the live form of
/// [`KafkaEosPublish`].
///
/// Every [`commit_interval`](KafkaEosPublish::commit_interval) the pipeline closes the window:
/// it waits until every delivery that published into it has settled (the shared watermark
/// reached the enrolled offsets), adds the settled source positions and their group metadata to
/// the transaction, and commits. On any failure - a failed publish, a settle stall (a handler
/// hanging or `retry()`-ing past the publisher's transaction timeout), a rebalance revoking
/// an enrolled partition, a commit error - the window aborts and the consumers seek back, so
/// the whole window redelivers and republishes into a fresh transaction; committed output
/// still never duplicates.
///
/// Works best over the default `LaneKey::Partition` worker lanes: a partition processes in
/// order on one lane, so the settle condition follows the lane head and windows close
/// promptly. Clones share the pipeline.
#[derive(Clone)]
pub struct EosPipeline {
    inner: Arc<PipelineInner>,
}

impl std::fmt::Debug for EosPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EosPipeline")
            .field("id", &self.inner.id)
            .field("interval", &self.inner.interval)
            .finish_non_exhaustive()
    }
}

impl EosPipeline {
    fn new(publisher: KafkaTransactionalPublisher, interval: Duration) -> Self {
        let id = publisher.id().to_owned();
        Self {
            inner: Arc::new(PipelineInner {
                publisher,
                id,
                interval,
                window: Mutex::new(Window {
                    phase: Phase::Idle,
                    enrolled: HashMap::new(),
                    failed: false,
                    epoch: 0,
                }),
                phase_changed: tokio::sync::Notify::new(),
                sources: Mutex::new(Vec::new()),
                committed: Mutex::new(HashMap::new()),
                session_low: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The pipeline id: the transactional id its producer is fenced by, and the id
    /// `Commit::Transactional` subscriptions register under.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    /// Publishes `msg` into the pipeline's open window on behalf of the delivery at `source`.
    ///
    /// The record joins the window's transaction and becomes visible at its commit, atomically
    /// with the source position. Publish, then return `Ack`: the settled watermark is what
    /// releases the window's commit.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Closed`] once the broker has shut down and
    /// [`KafkaError::Publish`] when opening the transaction or producing the record fails - the
    /// window aborts and redelivers, so failing the handler (`retry()`) is the right response.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the record in the window's transaction.
    ///
    /// # Panics
    ///
    /// Panics when the internal window mutex is poisoned, which requires a prior panic
    /// inside the pipeline (an invariant violation, not an operational failure).
    pub async fn publish(
        &self,
        source: &SourceOffset,
        msg: OutgoingMessage<'_>,
    ) -> Result<(), KafkaError> {
        let epoch = self.admit(source).await?;
        let sent = self.inner.publisher.publish(msg).await;
        if sent.is_err() {
            let mut window = self.inner.window.lock().expect("window mutex poisoned");
            // A failed produce poisons the transaction it was admitted into. The epoch guard
            // keeps a slow failure from an already-torn-down window off its successor, which
            // every other window-touching path already checks.
            if window.epoch == epoch {
                window.failed = true;
            }
        }
        sent
    }

    /// Joins the open window (opening one when idle), waiting out a commit in progress
    /// unless the delivery is already part of it. Returns the epoch of the window the source
    /// was admitted into, so the caller can attribute a later failure to that window only.
    async fn admit(&self, source: &SourceOffset) -> Result<u64, KafkaError> {
        loop {
            // The waiter is created before the phase check so a transition landing in
            // between is not missed.
            let phase_changed = self.inner.phase_changed.notified();
            // A window whose sources were repositioned is void: its transaction is about to
            // abort, and anything published into it would be purged with it. The replayed
            // delivery waits for the fresh window instead.
            let voided = self.repositioned();
            let action = {
                let mut window = self.inner.window.lock().expect("window mutex poisoned");
                match window.phase {
                    _ if voided && window.phase != Phase::Idle => Admission::Wait,
                    Phase::Open => {
                        Self::enroll(&mut window, &self.inner.session_low, source);
                        Admission::Admitted(window.epoch)
                    }
                    Phase::Idle => {
                        window.phase = Phase::Opening;
                        Admission::Opener
                    }
                    Phase::Opening => Admission::Wait,
                    Phase::Committing => {
                        // A delivery at or below the window's enrolled offsets is part of
                        // the committing window: its records must land in it (the commit is
                        // waiting for its settle). Anything else waits for the next window.
                        let participant = window
                            .enrolled
                            .get(&source.key())
                            .is_some_and(|max| source.offset <= *max);
                        if participant {
                            Admission::Admitted(window.epoch)
                        } else {
                            Admission::Wait
                        }
                    }
                }
            };
            match action {
                Admission::Admitted(epoch) => return Ok(epoch),
                Admission::Wait => {
                    phase_changed.await;
                }
                Admission::Opener => return self.open_window(source).await,
            }
        }
    }

    /// Opens the transaction as the winning publisher and spawns the window's commit task.
    /// Returns the opened window's epoch.
    async fn open_window(&self, source: &SourceOffset) -> Result<u64, KafkaError> {
        // The registry lookup happens once per window, here, and the publish path reads the
        // cached list. A reposition from before this window is already reflected in the
        // sources' bookkeeping, so its flag is cleared: only a seek landing while the window is
        // open has offsets of this window to invalidate.
        let registered = self.inner.publisher.state().eos_sources(&self.inner.id);
        for source in registered.iter().filter_map(EosSource::upgrade) {
            source.tracker.take_repositioned();
        }
        *self.inner.sources.lock().expect("sources mutex poisoned") = registered;
        let begun = self.inner.publisher.begin_transaction().await;
        let mut window = self.inner.window.lock().expect("window mutex poisoned");
        match begun {
            Ok(()) => {
                window.phase = Phase::Open;
                window.failed = false;
                Self::enroll(&mut window, &self.inner.session_low, source);
                let epoch = window.epoch;
                drop(window);
                tokio::spawn(run_window(Arc::clone(&self.inner), epoch));
                self.inner.phase_changed.notify_waiters();
                Ok(epoch)
            }
            Err(err) => {
                window.phase = Phase::Idle;
                drop(window);
                self.inner.phase_changed.notify_waiters();
                Err(err)
            }
        }
    }

    /// Whether any source of the open window has a pending reposition. Reads the cached source
    /// list (refreshed once per window) and one atomic per source, so it stays cheap enough for
    /// the publish path.
    fn repositioned(&self) -> bool {
        self.inner
            .sources
            .lock()
            .expect("sources mutex poisoned")
            .iter()
            .filter_map(EosSource::upgrade)
            .any(|source| source.tracker.is_repositioned())
    }

    fn enroll(
        window: &mut Window,
        session_low: &Mutex<HashMap<(String, i32), i64>>,
        source: &SourceOffset,
    ) {
        let key = source.key();
        session_low
            .lock()
            .expect("session low mutex poisoned")
            .entry(key.clone())
            .or_insert(source.offset);
        let max = window.enrolled.entry(key).or_insert(source.offset);
        if source.offset > *max {
            *max = source.offset;
        }
    }
}

enum Admission {
    Admitted(u64),
    Wait,
    Opener,
}

/// The per-window task: sleeps out the commit interval, closes admission, waits for the
/// participants to settle, and commits (or aborts and seeks back).
async fn run_window(inner: Arc<PipelineInner>, epoch: u64) {
    stay_open(&inner).await;
    let enrolled = {
        let mut window = inner.window.lock().expect("window mutex poisoned");
        if window.epoch != epoch || window.phase != Phase::Open {
            return;
        }
        window.phase = Phase::Committing;
        window.enrolled.clone()
    };
    let outcome = commit_window(&inner, &enrolled).await;
    {
        let mut window = inner.window.lock().expect("window mutex poisoned");
        window.phase = Phase::Idle;
        window.enrolled.clear();
        window.failed = false;
        window.epoch += 1;
    }
    inner.phase_changed.notify_waiters();
    if let Err(err) = outcome {
        error!(
            target: "ruststream_rdkafka",
            pipeline = %inner.id,
            error = %err,
            "EOS window aborted; its sources seek back and the window redelivers",
        );
    }
}

/// Holds the window open for the commit interval, or until a source is repositioned.
///
/// A seek voids everything the window would commit, so waiting out the rest of the interval
/// would only strand the replayed deliveries behind a transaction that is going to abort.
async fn stay_open(inner: &Arc<PipelineInner>) {
    let sources: Vec<LiveSource> = inner
        .sources
        .lock()
        .expect("sources mutex poisoned")
        .iter()
        .filter_map(EosSource::upgrade)
        .collect();
    // Waiters first, flag second: a reposition landing between the two is caught by the
    // already-registered waiters.
    let waiters: Vec<_> = sources
        .iter()
        .map(|source| Box::pin(source.tracker.reposition_waiter()))
        .collect();
    if waiters.is_empty()
        || sources
            .iter()
            .any(|source| source.tracker.is_repositioned())
    {
        if waiters.is_empty() {
            tokio::time::sleep(inner.interval).await;
        }
        return;
    }
    tokio::select! {
        () = tokio::time::sleep(inner.interval) => {}
        (..) = select_all(waiters) => {}
    }
}

/// Commits the window: settle-wait, offsets into the transaction, commit. Any failure runs
/// the abort path (abort the transaction, seek the sources back) and reports the cause.
async fn commit_window(
    inner: &Arc<PipelineInner>,
    enrolled: &HashMap<(String, i32), i64>,
) -> Result<(), KafkaError> {
    let sources = live_sources(&inner.id, &inner.publisher);

    // A seek while the window was open moved the read position out from under it: the offsets
    // this window would attach to its transaction describe records the subscription no longer
    // reads from, so committing them would carry the group past everything the seek replayed.
    // The window aborts instead, and the seek-back is skipped - the consumer already sits where
    // the seek put it.
    // Counting rather than `any`: the flag has to be taken from every source, so a second
    // source's reposition cannot linger into the next window.
    let repositioned = sources
        .iter()
        .filter(|source| source.tracker.take_repositioned())
        .count()
        > 0;
    if repositioned {
        abort_window(inner, &sources, enrolled, Repositioned::Yes).await;
        return Err(KafkaError::InvalidOptions(
            "the subscription was repositioned while this window was open; its records are \
             discarded and the replayed deliveries are processed into a fresh window"
                .to_owned(),
        ));
    }

    let failed = {
        let window = inner.window.lock().expect("window mutex poisoned");
        window.failed
    };
    let ready = if failed {
        Err(KafkaError::Publish(
            "a publish into this window failed; the transaction is poisoned"
                .to_owned()
                .into(),
        ))
    } else {
        wait_settled(inner, &sources, enrolled).await
    };
    let result = match ready {
        Ok(()) => try_commit(inner, &sources).await,
        Err(err) => Err(err),
    };
    if let Err(err) = result {
        abort_window(inner, &sources, enrolled, Repositioned::No).await;
        return Err(err);
    }
    Ok(())
}

/// The pipeline's registered sources that are still alive.
fn live_sources(id: &str, publisher: &KafkaTransactionalPublisher) -> Vec<LiveSource> {
    publisher
        .state()
        .eos_sources(id)
        .iter()
        .filter_map(EosSource::upgrade)
        .collect()
}

/// Whether the window is aborting because its sources were repositioned, which decides the fate
/// of the seek-back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Repositioned {
    Yes,
    No,
}

/// Waits until every enrolled (topic, partition) has settled up to its enrolled offset, with
/// the publisher's transaction-timeout as the stall deadline.
async fn wait_settled(
    inner: &Arc<PipelineInner>,
    sources: &[LiveSource],
    enrolled: &HashMap<(String, i32), i64>,
) -> Result<(), KafkaError> {
    let deadline = tokio::time::Instant::now() + inner.publisher.deadline();
    loop {
        // Waiters first, condition second: an advance between the two is caught by the
        // already-registered waiters.
        let waiters: Vec<_> = sources
            .iter()
            .map(|source| Box::pin(source.tracker.advance_waiter()))
            .collect();
        let pending = enrolled.iter().find(|((topic, partition), max)| {
            !sources.iter().any(|source| {
                source
                    .tracker
                    .stored_position(topic, *partition)
                    .is_some_and(|stored| stored >= **max)
            })
        });
        let Some(((topic, partition), max)) = pending else {
            return Ok(());
        };
        if waiters.is_empty() {
            return Err(KafkaError::InvalidOptions(format!(
                "EOS pipeline has no registered sources for its id; is the subscription in \
                 `Commit::Transactional` mode with the matching pipeline id? (waiting on \
                 {topic}[{partition}] up to offset {max})",
            )));
        }
        debug!(
            target: "ruststream_rdkafka",
            topic = %topic,
            partition = partition,
            up_to = max,
            "EOS window waiting for participants to settle",
        );
        if tokio::time::timeout_at(deadline, select_all(waiters))
            .await
            .is_err()
        {
            return Err(KafkaError::Publish(
                format!(
                    "EOS window stalled: {topic}[{partition}] did not settle up to offset \
                     {max} within the transaction deadline (a hung or retrying handler, or a \
                     revoked partition)",
                )
                .into(),
            ));
        }
    }
}

/// Adds every source's settled positions (with its group metadata) to the transaction and
/// commits it.
async fn try_commit(inner: &Arc<PipelineInner>, sources: &[LiveSource]) -> Result<(), KafkaError> {
    let mut sent: Vec<((String, i32), i64)> = Vec::new();
    for source in sources {
        let positions = source.tracker.stored_positions();
        if positions.is_empty() {
            continue;
        }
        let mut offsets = TopicPartitionList::new();
        for ((topic, partition), stored) in &positions {
            offsets
                .add_partition_offset(topic, *partition, Offset::Offset(stored + 1))
                .map_err(KafkaError::publish)?;
        }
        let metadata = group_metadata(source)?;
        inner.publisher.send_offsets(offsets, metadata).await?;
        sent.extend(positions.into_iter().map(|(key, stored)| (key, stored + 1)));
    }
    inner.publisher.commit().await?;
    {
        let mut committed = inner.committed.lock().expect("committed mutex poisoned");
        for (key, next) in sent {
            committed.insert(key, next);
        }
    }
    Ok(())
}

fn group_metadata(source: &LiveSource) -> Result<ConsumerGroupMetadata, KafkaError> {
    source.consumer.group_metadata().ok_or_else(|| {
        KafkaError::Publish(
            "the source consumer has no group metadata (not a group member yet or already \
             closed); cannot commit its offsets transactionally"
                .to_owned()
                .into(),
        )
    })
}

/// The abort path: abort the transaction and seek every enrolled partition back to the last
/// offset this pipeline committed (or the first offset it ever saw), so the whole window
/// redelivers promptly instead of waiting for a rebalance.
///
/// A window aborting because it was repositioned skips the seek-back entirely: the consumer is
/// already where the seek put it, and rewinding to this pipeline's own bookkeeping would undo
/// the caller's reposition. That bookkeeping is dropped with it, since it describes offsets of a
/// read position this subscription no longer has.
async fn abort_window(
    inner: &Arc<PipelineInner>,
    sources: &[LiveSource],
    enrolled: &HashMap<(String, i32), i64>,
    repositioned: Repositioned,
) {
    if let Err(err) = inner.publisher.abort().await {
        error!(
            target: "ruststream_rdkafka",
            error = %err,
            "EOS window abort failed; the transaction resolves by its broker-side timeout",
        );
    }
    if repositioned == Repositioned::Yes {
        {
            let mut committed = inner.committed.lock().expect("committed mutex poisoned");
            for key in enrolled.keys() {
                committed.remove(key);
            }
        }
        {
            let mut session_low = inner
                .session_low
                .lock()
                .expect("session low mutex poisoned");
            for key in enrolled.keys() {
                session_low.remove(key);
            }
        }
        return;
    }
    let committed = inner
        .committed
        .lock()
        .expect("committed mutex poisoned")
        .clone();
    let session_low = inner
        .session_low
        .lock()
        .expect("session low mutex poisoned")
        .clone();
    for key @ (topic, partition) in enrolled.keys() {
        let Some(target) = committed
            .get(key)
            .copied()
            .or_else(|| session_low.get(key).copied())
        else {
            continue;
        };
        let Some(source) = sources
            .iter()
            .find(|source| source.tracker.covers(topic, *partition))
        else {
            continue;
        };
        if let Err(err) = source.consumer.seek(
            topic,
            *partition,
            Offset::Offset(target),
            Duration::from_secs(5),
        ) {
            // A revoked partition cannot seek; its new owner resumes from the committed
            // offset on its own.
            debug!(
                target: "ruststream_rdkafka",
                topic = %topic,
                partition = partition,
                error = %err,
                "seek-back after an aborted EOS window failed",
            );
        }
    }
}

/// Header carrying a transactional delivery's source coordinates through the reply path.
///
/// Stamped onto every incoming delivery of a `Commit::Transactional` subscription (the value is
/// `"{partition}:{offset}:{topic}"`), relayed onto a publishing handler's reply by
/// [`EosReplies`], and consumed by the pipeline's [`Publisher`] impl to pair the reply with the
/// consumed offset. It is stripped from every outgoing publish, so it never reaches the wire.
pub const EOS_SOURCE_HEADER: &str = "kafka-eos-source";

pub(crate) fn encode_source(topic: &str, partition: i32, offset: i64) -> String {
    format!("{partition}:{offset}:{topic}")
}

fn decode_source(value: &str) -> Option<SourceOffset> {
    let mut parts = value.splitn(3, ':');
    let partition = parts.next()?.parse().ok()?;
    let offset = parts.next()?.parse().ok()?;
    let topic = parts.next()?;
    Some(SourceOffset::new(topic, partition, offset))
}

/// The [`PublishTransform`] relaying [`EOS_SOURCE_HEADER`] from the originating delivery onto
/// the reply, so the pipeline's [`Publisher`] impl can pair the reply with its consumed offset.
///
/// A `publish("replies")` handler over an exactly-once pipeline names it as the mount site's
/// transform step, right after the policy:
/// `b.include(enrich).publisher(KafkaEosPublish::new("enrich-1")).transform(EosReplies)`. Every
/// reply then joins the pipeline's open window paired with its delivery's consumed offset,
/// making the publishing-handler form exactly-once end to end - the handler just returns the
/// value. The chain takes the codec the same way, with a `.codec(..)` step.
///
/// This works only for subscriptions in `Commit::Transactional` mode naming that pipeline's id
/// (they are what stamps the source coordinates being relayed); a reply from any other
/// subscription fails with a clear error. The `retry_after` deferred-republish fallback does not
/// apply to these replies either: a delayed copy would break the offset-record pairing.
///
/// Generic over the handler's context type, so bare handlers (no ctx parameter, no `Ctx`
/// extractors) work.
#[derive(Debug, Clone, Copy, Default)]
pub struct EosReplies;

impl<C> PublishTransform<C> for EosReplies {
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
        if let Some(source) = cx.headers().get(EOS_SOURCE_HEADER) {
            let source = source.to_vec();
            out.headers_mut().insert(EOS_SOURCE_HEADER, source);
        }
    }
}

impl Publisher for EosPipeline {
    type Error = KafkaError;

    /// Publishes a reply into the pipeline's open window, paired with the source coordinates
    /// the [`EOS_SOURCE_HEADER`] carries (stripped before the record is produced).
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] when the header is missing or malformed - the
    /// originating subscription is not in `Commit::Transactional` mode for this pipeline, or
    /// the mount site left the [`EosReplies`] transform off the chain; otherwise as
    /// [`EosPipeline::publish`](EosPipeline::publish).
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the record in the window's transaction.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let Some(source) = msg
            .headers()
            .get_str(EOS_SOURCE_HEADER)
            .and_then(decode_source)
        else {
            return Err(KafkaError::InvalidOptions(
                "an EOS reply carries no source coordinates: the subscription must be in \
                 `Commit::Transactional` mode for this pipeline, and the reply publisher must \
                 relay them (add the `EosReplies` transform to the mount site's chain, \
                 `.publisher(KafkaEosPublish::new(id)).transform(EosReplies)`)"
                    .to_owned(),
            ));
        };
        let mut headers = msg.headers().clone();
        headers.remove(EOS_SOURCE_HEADER);
        let stripped = OutgoingMessage::new(msg.name(), msg.payload()).with_headers(headers);
        self.publish(&source, stripped).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_header_roundtrips_topics_with_colons() {
        let encoded = encode_source("orders:eu:v1", 3, 42);
        let decoded = decode_source(&encoded).expect("decodes");
        assert_eq!(decoded, SourceOffset::new("orders:eu:v1", 3, 42));
    }

    #[test]
    fn malformed_source_headers_are_rejected() {
        for bad in ["", "3", "3:x:orders", "x:42:orders"] {
            assert!(decode_source(bad).is_none(), "{bad:?} must not decode");
        }
    }

    #[test]
    fn the_policy_carries_the_pipeline_id_and_interval() {
        let policy = KafkaEosPublish::new("enrich-1").commit_interval(Duration::from_millis(250));
        assert_eq!(policy.id(), "enrich-1");
        assert_eq!(policy.interval, Duration::from_millis(250));
    }
}
