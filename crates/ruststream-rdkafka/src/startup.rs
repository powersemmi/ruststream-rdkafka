//! A group subscription between `subscribe` returning and its stream starting.
//!
//! librdkafka assigns a group's partitions only while something polls the consumer, and it
//! resolves where each partition starts only after the assignment: the group's committed offset,
//! or, when there is none, the `auto.offset.reset` end of the log. A subscription that returned
//! before either happened would miss what is published in between under the `latest` reset, so
//! `subscribe` waits here for the first assignment and writes a concrete start into every
//! assigned partition before librdkafka takes it.
//!
//! The subscription is then handed to the runtime, which may subscribe other registrations before
//! it polls any stream. A member that nobody polls cannot answer a rebalance, and a second member
//! joining its group would wait for it until the group gives up on it. So the consumer is kept:
//! a thread of the blocking pool polls it until the stream takes over, serving the group's events.
//! Its partitions are paused while it is kept, which is what keeps records off the queue the
//! keeper polls; the stream seeks each partition to its start and resumes it when it takes over.

use std::collections::HashMap;
use std::future::{Future as _, poll_fn};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::task::{Poll, Waker};
use std::time::Duration;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError as RdKafkaError, RDKafkaErrorCode};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use tokio::sync::oneshot;
use tracing::warn;

use crate::error::KafkaError;
use crate::record::{Cart, HeldRecord};
use crate::tracker::{CommitTracker, TrackedConsumer, TrackingContext};

/// How long one request resolving start positions waits for the cluster.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(10);

/// What `auto.offset.reset` makes of a partition the group has no committed offset for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reset {
    Earliest,
    Latest,
    /// The reset is an error librdkafka reports; the start is left to it.
    Error,
}

impl Reset {
    /// The reset a consumer created from `config` applies, librdkafka's spellings included.
    pub(crate) fn of(config: &ClientConfig) -> Self {
        match config.get("auto.offset.reset") {
            Some("smallest" | "earliest" | "beginning") => Self::Earliest,
            Some("error") => Self::Error,
            _ => Self::Latest,
        }
    }
}

/// A partition, as the start positions are keyed.
type PartitionKey = (String, i32);

/// Writes a concrete start into every partition of `list` whose offset librdkafka would
/// otherwise resolve after taking it: the group's committed offset, else `remembered`, else the
/// reset; the log ends are read now.
///
/// # Errors
///
/// Returns the client's error when the committed offsets or the log ends cannot be read.
pub(crate) fn resolve_starts<C>(
    consumer: &C,
    list: &mut TopicPartitionList,
    remembered: &HashMap<PartitionKey, Offset>,
    reset: Reset,
) -> Result<(), RdKafkaError>
where
    C: Consumer<TrackingContext>,
{
    let mut unresolved = TopicPartitionList::new();
    for element in list.elements() {
        if matches!(element.offset(), Offset::Invalid | Offset::Stored) {
            unresolved.add_partition(element.topic(), element.partition());
        }
    }
    if unresolved.count() > 0 {
        let committed = consumer.committed_offsets(unresolved, RESOLVE_TIMEOUT)?;
        for element in committed.elements() {
            let key = (element.topic().to_owned(), element.partition());
            let start = match element.offset() {
                Offset::Offset(offset) if offset >= 0 => Offset::Offset(offset),
                _ => match (remembered.get(&key), reset) {
                    (Some(start), _) => *start,
                    (None, Reset::Earliest) => Offset::Beginning,
                    (None, Reset::Latest) => Offset::End,
                    (None, Reset::Error) => continue,
                },
            };
            list.set_partition_offset(element.topic(), element.partition(), start)?;
        }
    }
    // A logical end resolves whenever the fetcher first asks the broker, which puts what is
    // published in the meantime behind the start: asking now pins it to this moment. Both ends
    // go in one request.
    let mut ends = TopicPartitionList::new();
    for element in list.elements() {
        if matches!(element.offset(), Offset::Beginning | Offset::End) {
            ends.add_partition_offset(element.topic(), element.partition(), element.offset())?;
        }
    }
    if ends.count() > 0 {
        let resolved = consumer.offsets_for_times(ends, RESOLVE_TIMEOUT)?;
        for element in resolved.elements() {
            element.error()?;
            if let Offset::Offset(offset) = element.offset() {
                list.set_partition_offset(
                    element.topic(),
                    element.partition(),
                    Offset::Offset(offset),
                )?;
            }
        }
    }
    Ok(())
}

/// How the wait in `subscribe` ended.
pub(crate) type Opened = Result<(), String>;

/// What the keeper and the rebalance callback share while a subscription is kept.
struct Kept {
    /// Set when the stream takes the consumer over, or the subscription closes before it does.
    released: bool,
    /// Answers the wait in `subscribe`, once.
    opened: Option<oneshot::Sender<Opened>>,
    /// Where each partition this member was given starts: what the stream seeks it to when it
    /// takes over, and where a partition handed back to this member resumes, since nothing has
    /// read it since.
    starts: HashMap<PartitionKey, Offset>,
    /// Wakes the keeper so it sees the release.
    waker: Option<Waker>,
    /// What the consumer last reported while the subscription waited, for the error a wait that
    /// runs out names.
    last_error: Option<String>,
}

/// A group subscription's state before its stream starts; see the module documentation.
pub(crate) struct Startup {
    /// Read once per stream opened, without the lock.
    released: AtomicBool,
    /// Held across each poll of the keeper and across the hand-over, so the two never poll the
    /// consumer at once: a record the keeper took after the hand-over would never be delivered.
    polling: Mutex<()>,
    kept: Mutex<Kept>,
    reset: Reset,
    /// The names subscribed that are not patterns: whether the cluster has any of them decides
    /// whether an assignment can come at all.
    literals: Vec<String>,
    /// Whether a subscribed name is a pattern.
    patterns: bool,
}

impl std::fmt::Debug for Startup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Startup")
            .field("released", &self.released.load(Ordering::Relaxed))
            .field("reset", &self.reset)
            .finish_non_exhaustive()
    }
}

impl Startup {
    /// A group subscription to `names`, kept until its stream starts, and the wait for its first
    /// assignment.
    pub(crate) fn kept(names: &[String], reset: Reset) -> (Self, oneshot::Receiver<Opened>) {
        let (opened, wait) = oneshot::channel();
        let startup = Self {
            released: AtomicBool::new(false),
            polling: Mutex::new(()),
            kept: Mutex::new(Kept {
                released: false,
                opened: Some(opened),
                starts: HashMap::new(),
                waker: None,
                last_error: None,
            }),
            reset,
            literals: names
                .iter()
                .filter(|name| !name.starts_with('^'))
                .cloned()
                .collect(),
            patterns: names.iter().any(|name| name.starts_with('^')),
        };
        (startup, wait)
    }

    /// A consumer that is never kept: one that names its partitions, and joins no group.
    pub(crate) fn released() -> Self {
        Self {
            released: AtomicBool::new(true),
            polling: Mutex::new(()),
            kept: Mutex::new(Kept {
                released: true,
                opened: None,
                starts: HashMap::new(),
                waker: None,
                last_error: None,
            }),
            reset: Reset::Latest,
            literals: Vec::new(),
            patterns: false,
        }
    }

    fn lock(&self) -> MutexGuard<'_, Kept> {
        self.kept.lock().expect("startup mutex poisoned")
    }

    fn lock_polling(&self) -> MutexGuard<'_, ()> {
        self.polling.lock().expect("startup polling mutex poisoned")
    }

    /// What the consumer last reported while the subscription waited.
    pub(crate) fn last_error(&self) -> Option<String> {
        self.lock().last_error.clone()
    }

    /// Prepares an assignment the group just handed over, when the subscription is still kept:
    /// every partition gets a concrete start. Returns whether the subscription is kept, which
    /// is what [`assigned`](Self::assigned) is told after librdkafka took the assignment.
    pub(crate) fn assigning<C>(&self, consumer: &C, partitions: &mut TopicPartitionList) -> bool
    where
        C: Consumer<TrackingContext>,
    {
        // Copied out rather than read under the lock: resolving asks the cluster, and a
        // subscription closing meanwhile must not wait for the answer.
        let remembered = {
            let kept = self.lock();
            if kept.released {
                return false;
            }
            kept.starts.clone()
        };
        if let Err(err) = resolve_starts(consumer, partitions, &remembered, self.reset) {
            let reason = format!("the start positions of its partitions could not be read: {err}");
            let opened = self.lock().opened.take();
            if let Some(opened) = opened {
                let _ = opened.send(Err(reason));
            } else {
                // librdkafka resolves what was left unresolved itself, as it does for a
                // subscription that is not kept.
                warn!(error = %err, "{reason}");
            }
        }
        true
    }

    /// Finishes an assignment of a kept subscription once librdkafka took it: its partitions are
    /// paused until the stream takes over, their starts are remembered, and the first one answers
    /// the wait in `subscribe`.
    pub(crate) fn assigned<C>(
        &self,
        consumer: &C,
        partitions: &TopicPartitionList,
        applied: Result<(), &RdKafkaError>,
    ) where
        C: Consumer<TrackingContext>,
    {
        // Paused after the assignment, because a partition librdkafka has not been given yet has
        // no state to pause. The pause fences off what was fetched before it: that is dropped,
        // and the stream seeks the partition back to its start when it takes over.
        if partitions.count() > 0
            && let Err(err) = consumer.pause(partitions)
        {
            warn!(error = %err, "a kept subscription could not pause its partitions");
        }
        let mut kept = self.lock();
        for element in partitions.elements() {
            if matches!(element.offset(), Offset::Offset(_)) {
                kept.starts.insert(
                    (element.topic().to_owned(), element.partition()),
                    element.offset(),
                );
            }
        }
        if let Some(opened) = kept.opened.take() {
            let _ = opened.send(
                applied.map_err(|err| format!("librdkafka did not take its assignment: {err}")),
            );
        }
    }

    /// Releases the pause of partitions the group takes back from a kept subscription: a pause
    /// outlives the assignment in librdkafka, and would hold the partition if it came back after
    /// the stream took over.
    pub(crate) fn revoking<C>(&self, consumer: &C, partitions: &TopicPartitionList)
    where
        C: Consumer<TrackingContext>,
    {
        if self.lock().released || partitions.count() == 0 {
            return;
        }
        if let Err(err) = consumer.resume(partitions) {
            warn!(error = %err, "a kept subscription could not release its revoked partitions");
        }
    }

    /// Records where a seek moved a kept subscription, so the stream starts there.
    pub(crate) fn moved(&self, targets: &TopicPartitionList) {
        let mut kept = self.lock();
        if kept.released {
            return;
        }
        for element in targets.elements() {
            kept.starts.insert(
                (element.topic().to_owned(), element.partition()),
                element.offset(),
            );
        }
    }

    /// Hands the consumer over to the stream: the keeper stops, and every assigned partition
    /// resumes from its start. A failure is left to the stream's side channel, since a
    /// subscription that does not read from where it was opened must not pass for a working one.
    pub(crate) fn hand_over(&self, consumer: &TrackedConsumer, tracker: &CommitTracker) {
        if self.released.load(Ordering::Acquire) {
            return;
        }
        let _polling = self.lock_polling();
        let mut kept = self.lock();
        if kept.released {
            return;
        }
        self.release_locked(&mut kept);
        if let Err(err) = resume_from_starts(consumer, &kept.starts) {
            tracker
                .attention
                .record_start_failure(KafkaError::InvalidOptions(format!(
                    "subscription {:?} could not resume its partitions from where it was opened: \
                     {err}",
                    consumer.context().subscription(),
                )));
        }
    }

    /// Stops the keeper of a subscription that closes before its stream started.
    pub(crate) fn release(&self) {
        if self.released.load(Ordering::Acquire) {
            return;
        }
        let mut kept = self.lock();
        self.release_locked(&mut kept);
    }

    fn release_locked(&self, kept: &mut Kept) {
        kept.released = true;
        self.released.store(true, Ordering::Release);
        if let Some(waker) = kept.waker.take() {
            waker.wake();
        }
    }

    /// A record the keeper took although the partition was paused: the stream takes the
    /// partition over from that record at the latest, and the partition is paused again.
    fn took<C>(&self, consumer: &C, topic: &str, partition: i32, offset: i64)
    where
        C: Consumer<TrackingContext>,
    {
        self.lock()
            .starts
            .entry((topic.to_owned(), partition))
            .and_modify(|start| {
                if matches!(start, Offset::Offset(known) if *known > offset) {
                    *start = Offset::Offset(offset);
                }
            })
            .or_insert(Offset::Offset(offset));
        let mut list = TopicPartitionList::new();
        list.add_partition(topic, partition);
        if let Err(err) = consumer.pause(&list) {
            warn!(error = %err, topic, partition, "a kept subscription could not pause a partition");
        }
    }

    /// What the consumer reported while kept. Two reports end the wait in `subscribe` early: a
    /// join the cluster refuses, which no retry changes, and a subscription that names only
    /// topics the cluster does not have, which is assigned nothing until one appears.
    fn reported<C>(&self, consumer: &C, err: &RdKafkaError)
    where
        C: Consumer<TrackingContext>,
    {
        let code = err.rdkafka_error_code();
        let mut kept = self.lock();
        kept.last_error = Some(err.to_string());
        if kept.opened.is_none() {
            return;
        }
        if code.is_some_and(refuses_the_join) {
            if let Some(opened) = kept.opened.take() {
                let _ = opened.send(Err(format!("the cluster refused its join: {err}")));
            }
            return;
        }
        if code != Some(RDKafkaErrorCode::UnknownTopicOrPartition) {
            return;
        }
        drop(kept);
        if self.patterns || !self.any_literal_exists(consumer) {
            let opened = self.lock().opened.take();
            if let Some(opened) = opened {
                let _ = opened.send(Ok(()));
            }
        }
    }

    /// Whether the cluster has any of the topics subscribed by name. A cluster that cannot be
    /// asked counts as having them, so the wait goes on to its bound.
    fn any_literal_exists<C>(&self, consumer: &C) -> bool
    where
        C: Consumer<TrackingContext>,
    {
        let Ok(metadata) = consumer.fetch_metadata(None, RESOLVE_TIMEOUT) else {
            return true;
        };
        metadata.topics().iter().any(|topic| {
            topic.error().is_none() && self.literals.iter().any(|name| name == topic.name())
        })
    }
}

/// Whether `code` is a refusal of the group join that librdkafka keeps retrying to no effect.
fn refuses_the_join(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::InconsistentGroupProtocol
            | RDKafkaErrorCode::InvalidGroupId
            | RDKafkaErrorCode::GroupAuthorizationFailed
            | RDKafkaErrorCode::TopicAuthorizationFailed
    )
}

/// Seeks every assigned partition with a remembered start back to it, then resumes the whole
/// assignment. Nothing has read a kept partition, so its start is where it resumes: the seek
/// undoes what the pause dropped, which librdkafka would otherwise skip.
fn resume_from_starts(
    consumer: &TrackedConsumer,
    starts: &HashMap<PartitionKey, Offset>,
) -> Result<(), RdKafkaError> {
    let assignment = consumer.assignment()?;
    let mut seek = TopicPartitionList::new();
    for element in assignment.elements() {
        if let Some(start) = starts.get(&(element.topic().to_owned(), element.partition())) {
            seek.add_partition_offset(element.topic(), element.partition(), *start)?;
        }
    }
    if seek.count() > 0 {
        let outcome = consumer.seek_partitions(seek, RESOLVE_TIMEOUT)?;
        for element in outcome.elements() {
            element.error()?;
        }
    }
    consumer.resume(&assignment)
}

/// Polls a kept consumer until its stream takes over, serving the group's events. Runs on the
/// blocking pool: the rebalance callbacks it serves ask the cluster for start positions.
///
/// It reads through the consume loop's own functions, so the client's code keeps one caller and
/// is inlined into that loop as before.
pub(crate) fn keep(cart: &Cart) {
    let client: &StreamConsumer<TrackingContext> = &cart.consumer;
    let startup = &client.context().startup;
    let mut next = pin!(HeldRecord::next(cart));
    futures::executor::block_on(poll_fn(|cx| {
        let _polling = startup.lock_polling();
        {
            let mut kept = startup.lock();
            if kept.released {
                return Poll::Ready(());
            }
            kept.waker = Some(cx.waker().clone());
        }
        loop {
            match next.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(record)) => {
                    startup.took(client, record.topic(), record.partition(), record.offset());
                }
                Poll::Ready(Err(err)) => startup.reported(client, &err),
            }
            next.set(HeldRecord::next(cart));
        }
    }));
}
