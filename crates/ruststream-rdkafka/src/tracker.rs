//! The per-partition acknowledgement watermark behind `Commit::Tracked`.
//!
//! Kafka commits a single position per partition, so per-message acknowledgement has to be
//! reduced to one: the tracker records every delivered offset, and settling advances the
//! stored position to just below the lowest still-outstanding delivery (or to the highest
//! delivered offset once none are outstanding). Only delivered offsets are tracked, so gaps in
//! the offset space that consumers never receive - transaction control records, aborted
//! batches under `read_committed`, compacted-away records - can never block the position.
//! Acks arriving out of order simply shrink the outstanding set, which keeps the committed
//! position correct under concurrent handler lanes. librdkafka's auto-commit flushes the
//! stored position in the background and once more when the consumer closes.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance};
use rdkafka::{ClientContext, TopicPartitionList};
use ruststream::Str;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use crate::error::KafkaError;
use crate::seek::{self, KafkaPosition};

/// Which partition's bookkeeping a delivery settles into, and which read position it belongs to.
///
/// Resolved once, where the record is pulled, and carried by the delivery from there: settling
/// indexes the partition instead of hashing its name again. The generation is what makes a settle
/// that arrives after a reposition (a seek, a revoke) a no-op rather than a move of a position
/// that no longer describes what this consumer reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartitionSlot {
    index: usize,
    generation: u64,
}

/// The partitions this subscription has delivered from, and the slot each one owns.
///
/// Entries are never removed - a revoked partition keeps its slot and takes a new generation -
/// so a slot handed to a delivery indexes the same partition for as long as that delivery lives.
#[derive(Debug, Default)]
struct Assigned {
    states: Vec<PartitionState>,
    /// Consulted when a partition is seen for the first time since the one before it, never
    /// once a run of its records is flowing.
    slots: HashMap<(Str, i32), usize>,
    /// The slot the last lookup resolved. A fetch hands a consumer one partition's records at a
    /// time, so a run of deliveries asks for the same partition under the very name the
    /// subscription minted, and answering from here is a name comparison instead of a hash of
    /// it.
    last: Option<(Str, i32, usize)>,
}

impl Assigned {
    /// The slot of a partition, opening one the first time it is seen.
    fn slot(&mut self, topic: &Str, partition: i32) -> usize {
        if let Some((name, known, index)) = &self.last
            && *known == partition
            && name == topic
        {
            return *index;
        }
        let next = self.states.len();
        let index = *self.slots.entry((topic.clone(), partition)).or_insert(next);
        if index == next {
            self.states
                .push(PartitionState::new(topic.clone(), partition));
        }
        self.last = Some((topic.clone(), partition, index));
        index
    }
}

#[derive(Debug)]
struct PartitionState {
    /// The partition this state belongs to, so the pipeline can name it back.
    topic: Str,
    partition: i32,
    /// Delivered offsets that have not settled yet.
    outstanding: BTreeSet<i64>,
    /// The highest delivered offset, `None` before the first delivery of this generation.
    highest: Option<i64>,
    /// The last position handed to the offset store; kept monotonic within a generation.
    stored: Option<i64>,
    /// Bumped by every reposition of this partition (a seek, a revoke). Deliveries carry the
    /// generation they were pulled in, and settling under a superseded one is a no-op.
    generation: u64,
}

impl PartitionState {
    fn new(topic: Str, partition: i32) -> Self {
        Self {
            topic,
            partition,
            outstanding: BTreeSet::new(),
            highest: None,
            stored: None,
            generation: 0,
        }
    }

    /// Starts the offset bookkeeping over at `offset` without touching the generation: a
    /// replayed delivery is the same read position continuing, not a new one.
    fn replay_from(&mut self, offset: i64) {
        self.outstanding = BTreeSet::from([offset]);
        self.highest = Some(offset);
        self.stored = None;
    }
}

/// Shared offset bookkeeping for one subscription in `Commit::Tracked` or
/// `Commit::Transactional` mode.
#[derive(Debug, Default)]
pub(crate) struct CommitTracker {
    partitions: Mutex<Assigned>,
    /// Woken whenever a stored position advances; the EOS committer waits on it for its
    /// settle condition.
    advanced: Notify,
    /// Set by every reposition, consumed by the EOS pipeline: an open window whose sources
    /// moved underneath it must abort instead of committing offsets from the read position the
    /// seek replaced.
    repositioned: AtomicBool,
    /// Woken by every reposition, so an open window closes at once instead of holding the
    /// replayed deliveries behind an interval it can no longer commit.
    repositions: Notify,
}

impl CommitTracker {
    /// Records a delivery as outstanding and returns the generation it belongs to; settling it
    /// later must carry that generation back (see [`settle_with`](Self::settle_with)).
    ///
    /// Kafka delivers strictly increasing offsets per partition within a session, so a
    /// regressing offset means the partition is being replayed (a seek or a re-assignment);
    /// the state resets so the watermark follows the replay instead of the stale position.
    // The guard spans the whole update: the generation returned here must be the one this
    // delivery was recorded under, so a concurrent reposition cannot slip in between.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn delivered(&self, topic: &Str, partition: i32, offset: i64) -> PartitionSlot {
        let mut partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        let index = partitions.slot(topic, partition);
        let state = &mut partitions.states[index];
        match state.highest {
            Some(highest) if offset <= highest => state.replay_from(offset),
            _ => {
                state.highest = Some(offset);
                state.outstanding.insert(offset);
            }
        }
        PartitionSlot {
            index,
            generation: state.generation,
        }
    }

    /// Moves the bookkeeping of a partition to a fresh read position, as
    /// [`KafkaSeeker`](crate::KafkaSeeker) does before it repositions the consumer (and as a
    /// revoked partition does implicitly).
    ///
    /// Everything the old position knew is dropped: the outstanding set (those deliveries are
    /// no longer this subscription's to settle), the stored watermark (a commit must never
    /// advance past a message the seek replayed but nobody has handled yet), and the highest
    /// delivered offset. The generation bump is what makes an in-flight delivery pulled before
    /// the seek settle into nothing instead of into the new position.
    pub(crate) fn reposition(&self, topic: &Str, partition: i32) {
        let mut partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        let index = partitions.slot(topic, partition);
        let state = &mut partitions.states[index];
        state.generation += 1;
        state.outstanding.clear();
        state.highest = None;
        state.stored = None;
        drop(partitions);
        self.repositioned.store(true, Ordering::Release);
        self.repositions.notify_waiters();
    }

    /// Whether this subscription was repositioned since the last check, clearing the flag.
    ///
    /// The EOS pipeline takes it when a window opens (to start from a clean slate) and again
    /// when it closes (a seek during the window invalidates the offsets it would commit).
    pub(crate) fn take_repositioned(&self) -> bool {
        self.repositioned.swap(false, Ordering::AcqRel)
    }

    /// Whether a reposition is pending, without consuming it: the publish path reads this to
    /// keep new records out of a window that is already void.
    pub(crate) fn is_repositioned(&self) -> bool {
        self.repositioned.load(Ordering::Acquire)
    }

    /// A waiter for the next reposition. Create it BEFORE checking
    /// [`is_repositioned`](Self::is_repositioned), so a reposition landing between the two is
    /// not missed.
    pub(crate) fn reposition_waiter(&self) -> Notified<'_> {
        self.repositions.notified()
    }

    /// Marks `offset` settled and, when the stored position advances, hands the new position
    /// to `store` (librdkafka commits it + 1).
    ///
    /// `store` runs while the tracker lock is held - it is a cheap in-memory librdkafka call,
    /// and ordering it under the lock is what keeps concurrent settles from ever handing the
    /// offset store a regressing position.
    // Holding the guard across `store` is the point of this method, not contention to tighten.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn settle_with<E>(
        &self,
        slot: PartitionSlot,
        offset: i64,
        store: impl FnOnce(i64) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        // A slot is only ever handed out by `delivered`, and the list it indexes never shrinks.
        let state = &mut partitions.states[slot.index];
        if state.generation != slot.generation {
            // The delivery was pulled before a reposition: its offset says nothing about the
            // position the subscription reads from now, so settling it must not move anything.
            return Ok(());
        }
        if !state.outstanding.remove(&offset) {
            // A duplicate settle, or a leftover from before a replay reset.
            return Ok(());
        }
        let Some(highest) = state.highest else {
            return Ok(());
        };
        let position = state
            .outstanding
            .first()
            .map_or(highest, |lowest| lowest - 1);
        if position < 0 || state.stored.is_some_and(|stored| position <= stored) {
            // Nothing committable yet (an unsettled delivery still bounds the position).
            return Ok(());
        }
        store(position)?;
        state.stored = Some(position);
        self.advanced.notify_waiters();
        Ok(())
    }

    /// The stored (settled) position of a partition, when this tracker owns it and progress
    /// has been made. The next offset to consume - what a Kafka commit wants - is this + 1.
    pub(crate) fn stored_position(&self, topic: &Str, partition: i32) -> Option<i64> {
        let partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        partitions
            .slots
            .get(&(topic.clone(), partition))
            .and_then(|index| partitions.states[*index].stored)
    }

    /// Whether this tracker has delivered state for the partition (it belongs to this
    /// subscription in the current assignment, at its current read position).
    pub(crate) fn covers(&self, topic: &Str, partition: i32) -> bool {
        let partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        partitions
            .slots
            .get(&(topic.clone(), partition))
            .is_some_and(|index| partitions.states[*index].highest.is_some())
    }

    /// Every partition with settled progress, as `((topic, partition), stored position)`.
    pub(crate) fn stored_positions(&self) -> Vec<((Str, i32), i64)> {
        let partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        partitions
            .states
            .iter()
            .filter_map(|state| {
                state
                    .stored
                    .map(|stored| ((state.topic.clone(), state.partition), stored))
            })
            .collect()
    }

    /// A waiter for the next stored-position advance. Create it BEFORE checking the awaited
    /// condition, so an advance landing between the check and the await is not missed.
    pub(crate) fn advance_waiter(&self) -> Notified<'_> {
        self.advanced.notified()
    }

    /// Resets the state of revoked partitions so a later re-assignment starts fresh.
    ///
    /// A revoke is a reposition like any other: the assignment the offsets belonged to is gone,
    /// which is exactly why a rebalance discards a seek - the subscription resumes from the
    /// committed offsets when the partition comes back.
    fn revoke(&self, revoked: &TopicPartitionList) {
        for element in revoked.elements() {
            self.reposition(&Str::from(element.topic()), element.partition());
        }
    }
}

/// Consumer context that resets the tracker when partitions are revoked in a rebalance, and that
/// carries a subscription's start position to the assignment it applies to.
pub(crate) struct TrackingContext {
    tracker: Arc<CommitTracker>,
    /// The subscription's name, so a rebalance that cannot honour its start position says which
    /// subscription it was.
    subscription: String,
    /// The position a `start_at(..)` named, waiting for partitions to apply it to. A group
    /// assigns nothing until something polls the consumer, and the runtime polls only after the
    /// subscription is open, so the position outlives the seek call that carried it.
    start: Mutex<Option<KafkaPosition>>,
    /// A start position the rebalance could not apply. Nothing can be returned from a
    /// librdkafka callback, and a subscription silently reading from somewhere other than the
    /// position it was opened at is what this wave exists to catch, so the failure waits here
    /// for the subscriber's stream to yield it.
    start_failure: Mutex<Option<KafkaError>>,
    /// Whether `start_failure` holds anything. The consume loop asks once per delivery, and a
    /// rebalance that could not open the subscription is a once-per-lifetime event, so the
    /// answer is read from here instead of by taking the mutex on every record.
    start_failed: AtomicBool,
    /// Woken when a start failure is recorded, so a stream waiting on a topic that may never
    /// deliver anything still reports it.
    start_failures: Notify,
}

impl TrackingContext {
    pub(crate) fn new(tracker: Arc<CommitTracker>, subscription: impl Into<String>) -> Self {
        Self {
            tracker,
            subscription: subscription.into(),
            start: Mutex::new(None),
            start_failure: Mutex::new(None),
            start_failed: AtomicBool::new(false),
            start_failures: Notify::new(),
        }
    }

    /// Keeps `position` for the next assignment, as a seek on a consumer holding nothing does.
    pub(crate) fn hold_start(&self, position: KafkaPosition) {
        *self.start.lock().expect("start position mutex poisoned") = Some(position);
    }

    /// Takes the held start position, if any. It applies to one assignment: a later rebalance
    /// discards a reposition like any other, and the group's committed offsets take over.
    fn take_start(&self) -> Option<KafkaPosition> {
        self.start
            .lock()
            .expect("start position mutex poisoned")
            .take()
    }

    /// Records a start position the rebalance could not apply, keeping the first one: it names
    /// what went wrong, and the ones behind it are the same assignment failing again.
    fn record_start_failure(&self, err: KafkaError) {
        let mut slot = self
            .start_failure
            .lock()
            .expect("start failure mutex poisoned");
        if slot.is_none() {
            *slot = Some(err);
        }
        drop(slot);
        // Released after the slot is filled, so a reader that sees the flag sees the failure.
        self.start_failed.store(true, Ordering::Release);
        self.start_failures.notify_waiters();
    }

    /// Takes the recorded start failure, for the subscriber to yield on its stream.
    pub(crate) fn take_start_failure(&self) -> Option<KafkaError> {
        if !self.start_failed.load(Ordering::Acquire) {
            return None;
        }
        let taken = self
            .start_failure
            .lock()
            .expect("start failure mutex poisoned")
            .take();
        self.start_failed.store(taken.is_none(), Ordering::Release);
        taken
    }

    /// A waiter for the next start failure. Create it BEFORE calling
    /// [`take_start_failure`](Self::take_start_failure), so one landing between the two is not
    /// missed.
    pub(crate) fn start_failure_waiter(&self) -> Notified<'_> {
        self.start_failures.notified()
    }
}

impl ClientContext for TrackingContext {}

impl ConsumerContext for TrackingContext {
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(revoked) = rebalance {
            self.tracker.revoke(revoked);
        }
    }

    /// Opens the subscription at its start position, on the partitions the group just handed
    /// over.
    ///
    /// This runs inside the poll that carried the assignment, so it is ahead of the first record
    /// those partitions deliver. It is the only point at which a `start_at(..)` on a group
    /// subscription can take effect: the position is named before anything polls the consumer,
    /// and until something does, the group has assigned nothing to seek.
    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let Rebalance::Assign(assigned) = rebalance else {
            return;
        };
        let Some(position) = self.take_start() else {
            return;
        };
        if let Err(err) = seek::apply_position(consumer, &self.tracker, assigned, &position) {
            // A callback can return nothing, so the failure travels to the subscriber instead:
            // reading from the group's committed position when the mount site named another one
            // is exactly what must not pass for a working subscription.
            self.record_start_failure(KafkaError::InvalidOptions(format!(
                "subscription {:?} could not be opened at its start position: {err}",
                self.subscription,
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    /// The name a subscription would have minted, which is what the real path carries.
    fn topic() -> Str {
        Str::from_static("t")
    }

    /// Runs a settle in the partition's current slot and returns the position it stored.
    fn settle(tracker: &CommitTracker, offset: i64) -> Option<i64> {
        settle_in(tracker, offset, slot(tracker, "t", 0))
    }

    /// Runs a settle carrying `slot`, returning the position it stored, if any.
    fn settle_in(tracker: &CommitTracker, offset: i64, slot: PartitionSlot) -> Option<i64> {
        let mut stored = None;
        tracker
            .settle_with(slot, offset, |position| {
                stored = Some(position);
                Ok::<(), Infallible>(())
            })
            .expect("infallible");
        stored
    }

    /// The current slot of a partition, without recording a delivery.
    fn slot(tracker: &CommitTracker, topic: &str, partition: i32) -> PartitionSlot {
        let mut partitions = tracker
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        let index = partitions.slot(&Str::from(topic), partition);
        PartitionSlot {
            index,
            generation: partitions.states[index].generation,
        }
    }

    #[test]
    fn contiguous_acks_advance_the_position() {
        let tracker = CommitTracker::default();
        tracker.delivered(&topic(), 0, 5);
        tracker.delivered(&topic(), 0, 6);
        assert_eq!(settle(&tracker, 5), Some(5));
        assert_eq!(settle(&tracker, 6), Some(6));
    }

    #[test]
    fn offset_gaps_never_block_the_position() {
        // Offset 2 is a gap the consumer never receives (a transaction marker or a
        // compacted-away record): settling around it must still advance.
        let tracker = CommitTracker::default();
        tracker.delivered(&topic(), 0, 0);
        tracker.delivered(&topic(), 0, 1);
        tracker.delivered(&topic(), 0, 3);
        assert_eq!(settle(&tracker, 0), Some(0));
        assert_eq!(settle(&tracker, 1), Some(2));
        assert_eq!(settle(&tracker, 3), Some(3));
    }

    #[test]
    fn out_of_order_acks_stay_bounded_by_the_lowest_outstanding() {
        let tracker = CommitTracker::default();
        for offset in 3..=5 {
            tracker.delivered(&topic(), 0, offset);
        }
        assert_eq!(settle(&tracker, 4), Some(2));
        assert_eq!(settle(&tracker, 5), None);
        assert_eq!(settle(&tracker, 3), Some(5));
    }

    #[test]
    fn unsettled_delivery_blocks_the_position() {
        let tracker = CommitTracker::default();
        for offset in 0..3 {
            tracker.delivered(&topic(), 0, offset);
        }
        // Offset 0 is never settled (a nack(true) hole): nothing may be stored.
        assert_eq!(settle(&tracker, 1), None);
        assert_eq!(settle(&tracker, 2), None);
    }

    #[test]
    fn partitions_are_tracked_independently() {
        let tracker = CommitTracker::default();
        tracker.delivered(&topic(), 0, 10);
        let one = tracker.delivered(&topic(), 1, 20);
        assert_eq!(settle_in(&tracker, 20, one), Some(20));
        assert_eq!(settle(&tracker, 10), Some(10));
    }

    #[test]
    fn a_slot_settles_the_partition_it_was_resolved_for() {
        // Three partitions of two topics, delivered in an interleaved order so a slot that
        // pointed at the wrong one would advance a position that belongs to another partition.
        let other = Str::from_static("u");
        let tracker = CommitTracker::default();
        let a = tracker.delivered(&topic(), 0, 100);
        let b = tracker.delivered(&topic(), 1, 200);
        let c = tracker.delivered(&other, 0, 300);

        assert_eq!(settle_in(&tracker, 200, b), Some(200));
        assert_eq!(tracker.stored_position(&topic(), 1), Some(200));
        assert_eq!(tracker.stored_position(&topic(), 0), None);
        assert_eq!(tracker.stored_position(&other, 0), None);

        assert_eq!(settle_in(&tracker, 300, c), Some(300));
        assert_eq!(settle_in(&tracker, 100, a), Some(100));
        let mut stored = tracker.stored_positions();
        stored.sort();
        assert_eq!(
            stored,
            vec![((topic(), 0), 100), ((topic(), 1), 200), ((other, 0), 300),],
        );
    }

    #[test]
    fn a_partition_keeps_its_slot_across_repositions() {
        // A revoke and a re-assignment of the same partition must not open a second slot: the
        // position the new generation stores has to be the one the pipeline reads back.
        let tracker = CommitTracker::default();
        let before = tracker.delivered(&topic(), 0, 5);
        tracker.reposition(&topic(), 0);
        let after = tracker.delivered(&topic(), 0, 9);
        assert_eq!(
            before.index, after.index,
            "the same partition keeps one slot across a reposition",
        );
        assert_eq!(settle_in(&tracker, 9, after), Some(9));
        assert_eq!(tracker.stored_positions(), vec![((topic(), 0), 9)]);
    }

    #[test]
    fn a_reposition_drops_the_settles_of_deliveries_pulled_before_it() {
        let tracker = CommitTracker::default();
        let before = tracker.delivered(&topic(), 0, 10);
        // The seek target is offset 4: nothing at or past 10 may be committed any more.
        tracker.reposition(&topic(), 0);
        assert_eq!(
            settle_in(&tracker, 10, before),
            None,
            "a delivery pulled before the seek must not advance the position",
        );

        // The replayed deliveries carry the new generation and store normally.
        let after = tracker.delivered(&topic(), 0, 4);
        assert_ne!(after, before, "a reposition must start a new generation");
        assert_eq!(settle_in(&tracker, 4, after), Some(4));
    }

    #[test]
    fn a_reposition_clears_the_stored_watermark() {
        let tracker = CommitTracker::default();
        tracker.delivered(&topic(), 0, 7);
        assert_eq!(settle(&tracker, 7), Some(7));
        tracker.reposition(&topic(), 0);
        assert_eq!(
            tracker.stored_position(&topic(), 0),
            None,
            "the watermark of the replaced read position must not survive the seek",
        );
        assert!(!tracker.covers(&topic(), 0), "nothing is delivered yet");

        // A commit may only resume from what the new position actually delivered.
        let generation = tracker.delivered(&topic(), 0, 2);
        assert_eq!(settle_in(&tracker, 2, generation), Some(2));
    }

    #[test]
    fn the_reposition_flag_is_taken_once() {
        let tracker = CommitTracker::default();
        assert!(!tracker.take_repositioned());
        tracker.reposition(&topic(), 0);
        assert!(tracker.take_repositioned());
        assert!(
            !tracker.take_repositioned(),
            "the flag reports each reposition to the pipeline exactly once",
        );
    }

    #[test]
    fn replay_resets_the_partition_state() {
        let tracker = CommitTracker::default();
        tracker.delivered(&topic(), 0, 10);
        assert_eq!(settle(&tracker, 10), Some(10));
        // A replay from an earlier offset (seek / re-assignment) starts the state over, and
        // the monotonic guard resets with it: the replayed offsets store again.
        tracker.delivered(&topic(), 0, 4);
        assert_eq!(settle(&tracker, 4), Some(4));
    }

    #[test]
    fn duplicate_settles_are_ignored() {
        let tracker = CommitTracker::default();
        tracker.delivered(&topic(), 0, 0);
        tracker.delivered(&topic(), 0, 1);
        assert_eq!(settle(&tracker, 0), Some(0));
        assert_eq!(settle(&tracker, 0), None);
        assert_eq!(settle(&tracker, 1), Some(1));
    }

    #[test]
    fn store_failure_is_retried_by_the_next_settle() {
        let tracker = CommitTracker::default();
        tracker.delivered(&topic(), 0, 0);
        tracker.delivered(&topic(), 0, 1);
        let failed: Result<(), &str> =
            tracker.settle_with(slot(&tracker, "t", 0), 0, |_| Err("store failed"));
        assert!(failed.is_err());
        // The failed settle consumed offset 0; the next settle advances past both.
        assert_eq!(settle(&tracker, 1), Some(1));
    }
}
