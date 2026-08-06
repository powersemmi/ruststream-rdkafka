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
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

#[derive(Debug, Default)]
struct PartitionState {
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
    partitions: Mutex<HashMap<(String, i32), PartitionState>>,
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
    pub(crate) fn delivered(&self, topic: &str, partition: i32, offset: i64) -> u64 {
        let mut partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        let state = partitions.entry((topic.to_owned(), partition)).or_default();
        match state.highest {
            Some(highest) if offset <= highest => state.replay_from(offset),
            _ => {
                state.highest = Some(offset);
                state.outstanding.insert(offset);
            }
        }
        state.generation
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
    pub(crate) fn reposition(&self, topic: &str, partition: i32) {
        let mut partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        let state = partitions.entry((topic.to_owned(), partition)).or_default();
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
        topic: &str,
        partition: i32,
        offset: i64,
        generation: u64,
        store: impl FnOnce(i64) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        let Some(state) = partitions.get_mut(&(topic.to_owned(), partition)) else {
            // The partition was revoked (or replay-reset) after this delivery; its position is
            // no longer ours to advance.
            return Ok(());
        };
        if state.generation != generation {
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
    pub(crate) fn stored_position(&self, topic: &str, partition: i32) -> Option<i64> {
        let partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        partitions
            .get(&(topic.to_owned(), partition))
            .and_then(|state| state.stored)
    }

    /// Whether this tracker has delivered state for the partition (it belongs to this
    /// subscription in the current assignment, at its current read position).
    pub(crate) fn covers(&self, topic: &str, partition: i32) -> bool {
        let partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        partitions
            .get(&(topic.to_owned(), partition))
            .is_some_and(|state| state.highest.is_some())
    }

    /// Every partition with settled progress, as `((topic, partition), stored position)`.
    pub(crate) fn stored_positions(&self) -> Vec<((String, i32), i64)> {
        let partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        partitions
            .iter()
            .filter_map(|(key, state)| state.stored.map(|stored| (key.clone(), stored)))
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
            self.reposition(element.topic(), element.partition());
        }
    }
}

/// Consumer context that resets the tracker when partitions are revoked in a rebalance.
pub(crate) struct TrackingContext {
    tracker: Arc<CommitTracker>,
}

impl TrackingContext {
    pub(crate) fn new(tracker: Arc<CommitTracker>) -> Self {
        Self { tracker }
    }
}

impl ClientContext for TrackingContext {}

impl ConsumerContext for TrackingContext {
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(revoked) = rebalance {
            self.tracker.revoke(revoked);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    /// Runs a settle in the partition's current generation and returns the position it stored.
    fn settle(tracker: &CommitTracker, offset: i64) -> Option<i64> {
        settle_in(tracker, offset, generation(tracker, "t", 0))
    }

    /// Runs a settle carrying `generation`, returning the position it stored, if any.
    fn settle_in(tracker: &CommitTracker, offset: i64, generation: u64) -> Option<i64> {
        let mut stored = None;
        tracker
            .settle_with("t", 0, offset, generation, |position| {
                stored = Some(position);
                Ok::<(), Infallible>(())
            })
            .expect("infallible");
        stored
    }

    /// The current generation of a partition, without recording a delivery.
    fn generation(tracker: &CommitTracker, topic: &str, partition: i32) -> u64 {
        tracker
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned")
            .get(&(topic.to_owned(), partition))
            .map_or(0, |state| state.generation)
    }

    #[test]
    fn contiguous_acks_advance_the_position() {
        let tracker = CommitTracker::default();
        tracker.delivered("t", 0, 5);
        tracker.delivered("t", 0, 6);
        assert_eq!(settle(&tracker, 5), Some(5));
        assert_eq!(settle(&tracker, 6), Some(6));
    }

    #[test]
    fn offset_gaps_never_block_the_position() {
        // Offset 2 is a gap the consumer never receives (a transaction marker or a
        // compacted-away record): settling around it must still advance.
        let tracker = CommitTracker::default();
        tracker.delivered("t", 0, 0);
        tracker.delivered("t", 0, 1);
        tracker.delivered("t", 0, 3);
        assert_eq!(settle(&tracker, 0), Some(0));
        assert_eq!(settle(&tracker, 1), Some(2));
        assert_eq!(settle(&tracker, 3), Some(3));
    }

    #[test]
    fn out_of_order_acks_stay_bounded_by_the_lowest_outstanding() {
        let tracker = CommitTracker::default();
        for offset in 3..=5 {
            tracker.delivered("t", 0, offset);
        }
        assert_eq!(settle(&tracker, 4), Some(2));
        assert_eq!(settle(&tracker, 5), None);
        assert_eq!(settle(&tracker, 3), Some(5));
    }

    #[test]
    fn unsettled_delivery_blocks_the_position() {
        let tracker = CommitTracker::default();
        for offset in 0..3 {
            tracker.delivered("t", 0, offset);
        }
        // Offset 0 is never settled (a nack(true) hole): nothing may be stored.
        assert_eq!(settle(&tracker, 1), None);
        assert_eq!(settle(&tracker, 2), None);
    }

    #[test]
    fn partitions_are_tracked_independently() {
        let tracker = CommitTracker::default();
        tracker.delivered("t", 0, 10);
        let one = tracker.delivered("t", 1, 20);
        let mut stored = None;
        tracker
            .settle_with("t", 1, 20, one, |position| {
                stored = Some(position);
                Ok::<(), Infallible>(())
            })
            .expect("infallible");
        assert_eq!(stored, Some(20));
        assert_eq!(settle(&tracker, 10), Some(10));
    }

    #[test]
    fn a_reposition_drops_the_settles_of_deliveries_pulled_before_it() {
        let tracker = CommitTracker::default();
        let before = tracker.delivered("t", 0, 10);
        // The seek target is offset 4: nothing at or past 10 may be committed any more.
        tracker.reposition("t", 0);
        assert_eq!(
            settle_in(&tracker, 10, before),
            None,
            "a delivery pulled before the seek must not advance the position",
        );

        // The replayed deliveries carry the new generation and store normally.
        let after = tracker.delivered("t", 0, 4);
        assert_ne!(after, before, "a reposition must start a new generation");
        assert_eq!(settle_in(&tracker, 4, after), Some(4));
    }

    #[test]
    fn a_reposition_clears_the_stored_watermark() {
        let tracker = CommitTracker::default();
        tracker.delivered("t", 0, 7);
        assert_eq!(settle(&tracker, 7), Some(7));
        tracker.reposition("t", 0);
        assert_eq!(
            tracker.stored_position("t", 0),
            None,
            "the watermark of the replaced read position must not survive the seek",
        );
        assert!(!tracker.covers("t", 0), "nothing is delivered yet");

        // A commit may only resume from what the new position actually delivered.
        let generation = tracker.delivered("t", 0, 2);
        assert_eq!(settle_in(&tracker, 2, generation), Some(2));
    }

    #[test]
    fn the_reposition_flag_is_taken_once() {
        let tracker = CommitTracker::default();
        assert!(!tracker.take_repositioned());
        tracker.reposition("t", 0);
        assert!(tracker.take_repositioned());
        assert!(
            !tracker.take_repositioned(),
            "the flag reports each reposition to the pipeline exactly once",
        );
    }

    #[test]
    fn replay_resets_the_partition_state() {
        let tracker = CommitTracker::default();
        tracker.delivered("t", 0, 10);
        assert_eq!(settle(&tracker, 10), Some(10));
        // A replay from an earlier offset (seek / re-assignment) starts the state over, and
        // the monotonic guard resets with it: the replayed offsets store again.
        tracker.delivered("t", 0, 4);
        assert_eq!(settle(&tracker, 4), Some(4));
    }

    #[test]
    fn duplicate_settles_are_ignored() {
        let tracker = CommitTracker::default();
        tracker.delivered("t", 0, 0);
        tracker.delivered("t", 0, 1);
        assert_eq!(settle(&tracker, 0), Some(0));
        assert_eq!(settle(&tracker, 0), None);
        assert_eq!(settle(&tracker, 1), Some(1));
    }

    #[test]
    fn store_failure_is_retried_by_the_next_settle() {
        let tracker = CommitTracker::default();
        tracker.delivered("t", 0, 0);
        tracker.delivered("t", 0, 1);
        let generation = generation(&tracker, "t", 0);
        let failed: Result<(), &str> =
            tracker.settle_with("t", 0, 0, generation, |_| Err("store failed"));
        assert!(failed.is_err());
        // The failed settle consumed offset 0; the next settle advances past both.
        assert_eq!(settle(&tracker, 1), Some(1));
    }
}
