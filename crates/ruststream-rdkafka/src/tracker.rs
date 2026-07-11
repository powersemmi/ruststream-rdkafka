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
use std::sync::{Arc, Mutex};

use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance};
use rdkafka::{ClientContext, TopicPartitionList};
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

#[derive(Debug)]
struct PartitionState {
    /// Delivered offsets that have not settled yet.
    outstanding: BTreeSet<i64>,
    /// The highest delivered offset.
    highest: i64,
    /// The last position handed to the offset store; kept monotonic within a session.
    stored: Option<i64>,
}

impl PartitionState {
    fn starting_at(offset: i64) -> Self {
        Self {
            outstanding: BTreeSet::from([offset]),
            highest: offset,
            stored: None,
        }
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
}

impl CommitTracker {
    /// Records a delivery as outstanding.
    ///
    /// Kafka delivers strictly increasing offsets per partition within a session, so a
    /// regressing offset means the partition is being replayed (a seek or a re-assignment);
    /// the state resets so the watermark follows the replay instead of the stale position.
    pub(crate) fn delivered(&self, topic: &str, partition: i32, offset: i64) {
        let mut partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        partitions
            .entry((topic.to_owned(), partition))
            .and_modify(|state| {
                if offset <= state.highest {
                    *state = PartitionState::starting_at(offset);
                } else {
                    state.highest = offset;
                    state.outstanding.insert(offset);
                }
            })
            .or_insert_with(|| PartitionState::starting_at(offset));
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
        if !state.outstanding.remove(&offset) {
            // A duplicate settle, or a leftover from before a replay reset.
            return Ok(());
        }
        let position = state
            .outstanding
            .first()
            .map_or(state.highest, |lowest| lowest - 1);
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
    /// subscription in the current assignment).
    pub(crate) fn covers(&self, topic: &str, partition: i32) -> bool {
        let partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        partitions.contains_key(&(topic.to_owned(), partition))
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

    /// Drops the state of revoked partitions so a later re-assignment starts fresh.
    fn clear(&self, revoked: &TopicPartitionList) {
        let mut partitions = self
            .partitions
            .lock()
            .expect("commit tracker mutex poisoned");
        for element in revoked.elements() {
            partitions.remove(&(element.topic().to_owned(), element.partition()));
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
            self.tracker.clear(revoked);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    /// Runs a settle and returns the position it stored, if any.
    fn settle(tracker: &CommitTracker, offset: i64) -> Option<i64> {
        let mut stored = None;
        tracker
            .settle_with("t", 0, offset, |position| {
                stored = Some(position);
                Ok::<(), Infallible>(())
            })
            .expect("infallible");
        stored
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
        tracker.delivered("t", 1, 20);
        let mut stored = None;
        tracker
            .settle_with("t", 1, 20, |position| {
                stored = Some(position);
                Ok::<(), Infallible>(())
            })
            .expect("infallible");
        assert_eq!(stored, Some(20));
        assert_eq!(settle(&tracker, 10), Some(10));
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
        let failed: Result<(), &str> = tracker.settle_with("t", 0, 0, |_| Err("store failed"));
        assert!(failed.is_err());
        // The failed settle consumed offset 0; the next settle advances past both.
        assert_eq!(settle(&tracker, 1), Some(1));
    }
}
