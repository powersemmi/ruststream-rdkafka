//! Repositioning a live subscription: the Kafka position vocabulary and the seeker that
//! applies it.
//!
//! Kafka is a replayable log, so a running consumer can be moved: back to reprocess, forward to
//! skip a poison region, or to the first record at a wall-clock time. The reposition applies to
//! the partitions **this consumer instance** currently holds, not to the group - other members
//! keep reading where they were, and nothing is committed on their behalf.

use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::{Offset, TopicPartitionList};
use ruststream::Seeker;
use tokio::task;

use crate::error::KafkaError;
use crate::tracker::{CommitTracker, TrackingContext};

/// How long a reposition waits for librdkafka (the seek itself, and the timestamp lookup).
const SEEK_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a reposition waits for the group to assign partitions before giving up. A seek
/// issued at startup (the `start_at(..)` clause) runs before the first fetch, so the assignment
/// may still be in flight.
const ASSIGNMENT_TIMEOUT: Duration = Duration::from_secs(30);
const ASSIGNMENT_POLL: Duration = Duration::from_millis(50);

/// Where a subscription should resume reading.
///
/// The stream-wide variants ([`Earliest`](Self::Earliest), [`Latest`](Self::Latest),
/// [`Timestamp`](Self::Timestamp)) apply to every partition currently assigned to this
/// consumer; [`Offset`](Self::Offset) names one partition. Build them with the constructors
/// ([`earliest`](Self::earliest), [`offset`](Self::offset), ...) - the variants are what the
/// seeker matches on.
///
/// # Examples
///
/// ```
/// use ruststream_rdkafka::KafkaPosition;
///
/// let replay_all = KafkaPosition::earliest();
/// let skip_ahead = KafkaPosition::offset(3, 1_024);
/// let since_noon = KafkaPosition::timestamp(1_767_000_000_000);
/// # let _ = (replay_all, skip_ahead, since_noon);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum KafkaPosition {
    /// The earliest offset still retained, on every assigned partition.
    Earliest,
    /// The end of the log, on every assigned partition, as the broker reports it while the seek
    /// runs: records published after that point arrive normally, everything before is skipped.
    Latest,
    /// The first record at or after this timestamp (epoch milliseconds), resolved per assigned
    /// partition. A partition with no such record resumes at its end.
    Timestamp(i64),
    /// An absolute offset on one partition.
    Offset {
        /// The topic, when the position names one. [`Positioned`](ruststream::Positioned)
        /// captures the delivery's own topic here; [`offset`](Self::offset) leaves it unset,
        /// which repositions that partition index on every assigned topic (one topic being the
        /// usual case).
        topic: Option<String>,
        /// The partition to reposition.
        partition: i32,
        /// The offset to resume from: the record at this offset is delivered next.
        offset: i64,
    },
}

impl KafkaPosition {
    /// Every assigned partition, at the earliest retained offset.
    #[must_use]
    pub const fn earliest() -> Self {
        Self::Earliest
    }

    /// Every assigned partition, at the end of the log.
    #[must_use]
    pub const fn latest() -> Self {
        Self::Latest
    }

    /// One partition, at an absolute offset.
    #[must_use]
    pub const fn offset(partition: i32, offset: i64) -> Self {
        Self::Offset {
            topic: None,
            partition,
            offset,
        }
    }

    /// One partition of one topic, at an absolute offset. The form a delivery's
    /// [`position`](ruststream::Positioned::position) returns, and what a multi-topic
    /// subscription needs to name a partition unambiguously.
    #[must_use]
    pub fn topic_offset(topic: impl Into<String>, partition: i32, offset: i64) -> Self {
        Self::Offset {
            topic: Some(topic.into()),
            partition,
            offset,
        }
    }

    /// Every assigned partition, at the first record whose timestamp is at or after
    /// `when_millis` (epoch milliseconds).
    #[must_use]
    pub const fn timestamp(when_millis: i64) -> Self {
        Self::Timestamp(when_millis)
    }
}

/// Repositions a live [`KafkaSubscriber`](crate::KafkaSubscriber), minted by
/// [`Seekable::seeker`](ruststream::Seekable::seeker).
///
/// Cheap to clone and usable while the subscription's stream runs, which is the point: the
/// runtime owns the subscriber, so a handler reaches its subscription through an injected
/// `Seek(seeker)` parameter or through a token minted at the mount site.
///
/// # Scope
///
/// A seek moves **this consumer instance**, over the partitions it currently holds. It is not a
/// group operation: it commits nothing, and other members of the group are unaffected.
///
/// # Rebalances discard a seek
///
/// The reposition lives in the assignment it was applied to. When a rebalance revokes those
/// partitions - a member joining or leaving, a session timeout, a topic-metadata change - the
/// seek goes with them: whoever gets the partitions next (this instance included) resumes from
/// the group's committed offsets. Repositioning is therefore an operational action on a running
/// consumer, not durable state; a position that must survive restarts belongs in the
/// subscription descriptor ([`StartOffset`](crate::StartOffset)) or in a `start_at(..)` clause,
/// which reapplies it on every startup.
#[derive(Clone)]
pub struct KafkaSeeker {
    consumer: Arc<StreamConsumer<TrackingContext>>,
    tracker: Arc<CommitTracker>,
}

impl std::fmt::Debug for KafkaSeeker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSeeker").finish_non_exhaustive()
    }
}

impl KafkaSeeker {
    pub(crate) const fn new(
        consumer: Arc<StreamConsumer<TrackingContext>>,
        tracker: Arc<CommitTracker>,
    ) -> Self {
        Self { consumer, tracker }
    }
}

impl Seeker for KafkaSeeker {
    type Position = KafkaPosition;
    type Error = KafkaError;

    /// Moves this consumer's assigned partitions to `to`; the next delivery comes from there.
    ///
    /// The offset bookkeeping moves with the read position: the tracked watermark of every
    /// repositioned partition is dropped, so a later commit cannot advance past a record the
    /// seek replayed but nobody handled, and an exactly-once window that was open when the seek
    /// landed aborts instead of committing offsets from the position it replaced.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] when the position names no partition this
    /// consumer holds (including a subscription whose assignment never arrived) and
    /// [`KafkaError::Consume`] when librdkafka rejects the reposition or the timestamp lookup
    /// fails.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the consumer repositioned, its
    /// bookkeeping already reset.
    async fn seek(&self, to: Self::Position) -> Result<(), Self::Error> {
        let consumer = Arc::clone(&self.consumer);
        let tracker = Arc::clone(&self.tracker);
        // Every librdkafka call on this path blocks (the assignment wait, the timestamp lookup,
        // the seek itself), so the whole reposition runs on the blocking pool.
        task::spawn_blocking(move || reposition(&consumer, &tracker, &to))
            .await
            .map_err(|err| KafkaError::Consume(Box::new(err)))?
    }
}

/// Resolves `to` against the current assignment, resets the bookkeeping of every partition it
/// names, and moves the consumer.
fn reposition(
    consumer: &StreamConsumer<TrackingContext>,
    tracker: &CommitTracker,
    to: &KafkaPosition,
) -> Result<(), KafkaError> {
    let assignment = await_assignment(consumer)?;
    let targets = resolve(consumer, &assignment, to)?;
    if targets.count() == 0 {
        return Err(KafkaError::InvalidOptions(format!(
            "{to:?} names no partition assigned to this consumer; a seek moves the partitions \
             this instance holds, and its assignment is {}",
            describe(&assignment),
        )));
    }

    // The bookkeeping is reset before the consumer moves: between the two, a delivery pulled
    // from the old position could otherwise settle into the new one and commit past records the
    // replay has not handled yet.
    for element in targets.elements() {
        tracker.reposition(element.topic(), element.partition());
    }
    clear_stored_offsets(consumer, &targets)?;

    let outcome = consumer
        .seek_partitions(targets, SEEK_TIMEOUT)
        .map_err(KafkaError::consume)?;
    for element in outcome.elements() {
        element.error().map_err(KafkaError::consume)?;
    }
    Ok(())
}

/// Clears librdkafka's own stored offsets for the repositioned partitions.
///
/// The crate's watermark is not the only offset bookkeeping in play: librdkafka keeps a stored
/// position per partition and auto-commit flushes it, periodically and once more when the
/// consumer closes. That store still holds the position from before the seek, so leaving it
/// would let a commit carry the group past everything the seek replayed - the acks that built
/// it describe a read position this subscription no longer has. A logical `Invalid` offset is
/// how librdkafka itself clears the store when a partition is revoked.
pub(crate) fn clear_stored_offsets(
    consumer: &StreamConsumer<TrackingContext>,
    targets: &TopicPartitionList,
) -> Result<(), KafkaError> {
    let mut cleared = TopicPartitionList::new();
    for element in targets.elements() {
        cleared
            .add_partition_offset(element.topic(), element.partition(), Offset::Invalid)
            .map_err(KafkaError::consume)?;
    }
    match consumer.store_offsets(&cleared) {
        Ok(()) => Ok(()),
        // `Commit::Auto` leaves librdkafka's own offset store enabled, which is exactly the
        // mode where it refuses application stores - and where it owns the position anyway, so
        // there is nothing of ours to clear.
        Err(err) if err.rdkafka_error_code() == Some(RDKafkaErrorCode::InvalidArgument) => Ok(()),
        Err(err) => Err(KafkaError::consume(err)),
    }
}

/// The partitions this consumer holds, waiting out the group assignment when a seek runs before
/// the first fetch (the `start_at(..)` clause does).
fn await_assignment(
    consumer: &StreamConsumer<TrackingContext>,
) -> Result<TopicPartitionList, KafkaError> {
    let deadline = std::time::Instant::now() + ASSIGNMENT_TIMEOUT;
    loop {
        let assignment = consumer.assignment().map_err(KafkaError::consume)?;
        if assignment.count() > 0 || std::time::Instant::now() >= deadline {
            return Ok(assignment);
        }
        // The group assigns partitions asynchronously after subscribe; librdkafka's background
        // poll drives it, so waiting here does not deadlock against the message stream.
        std::thread::sleep(ASSIGNMENT_POLL);
    }
}

/// Builds the seek list: which assigned partitions move, and to which offsets.
fn resolve(
    consumer: &StreamConsumer<TrackingContext>,
    assignment: &TopicPartitionList,
    to: &KafkaPosition,
) -> Result<TopicPartitionList, KafkaError> {
    let mut targets = TopicPartitionList::new();
    match to {
        // The log ends are resolved here rather than handed to librdkafka as logical offsets:
        // a logical end resolves whenever the fetcher next asks the broker, which would put
        // records published in the meantime on the wrong side of the reposition. Asking now
        // pins "the end of the log" to the moment the seek ran.
        KafkaPosition::Earliest | KafkaPosition::Latest => {
            for element in assignment.elements() {
                let (low, high) = consumer
                    .fetch_watermarks(element.topic(), element.partition(), SEEK_TIMEOUT)
                    .map_err(KafkaError::consume)?;
                let offset = if matches!(to, KafkaPosition::Earliest) {
                    low
                } else {
                    high
                };
                targets
                    .add_partition_offset(
                        element.topic(),
                        element.partition(),
                        Offset::Offset(offset),
                    )
                    .map_err(KafkaError::consume)?;
            }
        }
        KafkaPosition::Timestamp(when_millis) => {
            let mut query = TopicPartitionList::new();
            for element in assignment.elements() {
                query
                    .add_partition_offset(
                        element.topic(),
                        element.partition(),
                        Offset::Offset(*when_millis),
                    )
                    .map_err(KafkaError::consume)?;
            }
            let resolved = consumer
                .offsets_for_times(query, SEEK_TIMEOUT)
                .map_err(KafkaError::consume)?;
            for element in resolved.elements() {
                // A partition with no record at or after the timestamp answers with no usable
                // offset; the log end is the honest resume point (everything older is before
                // the requested time).
                let offset = match element.offset() {
                    Offset::Offset(offset) if offset >= 0 => Offset::Offset(offset),
                    _ => Offset::End,
                };
                targets
                    .add_partition_offset(element.topic(), element.partition(), offset)
                    .map_err(KafkaError::consume)?;
            }
        }
        KafkaPosition::Offset {
            topic,
            partition,
            offset,
        } => {
            for element in assignment.elements() {
                let named = topic.as_ref().is_none_or(|name| name == element.topic());
                if named && element.partition() == *partition {
                    targets
                        .add_partition_offset(
                            element.topic(),
                            element.partition(),
                            Offset::Offset(*offset),
                        )
                        .map_err(KafkaError::consume)?;
                }
            }
        }
    }
    Ok(targets)
}

/// A human-readable assignment for the "nothing to seek" error.
fn describe(assignment: &TopicPartitionList) -> String {
    if assignment.count() == 0 {
        return "empty".to_owned();
    }
    assignment
        .elements()
        .iter()
        .map(|element| format!("{}[{}]", element.topic(), element.partition()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_build_the_variants_the_seeker_matches_on() {
        assert_eq!(KafkaPosition::earliest(), KafkaPosition::Earliest);
        assert_eq!(KafkaPosition::latest(), KafkaPosition::Latest);
        assert_eq!(KafkaPosition::timestamp(42), KafkaPosition::Timestamp(42));
        assert_eq!(
            KafkaPosition::offset(3, 1_024),
            KafkaPosition::Offset {
                topic: None,
                partition: 3,
                offset: 1_024,
            },
        );
        assert_eq!(
            KafkaPosition::topic_offset("orders", 3, 1_024),
            KafkaPosition::Offset {
                topic: Some("orders".to_owned()),
                partition: 3,
                offset: 1_024,
            },
        );
    }
}
