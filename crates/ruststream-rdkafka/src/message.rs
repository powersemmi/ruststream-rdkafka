//! The delivery type yielded by [`KafkaSubscriber`](crate::KafkaSubscriber).

use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use ruststream::{AckError, Headers, IncomingMessage, Partitioned, Positioned};

use crate::retry::{
    DLQ_SOURCE_OFFSET_HEADER, DLQ_SOURCE_PARTITION_HEADER, DLQ_SOURCE_TOPIC_HEADER,
    RETRY_COUNT_HEADER, Retry, RetryContext,
};
use crate::seek::KafkaPosition;
use crate::tracker::{CommitTracker, TrackingContext};

/// Header carrying a message's partition key, mapped onto Kafka's native record key.
///
/// On publish, this header becomes the record key (so Kafka itself routes deliveries that share
/// a key to the same partition) and is not duplicated as a wire header. On consume, the header
/// always mirrors the native record key - a same-named wire header from a foreign producer is
/// not preserved, because the record key is Kafka's source of truth for partitioning. Keyed
/// worker lanes (`workers(n, by_key)`) read it through
/// [`IncomingMessage::partition_key`]; [`Partitioned`] mirrors it as the capability surface.
pub const PARTITION_KEY_HEADER: &str = "kafka-partition-key";

/// Header naming the explicit destination partition for a publish (an ASCII decimal).
///
/// When present, the publisher targets that exact partition (winning over the partitioner and
/// the record key) and strips the header from the wire. An unparsable value fails the publish
/// with a clear error instead of silently falling back to the partitioner.
pub const PARTITION_HEADER: &str = "kafka-partition";

/// How this delivery settles when acked.
///
/// The tracked forms carry the generation the delivery was pulled in, so a settle that arrives
/// after the subscription was repositioned (a seek, a rebalance) is dropped instead of moving a
/// position that no longer describes what this consumer reads.
pub(crate) enum Settlement {
    /// `Commit::Auto`: librdkafka owns the committed position; `ack`/`nack` are advisory.
    Advisory,
    /// `Commit::Tracked`: an ack advances the shared watermark and stores the new position.
    Tracked {
        consumer: Arc<StreamConsumer<TrackingContext>>,
        tracker: Arc<CommitTracker>,
        generation: u64,
    },
    /// `Commit::Transactional`: an ack advances the shared watermark only - the EOS pipeline
    /// commits positions through the producer transaction, so nothing is stored here.
    Transactional {
        tracker: Arc<CommitTracker>,
        generation: u64,
    },
}

/// One Kafka delivery: an owned snapshot of the record plus its settlement handle.
///
/// Settlement mapping depends on the [`Commit`](crate::Commit) mode of the subscription:
///
/// Under `Commit::Auto` (the default) librdkafka owns the committed position - it is stored
/// the moment a message is handed to the application - so `ack` and both `nack` forms are
/// advisory no-ops; in particular `nack(true)` does NOT cause a redelivery.
///
/// Under `Commit::Tracked`:
///
/// - [`ack`](IncomingMessage::ack) settles the offset and advances the stored position across
///   everything settled below it.
/// - [`nack(false)`](IncomingMessage::nack) drops the message: the offset settles so the
///   position can move past it (Kafka has no per-message dead-letter path; a dead-letter topic
///   is a planned descriptor option).
/// - [`nack(true)`](IncomingMessage::nack) leaves the offset unsettled: the committed position
///   stays below it, so Kafka redelivers from there when the partition is next re-fetched (a
///   rebalance or a restart). Until then the unsettled offset also blocks the position,
///   keeping every later ack uncommitted - precise, but worth knowing when a handler nacks in
///   a loop.
///
/// Wire headers map name for name; a null-valued Kafka header arrives with an empty value
/// (presence preserved).
#[derive(Debug)]
pub struct KafkaMessage {
    payload: Bytes,
    headers: Headers,
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_millis: Option<i64>,
    settlement: Settlement,
    /// The keyed-lane key: the source partition (the default), or the record key under
    /// `LaneKey::RecordKey`.
    lane: Option<Bytes>,
    retry: Option<Arc<RetryContext>>,
}

impl fmt::Debug for Settlement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Advisory => f.write_str("Advisory"),
            Self::Tracked { .. } => f.debug_struct("Tracked").finish_non_exhaustive(),
            Self::Transactional { .. } => f.debug_struct("Transactional").finish_non_exhaustive(),
        }
    }
}

impl KafkaMessage {
    // An internal constructor mirroring the record's natural fields; grouping them into
    // intermediate structs would only add indirection for the one caller.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        payload: Bytes,
        headers: Headers,
        topic: String,
        partition: i32,
        offset: i64,
        timestamp_millis: Option<i64>,
        settlement: Settlement,
        lane: Option<Bytes>,
        retry: Option<Arc<RetryContext>>,
    ) -> Self {
        Self {
            payload,
            headers,
            topic,
            partition,
            offset,
            timestamp_millis,
            settlement,
            lane,
            retry,
        }
    }

    /// The topic this record was consumed from.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The partition this record was consumed from.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// The record's offset within its partition.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// The record's timestamp in milliseconds since the epoch, when the broker provided one.
    #[must_use]
    pub fn timestamp_millis(&self) -> Option<i64> {
        self.timestamp_millis
    }

    /// The record key, surfaced from Kafka's native key (see [`PARTITION_KEY_HEADER`]).
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }

    /// Replaces the payload with its registry-transcoded form (the subscriber's async
    /// middleware), before the delivery is handed on.
    #[cfg(feature = "schema-registry")]
    pub(crate) fn replace_payload(&mut self, payload: Bytes) {
        self.payload = payload;
    }

    fn settle(self) -> Result<(), AckError> {
        match self.settlement {
            Settlement::Advisory => Ok(()),
            Settlement::Tracked {
                consumer,
                tracker,
                generation,
            } => tracker
                .settle_with(
                    &self.topic,
                    self.partition,
                    self.offset,
                    generation,
                    |position| consumer.store_offset(&self.topic, self.partition, position),
                )
                .map_err(|err| AckError::Broker(Box::new(err))),
            Settlement::Transactional {
                tracker,
                generation,
            } => {
                let infallible: Result<(), Infallible> = tracker.settle_with(
                    &self.topic,
                    self.partition,
                    self.offset,
                    generation,
                    |_position| Ok(()),
                );
                infallible.expect("no-op store cannot fail");
                Ok(())
            }
        }
    }

    /// The number of retry republishes already behind this delivery, from
    /// [`RETRY_COUNT_HEADER`]; the original publish carries none.
    fn retry_attempts(&self) -> u32 {
        self.headers
            .get_str(RETRY_COUNT_HEADER)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }

    /// The retry path for `nack(true)` when a policy is configured.
    async fn retry_requeue(self, retry: Arc<RetryContext>) -> Result<(), AckError> {
        match retry.policy() {
            Some(Retry::Topic(topic)) => {
                let next_delivery = self.retry_attempts() + 2;
                if retry.over_cap(next_delivery) {
                    return self.drop_path(&retry).await;
                }
                let mut headers = self.headers.clone();
                headers.insert(RETRY_COUNT_HEADER, (self.retry_attempts() + 1).to_string());
                retry
                    .republish(topic, &self.payload, &headers)
                    .await
                    .map_err(|err| AckError::Broker(Box::new(err)))?;
                self.settle()
            }
            Some(Retry::SeekBack) => {
                let next_delivery =
                    retry.next_seek_delivery(&self.topic, self.partition, self.offset);
                if retry.over_cap(next_delivery) {
                    retry.forget_seeks(&self.topic, self.partition, self.offset);
                    return self.drop_path(&retry).await;
                }
                retry.record_seek(&self.topic, self.partition, self.offset);
                retry
                    .seek_back(&self.topic, self.partition, self.offset)
                    .map_err(|err| AckError::Broker(Box::new(err)))
                // Deliberately NOT settled: the seeked redelivery replays this offset, and
                // under Tracked the replay resets the partition's watermark state.
            }
            Some(Retry::Drop) | None => self.drop_path(&retry).await,
        }
    }

    /// The drop path: dead-letter when configured, then settle.
    async fn drop_path(self, retry: &RetryContext) -> Result<(), AckError> {
        if let Some(dlq) = retry.dead_letter() {
            let mut headers = self.headers.clone();
            headers.insert(DLQ_SOURCE_TOPIC_HEADER, self.topic.clone());
            headers.insert(DLQ_SOURCE_PARTITION_HEADER, self.partition.to_string());
            headers.insert(DLQ_SOURCE_OFFSET_HEADER, self.offset.to_string());
            retry
                .republish(dlq, &self.payload, &headers)
                .await
                .map_err(|err| AckError::Broker(Box::new(err)))?;
        }
        self.settle()
    }
}

impl IncomingMessage for KafkaMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &Headers {
        &self.headers
    }

    /// Marks the offset processed (see the type-level settlement mapping).
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] when the offset store rejects the new position, for example
    /// because `enable.auto.offset.store` was overridden back to `true` on a `Commit::Tracked`
    /// subscription.
    ///
    /// # Cancel safety
    ///
    /// Cancel safe: the watermark update is synchronous, so the future either completed or did
    /// nothing.
    async fn ack(self) -> Result<(), AckError> {
        self.settle()
    }

    /// Settles negatively. With a [`Retry`] policy configured on the subscription,
    /// `requeue = true` runs it (republish to the retry topic, seek back, or drop) and
    /// `requeue = false` runs the drop path (dead-letter when configured, then settle).
    /// Without a policy, `requeue = false` settles the offset and `requeue = true` leaves it
    /// unsettled for Kafka's native re-consumption - which under `Commit::Auto` makes both
    /// forms advisory no-ops (see the type-level settlement mapping).
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] when a retry/dead-letter republish or seek fails, and
    /// under the same conditions as [`ack`](Self::ack).
    ///
    /// # Cancel safety
    ///
    /// Without a policy: cancel safe (the watermark update is synchronous). With a policy: not
    /// cancel safe - dropping the future may leave the retry or dead-letter copy published
    /// with the original unsettled (a duplicate, never a loss).
    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        match (self.retry.clone(), requeue) {
            (Some(retry), true) => self.retry_requeue(retry).await,
            (Some(retry), false) => self.drop_path(&retry).await,
            // Leaving the offset unsettled is the whole mechanism: under Tracked the committed
            // position stays below it, so Kafka redelivers from there on the next fetch of
            // this partition.
            (None, true) => Ok(()),
            (None, false) => self.settle(),
        }
    }

    /// The keyed-lane key, so keyed worker lanes see it without a `Partitioned` bound: the
    /// source partition (the default), or the record key under
    /// [`LaneKey::RecordKey`](crate::LaneKey::RecordKey).
    fn partition_key(&self) -> Option<&[u8]> {
        self.lane.as_deref()
    }
}

impl Positioned for KafkaMessage {
    type Position = KafkaPosition;

    /// This delivery's own coordinates: seeking to them redelivers exactly this record (and the
    /// ordered suffix behind it on the partition).
    fn position(&self) -> Self::Position {
        KafkaPosition::topic_offset(&self.topic, self.partition, self.offset)
    }
}

impl Partitioned for KafkaMessage {
    /// The keyed-lane key (see [`IncomingMessage::partition_key`] on this type): the source
    /// partition (the default), or the record key under
    /// [`LaneKey::RecordKey`](crate::LaneKey::RecordKey).
    fn partition_key(&self) -> Option<&[u8]> {
        self.lane.as_deref()
    }
}
