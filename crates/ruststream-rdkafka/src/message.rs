//! The delivery type yielded by [`KafkaSubscriber`](crate::KafkaSubscriber).

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use ruststream::{AckError, Headers, IncomingMessage, Partitioned};

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

/// How this delivery settles when acked.
pub(crate) enum Settlement {
    /// `Commit::Auto`: librdkafka owns the committed position; `ack`/`nack` are advisory.
    Advisory,
    /// `Commit::Tracked`: an ack advances the shared watermark and stores the new position.
    Tracked {
        consumer: Arc<StreamConsumer<TrackingContext>>,
        tracker: Arc<CommitTracker>,
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
}

impl fmt::Debug for Settlement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Advisory => f.write_str("Advisory"),
            Self::Tracked { .. } => f.debug_struct("Tracked").finish_non_exhaustive(),
        }
    }
}

impl KafkaMessage {
    pub(crate) fn new(
        payload: Bytes,
        headers: Headers,
        topic: String,
        partition: i32,
        offset: i64,
        timestamp_millis: Option<i64>,
        settlement: Settlement,
    ) -> Self {
        Self {
            payload,
            headers,
            topic,
            partition,
            offset,
            timestamp_millis,
            settlement,
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

    fn settle(self) -> Result<(), AckError> {
        match self.settlement {
            Settlement::Advisory => Ok(()),
            Settlement::Tracked { consumer, tracker } => tracker
                .settle_with(&self.topic, self.partition, self.offset, |position| {
                    consumer.store_offset(&self.topic, self.partition, position)
                })
                .map_err(|err| AckError::Broker(Box::new(err))),
        }
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

    /// Settles negatively: drops the offset (`requeue = false`) or leaves it unsettled for
    /// Kafka's native re-consumption (`requeue = true`). Only meaningful under
    /// `Commit::Tracked`; under `Commit::Auto` both forms are advisory no-ops (see the
    /// type-level settlement mapping).
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] under the same conditions as [`ack`](Self::ack).
    ///
    /// # Cancel safety
    ///
    /// Cancel safe: the watermark update is synchronous, so the future either completed or did
    /// nothing.
    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        if requeue {
            // Leaving the offset unsettled is the whole mechanism: under Tracked the committed
            // position stays below it, so Kafka redelivers from there on the next fetch of
            // this partition.
            return Ok(());
        }
        self.settle()
    }

    /// The record key, so keyed worker lanes see it without a `Partitioned` bound.
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }
}

impl Partitioned for KafkaMessage {
    /// The record key Kafka partitioned this message by, or `None` for keyless records.
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }
}
