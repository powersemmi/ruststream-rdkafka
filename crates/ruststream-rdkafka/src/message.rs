//! The delivery type yielded by [`KafkaSubscriber`](crate::KafkaSubscriber).

use std::convert::Infallible;
use std::fmt;
use std::future::{Future, ready};
use std::io::Write as _;
use std::sync::{Arc, OnceLock};

#[cfg(feature = "schema-registry")]
use bytes::Bytes;
use rdkafka::consumer::Consumer as _;
use ruststream::{AckError, HeaderMap, IncomingMessage, Partitioned, Positioned, Str};

use crate::convert;
use crate::record::HeldRecord;
use crate::seek::{KafkaPosition, KafkaSeeker};
use crate::tracker::CommitTracker;

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
///
/// The tracked forms carry the generation the delivery was pulled in, so a settle that arrives
/// after the subscription was repositioned (a seek, a rebalance) is dropped instead of moving a
/// position that no longer describes what this consumer reads.
pub(crate) enum Settlement {
    /// `Commit::Auto`: librdkafka owns the committed position; `ack`/`nack` are advisory.
    Advisory,
    /// `Commit::Tracked`: an ack advances the shared watermark and stores the new position,
    /// through the consumer the delivery's own record keeps alive.
    Tracked {
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

/// How a delivery answers [`IncomingMessage::partition_key`], which most deliveries are never
/// asked.
///
/// Both forms answer out of what the delivery already holds, so a subscription with no keyed
/// lanes - the default - pays nothing for a key nobody reads.
pub(crate) enum Lane {
    /// The record key, which the delivery carries as its [`PARTITION_KEY_HEADER`] header.
    RecordKey,
    /// The source partition, written into the delivery's own bytes on the first ask.
    Partition(OnceLock<PartitionText>),
}

impl Lane {
    /// This delivery's lane key.
    fn of<'a>(&'a self, msg: &'a KafkaMessage) -> Option<&'a [u8]> {
        match self {
            Self::RecordKey => msg.record.key(),
            Self::Partition(text) => Some(
                text.get_or_init(|| PartitionText::of(msg.partition))
                    .as_bytes(),
            ),
        }
    }
}

impl fmt::Debug for Lane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecordKey => f.write_str("RecordKey"),
            Self::Partition(_) => f.write_str("Partition"),
        }
    }
}

/// A partition number as the decimal text a lane key is, held inline: the widest `i32` is eleven
/// bytes, which is shorter than the pointer a heap copy of it would cost.
#[derive(Debug)]
pub(crate) struct PartitionText {
    bytes: [u8; 11],
    len: usize,
}

impl PartitionText {
    fn of(partition: i32) -> Self {
        let mut bytes = [0u8; 11];
        let mut cursor = &mut bytes[..];
        write!(cursor, "{partition}").expect("eleven bytes hold the widest i32");
        let len = 11 - cursor.len();
        Self { bytes, len }
    }

    fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
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
///   position can move past it.
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
    /// The record librdkafka fetched, kept where it put it: the body, the key and the wire
    /// headers are read out of the fetch buffer, and the consumer that owns that buffer stays
    /// open for as long as this delivery does.
    record: HeldRecord,
    /// The payload the registry middleware transcoded, when there is one. Without it the body
    /// is the record's own.
    #[cfg(feature = "schema-registry")]
    transcoded: Option<Bytes>,
    /// The `RustStream` view of the record's headers, built on the first read: a delivery
    /// nothing asks headers of never pays for them.
    headers: OnceLock<HeaderMap>,
    /// The topic, shared with the subscription that read it: a Kafka consumer reads a handful of
    /// topics and delivers millions of records, so the name is minted once per topic and every
    /// delivery of it takes a reference count.
    topic: Str,
    partition: i32,
    offset: i64,
    timestamp_millis: Option<i64>,
    settlement: Settlement,
    /// How this delivery answers `partition_key()`.
    lane: Lane,
    /// The subscription's own reposition handle, minted when it opened: this is what lets a
    /// per-delivery context be built from the delivery alone.
    seeker: Arc<KafkaSeeker>,
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
        record: HeldRecord,
        headers: OnceLock<HeaderMap>,
        topic: Str,
        partition: i32,
        offset: i64,
        timestamp_millis: Option<i64>,
        settlement: Settlement,
        lane: Lane,
        seeker: Arc<KafkaSeeker>,
    ) -> Self {
        Self {
            record,
            #[cfg(feature = "schema-registry")]
            transcoded: None,
            headers,
            topic,
            partition,
            offset,
            timestamp_millis,
            settlement,
            lane,
            seeker,
        }
    }

    /// The subscription's reposition handle, for the context built off this delivery.
    pub(crate) fn seeker_handle(&self) -> Arc<KafkaSeeker> {
        Arc::clone(&self.seeker)
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
        self.record.key()
    }

    /// Replaces the payload with its registry-transcoded form (the subscriber's async
    /// middleware), before the delivery is handed on.
    #[cfg(feature = "schema-registry")]
    pub(crate) fn replace_payload(&mut self, payload: Bytes) {
        self.transcoded = Some(payload);
    }

    fn settle(self) -> Result<(), AckError> {
        match &self.settlement {
            Settlement::Advisory => Ok(()),
            Settlement::Tracked {
                tracker,
                generation,
            } => tracker
                .settle_with(
                    &self.topic,
                    self.partition,
                    self.offset,
                    *generation,
                    |position| {
                        // The watermark is this delivery's own offset whenever nothing below it
                        // is still outstanding, which is every settle of an in-order handler.
                        // There the record answers for its own topic and librdkafka takes the
                        // position off the record's topic handle, instead of looking one up by
                        // name (a `CString`, a topic create and a topic destroy under its own
                        // lock, per message).
                        if let Some(record) = self.record.fetched()
                            && position == self.offset
                        {
                            self.record.consumer().store_offset_from_message(record)
                        } else {
                            self.record.consumer().store_offset(
                                &self.topic,
                                self.partition,
                                position,
                            )
                        }
                    },
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
                    *generation,
                    |_position| Ok(()),
                );
                infallible.expect("no-op store cannot fail");
                Ok(())
            }
        }
    }
}

impl IncomingMessage for KafkaMessage {
    fn payload(&self) -> &[u8] {
        #[cfg(feature = "schema-registry")]
        if let Some(transcoded) = &self.transcoded {
            return transcoded;
        }
        self.record.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.headers
            .get_or_init(|| convert::headers_from_message(&self.record))
    }

    /// Marks the offset processed (see the type-level settlement mapping).
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] when librdkafka refuses the stored position, which is a
    /// client-state failure rather than a network one - the partition is no longer this
    /// consumer's, say. A descriptor passthrough that would take the offset store back is not
    /// one of those cases and never reaches here: librdkafka would accept the store and keep
    /// owning the position, so [`Commit::Tracked`](crate::Commit::Tracked) and
    /// [`Commit::Transactional`](crate::Commit::Transactional) refuse such a subscription at
    /// startup instead.
    ///
    /// # Cancel safety
    ///
    /// Cancel safe: the watermark update is synchronous, so the future either completed or did
    /// nothing.
    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(self.settle())
    }

    /// Settles negatively. `requeue = false` drops the delivery: the offset settles so the
    /// committed position can move past it. `requeue = true` leaves the offset unsettled, which
    /// is Kafka's own redelivery - the committed position stays below it and the partition is
    /// re-consumed from there on its next fetch. Under `Commit::Auto` both forms are advisory
    /// no-ops (see the type-level settlement mapping).
    ///
    /// Kafka counts no deliveries of its own, so a registration that caps its attempts with
    /// `max_attempts(..)` gets the framework's count instead: the runtime republishes the
    /// delivery through the registration's retry publisher so the count travels with it.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] under the same conditions as [`ack`](Self::ack).
    ///
    /// # Cancel safety
    ///
    /// Cancel safe: the watermark update is synchronous, so the future either completed or did
    /// nothing.
    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        // Leaving the offset unsettled is the whole mechanism: under Tracked the committed
        // position stays below it, so Kafka redelivers from there on the next fetch of this
        // partition.
        ready(if requeue { Ok(()) } else { self.settle() })
    }

    /// The keyed-lane key, so keyed worker lanes see it without a `Partitioned` bound: the
    /// source partition (the default), or the record key under
    /// [`LaneKey::RecordKey`](crate::LaneKey::RecordKey).
    fn partition_key(&self) -> Option<&[u8]> {
        self.lane.of(self)
    }
}

impl Positioned for KafkaMessage {
    type Position = KafkaPosition;

    /// This delivery's own coordinates: seeking to them redelivers exactly this record (and the
    /// ordered suffix behind it on the partition).
    fn position(&self) -> Self::Position {
        KafkaPosition::topic_offset(&*self.topic, self.partition, self.offset)
    }
}

impl Partitioned for KafkaMessage {
    /// The keyed-lane key (see [`IncomingMessage::partition_key`] on this type): the source
    /// partition (the default), or the record key under
    /// [`LaneKey::RecordKey`](crate::LaneKey::RecordKey).
    fn partition_key(&self) -> Option<&[u8]> {
        self.lane.of(self)
    }
}
