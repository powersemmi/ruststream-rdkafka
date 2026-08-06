//! Per-subscription retry and dead-letter policies for negative acknowledgement.
//!
//! Kafka has no per-message negative acknowledgement, so `nack(true)` natively means "leave
//! the offset unsettled and re-consume later". These policies give it an immediate meaning:
//! republish to a retry topic (with an attempt counter riding in a header), seek the partition
//! back to re-consume in place, or drop - with an optional dead-letter topic on the drop path
//! and a poison cap on the number of deliveries. Everything here is off by default; without a
//! policy, settlement behaves exactly as the commit mode describes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};
use ruststream::Headers;

use crate::broker::ConnState;
use crate::convert;
use crate::error::KafkaError;
use crate::tracker::{CommitTracker, TrackingContext};

/// Header carrying the number of retry republishes a message has been through.
///
/// Set by [`Retry::Topic`] on every republish (an ASCII decimal), read back to enforce
/// [`KafkaTopic::max_deliveries`](crate::KafkaTopic::max_deliveries) across hops. A message on
/// its retry topic with the value `2` is on its third delivery.
pub const RETRY_COUNT_HEADER: &str = "kafka-retry-count";

/// Headers stamped onto a dead-lettered message with the origin of the failed delivery.
///
/// `kafka-dlq-source-topic`, `kafka-dlq-source-partition`, and `kafka-dlq-source-offset` name
/// the topic, partition, and offset the message failed on, so a dead-letter consumer can trace
/// it back without parsing payloads.
pub const DLQ_SOURCE_TOPIC_HEADER: &str = "kafka-dlq-source-topic";
/// See [`DLQ_SOURCE_TOPIC_HEADER`].
pub const DLQ_SOURCE_PARTITION_HEADER: &str = "kafka-dlq-source-partition";
/// See [`DLQ_SOURCE_TOPIC_HEADER`].
pub const DLQ_SOURCE_OFFSET_HEADER: &str = "kafka-dlq-source-offset";

/// What `nack(true)` does on this subscription (see
/// [`KafkaTopic::retry`](crate::KafkaTopic::retry)).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Retry {
    /// Republish the message to this topic (stamping [`RETRY_COUNT_HEADER`]), then settle the
    /// original offset. Republish-first ordering makes a crash between the two steps a
    /// duplicate, never a loss.
    Topic(String),
    /// Seek the partition back to the message's offset and re-consume it in place. Redelivery
    /// is immediate, but everything after the offset on that partition replays too
    /// (at-least-once duplicates), and the delivery count only survives within the session.
    SeekBack,
    /// Treat `nack(true)` like the drop path (dead-letter when configured, settle otherwise).
    Drop,
}

/// The resolved retry/dead-letter wiring one subscription hands to its deliveries.
pub(crate) struct RetryContext {
    policy: Option<Retry>,
    max_deliveries: Option<u32>,
    dead_letter: Option<String>,
    state: Arc<ConnState>,
    consumer: Arc<StreamConsumer<TrackingContext>>,
    tracker: Arc<CommitTracker>,
    /// In-session delivery counts for `SeekBack`, keyed by the seeked offset. Entries are
    /// removed when the offset resolves to the drop path; a poison offset therefore holds at
    /// most one entry at a time.
    seeks: Mutex<HashMap<(String, i32, i64), u32>>,
}

impl std::fmt::Debug for RetryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryContext")
            .field("policy", &self.policy)
            .field("max_deliveries", &self.max_deliveries)
            .field("dead_letter", &self.dead_letter)
            .finish_non_exhaustive()
    }
}

impl RetryContext {
    pub(crate) fn new(
        policy: Option<Retry>,
        max_deliveries: Option<u32>,
        dead_letter: Option<String>,
        state: Arc<ConnState>,
        consumer: Arc<StreamConsumer<TrackingContext>>,
        tracker: Arc<CommitTracker>,
    ) -> Self {
        Self {
            policy,
            max_deliveries,
            dead_letter,
            state,
            consumer,
            tracker,
            seeks: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn policy(&self) -> Option<&Retry> {
        self.policy.as_ref()
    }

    pub(crate) fn dead_letter(&self) -> Option<&str> {
        self.dead_letter.as_deref()
    }

    /// Whether delivery number `delivery` (1-based) exceeds the poison cap.
    pub(crate) fn over_cap(&self, delivery: u32) -> bool {
        self.max_deliveries.is_some_and(|cap| delivery > cap)
    }

    /// Counts a seek-back redelivery of `offset` and returns the delivery number it will be
    /// (the original delivery is number one).
    pub(crate) fn next_seek_delivery(&self, topic: &str, partition: i32, offset: i64) -> u32 {
        let mut seeks = self.seeks.lock().expect("seek counter mutex poisoned");
        let seeks_done = *seeks
            .entry((topic.to_owned(), partition, offset))
            .or_insert(0);
        drop(seeks);
        seeks_done + 2
    }

    pub(crate) fn record_seek(&self, topic: &str, partition: i32, offset: i64) {
        let mut seeks = self.seeks.lock().expect("seek counter mutex poisoned");
        *seeks
            .entry((topic.to_owned(), partition, offset))
            .or_insert(0) += 1;
    }

    pub(crate) fn forget_seeks(&self, topic: &str, partition: i32, offset: i64) {
        let mut seeks = self.seeks.lock().expect("seek counter mutex poisoned");
        seeks.remove(&(topic.to_owned(), partition, offset));
    }

    /// Seeks the partition back so `offset` (and everything after it) re-consumes.
    pub(crate) fn seek_back(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), KafkaError> {
        // The same resets the explicit seeker performs, for the same reason: deliveries already
        // in flight from beyond this offset belong to the read position being replaced, so
        // neither their settles nor librdkafka's own stored position may commit past the
        // records this rewind replays.
        self.tracker.reposition(topic, partition);
        let mut rewound = TopicPartitionList::new();
        rewound
            .add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(KafkaError::consume)?;
        crate::seek::clear_stored_offsets(&self.consumer, &rewound)?;
        self.consumer
            .seek(
                topic,
                partition,
                Offset::Offset(offset),
                Duration::from_secs(5),
            )
            .map_err(KafkaError::consume)
    }

    /// Republishes `payload` with `headers` to `topic` and awaits the delivery report, so the
    /// caller settles the original only after the copy is durably accepted.
    pub(crate) async fn republish(
        &self,
        topic: &str,
        payload: &[u8],
        headers: &Headers,
    ) -> Result<(), KafkaError> {
        self.state.ensure_open(topic)?;
        let parts = convert::headers_for_publish(headers)?;
        let mut record = FutureRecord::<[u8], [u8]>::to(topic).payload(payload);
        if let Some(key) = &parts.key {
            record = record.key(key.as_ref());
        }
        if let Some(partition) = parts.partition {
            record = record.partition(partition);
        }
        if let Some(native) = parts.headers {
            record = record.headers(native);
        }
        self.state
            .producer()
            .send(record, Timeout::Never)
            .await
            .map(|_delivery| ())
            .map_err(|(err, _record)| KafkaError::publish(err))
    }
}
