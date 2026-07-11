//! The subscriber: a stream of Kafka deliveries from one topic subscription.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use rdkafka::Message as _;
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::RDKafkaErrorCode;
use ruststream::Subscriber;
use tracing::{debug, warn};

use crate::convert;
use crate::error::KafkaError;
use crate::message::{KafkaMessage, Settlement};
use crate::topic::Commit;
use crate::tracker::{CommitTracker, TrackingContext};

/// Whether librdkafka is already retrying this error by itself, making a stream error item
/// noise rather than signal. The set is deliberately small and explicit; when in doubt, the
/// error is forwarded.
fn is_transient(err: &rdkafka::error::KafkaError) -> bool {
    // A subscribed topic that does not exist (yet): pending creation is routine when the
    // broker auto-creates topics, and librdkafka keeps refreshing metadata until it appears.
    err.rdkafka_error_code() == Some(RDKafkaErrorCode::UnknownTopicOrPartition)
}

/// A consumer-group member on one topic, yielding [`KafkaMessage`] deliveries.
///
/// Created by subscribing a [`KafkaTopic`](crate::KafkaTopic) descriptor (or a bare topic name)
/// through [`KafkaBroker`](crate::KafkaBroker). The subscriber owns a dedicated librdkafka
/// consumer; dropping it closes the consumer, which leaves the group and (under auto-commit)
/// commits the final stored position. Under `Commit::Tracked` each in-flight delivery keeps
/// the consumer alive, so the close happens once the last outstanding message settles or
/// drops - do not rely on subscriber drop as an immediate group-departure barrier.
///
/// Back-pressure: polling the stream is what drives the consumer, so consuming slower simply
/// fetches slower; librdkafka's own fetch queue bounds (`queued.max.messages.kbytes` and
/// friends, settable through [`KafkaTopic::config`](crate::KafkaTopic::config)) cap local
/// buffering.
pub struct KafkaSubscriber {
    consumer: Arc<StreamConsumer<TrackingContext>>,
    topic: String,
    commit: Commit,
    tracker: Arc<CommitTracker>,
    /// Whether the subscriber is inside an episode of transient consume errors; the first
    /// error of an episode warns, repeats are debug, recovery closes the episode.
    in_transient_episode: bool,
}

impl KafkaSubscriber {
    pub(crate) fn new(
        consumer: Arc<StreamConsumer<TrackingContext>>,
        topic: String,
        commit: Commit,
        tracker: Arc<CommitTracker>,
    ) -> Self {
        Self {
            consumer,
            topic,
            commit,
            tracker,
            in_transient_episode: false,
        }
    }

    /// Logs a transient consume error: one warning when the episode starts (the signal a
    /// human acts on in monitoring), debug for the repeats.
    fn note_transient(&mut self, err: &rdkafka::error::KafkaError) {
        if self.in_transient_episode {
            debug!(
                target: "ruststream_rdkafka",
                topic = %self.topic,
                error = %err,
                "transient consume error (repeat)",
            );
        } else {
            self.in_transient_episode = true;
            warn!(
                target: "ruststream_rdkafka",
                topic = %self.topic,
                error = %err,
                "transient consume error; librdkafka keeps retrying",
            );
        }
    }

    /// Closes a transient-error episode once deliveries flow again.
    fn note_recovered(&mut self) {
        if self.in_transient_episode {
            self.in_transient_episode = false;
            debug!(
                target: "ruststream_rdkafka",
                topic = %self.topic,
                "recovered from transient consume errors",
            );
        }
    }

    /// The topic this subscriber consumes.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    fn map_delivery(&self, delivery: &rdkafka::message::BorrowedMessage<'_>) -> KafkaMessage {
        let headers = convert::headers_from_message(delivery);
        let payload = delivery
            .payload()
            .map_or_else(Bytes::new, Bytes::copy_from_slice);
        let settlement = match self.commit {
            Commit::Auto => Settlement::Advisory,
            Commit::Tracked => {
                self.tracker
                    .delivered(delivery.topic(), delivery.partition(), delivery.offset());
                Settlement::Tracked {
                    consumer: Arc::clone(&self.consumer),
                    tracker: Arc::clone(&self.tracker),
                }
            }
        };
        KafkaMessage::new(
            payload,
            headers,
            delivery.topic().to_owned(),
            delivery.partition(),
            delivery.offset(),
            delivery.timestamp().to_millis(),
            settlement,
        )
    }
}

impl fmt::Debug for KafkaSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaSubscriber")
            .field("topic", &self.topic)
            .field("commit", &self.commit)
            .finish_non_exhaustive()
    }
}

impl Subscriber for KafkaSubscriber {
    type Message = KafkaMessage;
    type Error = KafkaError;

    /// Streams deliveries as they arrive; the stream yields an error item when the consumer
    /// fails (it does not end on its own - drop the subscriber to leave the group).
    ///
    /// Errors librdkafka is already retrying by itself are not forwarded as stream items:
    /// today that is exactly `UnknownTopicOrPartition` (a subscribed topic pending creation).
    /// Such an episode surfaces as one warning when it starts - the monitoring signal to act
    /// on - with debug lines for the repeats and for the recovery, so a topic that appears
    /// late (broker auto-creation, provisioning races) recovers without flooding the dispatch
    /// error log, while a topic that never appears leaves the warning standing. Everything
    /// else is forwarded.
    ///
    /// # Cancel safety
    ///
    /// Polling is cancel safe (the underlying `recv` is documented cancellation safe, so no
    /// delivery is lost by dropping the stream between polls), and the stream can be re-created
    /// by calling `stream` again: deliveries buffer in the consumer, not in the returned stream.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        futures::stream::unfold(self, |sub| async move {
            loop {
                match sub.consumer.recv().await {
                    Ok(delivery) => {
                        let item = sub.map_delivery(&delivery);
                        drop(delivery);
                        sub.note_recovered();
                        return Some((Ok(item), sub));
                    }
                    Err(err) if is_transient(&err) => sub.note_transient(&err),
                    Err(err) => return Some((Err(KafkaError::consume(err)), sub)),
                }
            }
        })
    }
}
