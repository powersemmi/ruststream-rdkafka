//! The subscriber: a stream of Kafka deliveries from one topic subscription.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use rdkafka::Message as _;
use rdkafka::consumer::StreamConsumer;
use ruststream::Subscriber;

use crate::convert;
use crate::error::KafkaError;
use crate::message::{KafkaMessage, Settlement};
use crate::topic::Commit;
use crate::tracker::{CommitTracker, TrackingContext};

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
    /// # Cancel safety
    ///
    /// Polling is cancel safe (the underlying `recv` is documented cancellation safe, so no
    /// delivery is lost by dropping the stream between polls), and the stream can be re-created
    /// by calling `stream` again: deliveries buffer in the consumer, not in the returned stream.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        futures::stream::unfold(self, |sub| async move {
            let item = match sub.consumer.recv().await {
                Ok(delivery) => Ok(sub.map_delivery(&delivery)),
                Err(err) => Err(KafkaError::consume(err)),
            };
            Some((item, sub))
        })
    }
}
