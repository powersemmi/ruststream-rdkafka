//! The subscriber: a stream of Kafka deliveries from one topic subscription.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use futures::future::FutureExt as _;
use rdkafka::Message as _;
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::RDKafkaErrorCode;
#[cfg(feature = "schema-registry")]
use ruststream::IncomingMessage;
use ruststream::{BatchSubscriber, Seekable, Subscriber};
use tracing::{debug, warn};

use crate::convert;
use crate::eos::EOS_SOURCE_HEADER;
use crate::error::KafkaError;
use crate::message::{KafkaMessage, PARTITION_KEY_HEADER, Settlement};
use crate::retry::RetryContext;
use crate::seek::KafkaSeeker;
use crate::topic::{Commit, LaneKey};
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
    lane_key: LaneKey,
    retry: Option<Arc<RetryContext>>,
    /// Minted once, when the subscription opens: every delivery carries a clone so a handler's
    /// context can hand out the reposition handle for one reference-count bump.
    seeker: Arc<KafkaSeeker>,
    #[cfg(feature = "schema-registry")]
    schema_registry: Option<crate::schema_registry::SchemaRegistry>,
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
        lane_key: LaneKey,
        retry: Option<Arc<RetryContext>>,
    ) -> Self {
        let seeker = Arc::new(KafkaSeeker::new(
            Arc::clone(&consumer),
            Arc::clone(&tracker),
        ));
        Self {
            consumer,
            topic,
            commit,
            tracker,
            lane_key,
            retry,
            seeker,
            #[cfg(feature = "schema-registry")]
            schema_registry: None,
            in_transient_episode: false,
        }
    }

    #[cfg(feature = "schema-registry")]
    pub(crate) fn with_schema_registry(
        mut self,
        registry: Option<crate::schema_registry::SchemaRegistry>,
    ) -> Self {
        self.schema_registry = registry;
        self
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

    /// The registry middleware: transcodes a framed payload to plain JSON on the async
    /// consume path, so handlers and the default codec see JSON regardless of the wire
    /// format. A no-op without an attached registry or for non-framed payloads.
    #[cfg(feature = "schema-registry")]
    async fn transcode(&self, item: &mut KafkaMessage) {
        if let Some(registry) = &self.schema_registry
            && let Some(json) = registry
                .incoming_to_json(IncomingMessage::payload(item))
                .await
        {
            item.replace_payload(Bytes::from(json));
        }
    }

    fn map_delivery(&self, delivery: &rdkafka::message::BorrowedMessage<'_>) -> KafkaMessage {
        let mut headers = convert::headers_from_message(delivery);
        if matches!(self.commit, Commit::Transactional(_)) {
            // The source coordinates ride the delivery's headers so the reply path can pair a
            // publishing handler's reply with its consumed offset (see EosPipeline::replies);
            // stripped from every outgoing publish, so they never hit the wire.
            headers.insert(
                EOS_SOURCE_HEADER,
                crate::eos::encode_source(
                    delivery.topic(),
                    delivery.partition(),
                    delivery.offset(),
                ),
            );
        }
        let payload = delivery
            .payload()
            .map_or_else(Bytes::new, Bytes::copy_from_slice);
        // The generation is captured here, where the delivery is pulled, never where it settles:
        // that is what lets a reposition landing in between tell a delivery of the replaced read
        // position apart from one the new position produced.
        let settlement = match &self.commit {
            Commit::Auto => Settlement::Advisory,
            Commit::Tracked => Settlement::Tracked {
                consumer: Arc::clone(&self.consumer),
                tracker: Arc::clone(&self.tracker),
                generation: self.tracker.delivered(
                    delivery.topic(),
                    delivery.partition(),
                    delivery.offset(),
                ),
            },
            Commit::Transactional(_) => Settlement::Transactional {
                tracker: Arc::clone(&self.tracker),
                generation: self.tracker.delivered(
                    delivery.topic(),
                    delivery.partition(),
                    delivery.offset(),
                ),
            },
        };
        let lane = match self.lane_key {
            LaneKey::RecordKey => headers
                .get(PARTITION_KEY_HEADER)
                .map(Bytes::copy_from_slice),
            LaneKey::Partition => Some(Bytes::from(delivery.partition().to_string())),
        };
        KafkaMessage::new(
            payload,
            headers,
            delivery.topic().to_owned(),
            delivery.partition(),
            delivery.offset(),
            delivery.timestamp().to_millis(),
            settlement,
            lane,
            self.retry.clone(),
            Arc::clone(&self.seeker),
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
                        #[allow(unused_mut)] // mutated by the registry transcode only
                        let mut item = sub.map_delivery(&delivery);
                        drop(delivery);
                        #[cfg(feature = "schema-registry")]
                        sub.transcode(&mut item).await;
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

impl Seekable for KafkaSubscriber {
    type Seeker = KafkaSeeker;

    /// Hands out a handle for repositioning this subscription; see [`KafkaSeeker`] for the
    /// scope of a seek and what a rebalance does to it.
    fn seeker(&self) -> Self::Seeker {
        KafkaSeeker::clone(&self.seeker)
    }
}

impl BatchSubscriber for KafkaSubscriber {
    type Batch = Vec<KafkaMessage>;

    /// Streams non-empty pages natively: each waits for one delivery, then drains everything
    /// librdkafka has already fetched. There is no crate-imposed window - the page is bounded
    /// by librdkafka's own fetch-queue limits (`queued.max.messages.kbytes` and friends,
    /// settable through [`KafkaTopic::config`](crate::KafkaTopic::config)); wrap the source in
    /// the core [`Buffered`](ruststream::Buffered) adapter for an explicit size/deadline
    /// window. A consumer error inside an open page yields the page first; the error (if it
    /// persists) surfaces on the next poll.
    ///
    /// # Cancel safety
    ///
    /// Same guarantees as [`Subscriber::stream`]: cancel safe between polls, no delivery is
    /// lost by dropping the stream.
    fn batches(
        &mut self,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        futures::stream::unfold(self, |sub| async move {
            // Wait for the page's first delivery.
            let first = loop {
                match sub.consumer.recv().await {
                    Ok(delivery) => break sub.map_delivery(&delivery),
                    Err(err) if is_transient(&err) => sub.note_transient(&err),
                    Err(err) => return Some((Err(KafkaError::consume(err)), sub)),
                }
            };
            sub.note_recovered();

            let mut batch = vec![first];
            // Drain what is already fetched; recv is cancel safe, so dropping the probe
            // future loses nothing.
            while let Some(result) = sub.consumer.recv().now_or_never() {
                match result {
                    Ok(delivery) => {
                        let item = sub.map_delivery(&delivery);
                        batch.push(item);
                    }
                    Err(err) if is_transient(&err) => sub.note_transient(&err),
                    // Yield what was collected; a persistent error re-surfaces on the next
                    // page's first recv.
                    Err(_) => break,
                }
            }
            #[cfg(feature = "schema-registry")]
            for item in &mut batch {
                sub.transcode(item).await;
            }
            Some((Ok(batch), sub))
        })
    }
}
