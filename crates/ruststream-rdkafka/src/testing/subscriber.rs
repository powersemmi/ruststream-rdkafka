//! The in-process subscriber and its delivery type.

use std::fmt;
use std::future::{Future, ready};
use std::sync::Arc;

use futures::Stream;
use ruststream::testing::Coordinator;
use ruststream::{AckError, BatchSubscriber, Headers, IncomingMessage, Partitioned, Subscriber};

use super::broker::TestBrokerState;
use super::router::{DeliveryReceiver, DeliverySender, SubscriptionId, TestDelivery};
use crate::error::KafkaError;

/// In-process subscriber on one topic name.
///
/// Yielded messages settle like the routing contract expects: ack finalizes, `nack(true)`
/// re-enqueues to this same subscription, `nack(false)` drops. The real transport's
/// committed-position semantics (holes, watermarks, redelivery on rebalance) are deliberately
/// not simulated.
pub struct KafkaTestSubscriber {
    state: Arc<TestBrokerState>,
    ids: Vec<SubscriptionId>,
    topic: String,
    sender: DeliverySender,
    receiver: DeliveryReceiver,
    coordinator: Option<Coordinator>,
}

impl KafkaTestSubscriber {
    pub(crate) fn open_many(state: &Arc<TestBrokerState>, topics: &[String]) -> Self {
        let (ids, sender, receiver) = state.router.subscribe_many(topics);
        let coordinator = state.coordinator();
        Self {
            state: Arc::clone(state),
            ids,
            topic: topics.join(","),
            sender,
            receiver,
            coordinator,
        }
    }

    /// The subscribed topic name(s), joined with `,` when there are several.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl Drop for KafkaTestSubscriber {
    fn drop(&mut self) {
        for id in &self.ids {
            self.state.router.unsubscribe(*id);
        }
    }
}

impl fmt::Debug for KafkaTestSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaTestSubscriber")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

impl Subscriber for KafkaTestSubscriber {
    type Message = KafkaTestMessage;
    type Error = KafkaError;

    /// Streams injected deliveries; never yields an error.
    ///
    /// # Cancel safety
    ///
    /// Cancel safe and re-enterable: the receiver is polled in place, so dropping the returned
    /// stream loses nothing and `stream` can be called again.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let Self {
            receiver,
            sender,
            coordinator,
            ..
        } = self;
        futures::stream::poll_fn(move |cx| {
            receiver.poll_recv(cx).map(|delivery| {
                delivery.map(|delivery| {
                    Ok(KafkaTestMessage {
                        delivery: Some(delivery),
                        sender: sender.clone(),
                        coordinator: coordinator.clone(),
                    })
                })
            })
        })
    }
}

impl BatchSubscriber for KafkaTestSubscriber {
    type Batch = Vec<KafkaTestMessage>;

    /// Streams non-empty pages natively: each waits for one delivery, then drains whatever
    /// else is already enqueued (mirroring the real subscriber's drain-what-is-fetched
    /// behavior).
    ///
    /// # Cancel safety
    ///
    /// Same guarantees as [`Subscriber::stream`]: cancel safe between polls.
    fn batches(
        &mut self,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        let Self {
            receiver,
            sender,
            coordinator,
            ..
        } = self;
        futures::stream::poll_fn(move |cx| {
            receiver.poll_recv(cx).map(|delivery| {
                delivery.map(|first| {
                    let mut batch = vec![KafkaTestMessage {
                        delivery: Some(first),
                        sender: sender.clone(),
                        coordinator: coordinator.clone(),
                    }];
                    while let Ok(delivery) = receiver.try_recv() {
                        batch.push(KafkaTestMessage {
                            delivery: Some(delivery),
                            sender: sender.clone(),
                            coordinator: coordinator.clone(),
                        });
                    }
                    Ok(batch)
                })
            })
        })
    }
}

/// One in-process delivery.
pub struct KafkaTestMessage {
    delivery: Option<TestDelivery>,
    sender: DeliverySender,
    coordinator: Option<Coordinator>,
}

impl KafkaTestMessage {
    fn take(&mut self) -> TestDelivery {
        // The settle methods consume `self`, so a second settle cannot compile; reaching this
        // twice is an internal invariant violation.
        self.delivery
            .take()
            .expect("KafkaTestMessage settled twice")
    }
}

impl Drop for KafkaTestMessage {
    fn drop(&mut self) {
        // Balance the router's `enqueued` exactly once per delivery, whatever the dispatch
        // path did (ack, nack, panic, or plain drop).
        if let Some(coordinator) = self.coordinator.take() {
            coordinator.consumed();
        }
    }
}

impl fmt::Debug for KafkaTestMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaTestMessage")
            .field("delivery", &self.delivery)
            .finish_non_exhaustive()
    }
}

impl IncomingMessage for KafkaTestMessage {
    fn payload(&self) -> &[u8] {
        &self
            .delivery
            .as_ref()
            .expect("message accessed after settlement")
            .payload
    }

    fn headers(&self) -> &Headers {
        &self
            .delivery
            .as_ref()
            .expect("message accessed after settlement")
            .headers
    }

    /// The partition key from the `PARTITION_KEY_HEADER`, mirroring the real message so keyed
    /// worker lanes behave the same in-process.
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(crate::PARTITION_KEY_HEADER)
    }

    /// Finalizes the delivery.
    ///
    /// # Errors
    ///
    /// Never fails; the in-process transport has no position to store.
    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        drop(self.take());
        ready(Ok(()))
    }

    /// Re-enqueues to the same subscription (`requeue = true`) or drops (`requeue = false`).
    ///
    /// # Errors
    ///
    /// Never fails; the in-process transport has no position to store.
    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self.take();
        if requeue && self.sender.send(delivery).is_ok() {
            // This bypasses the router fanout, so account for the new in-flight delivery here.
            if let Some(coordinator) = &self.coordinator {
                coordinator.enqueued();
            }
        }
        ready(Ok(()))
    }
}

impl Partitioned for KafkaTestMessage {
    /// The partition key from the `PARTITION_KEY_HEADER`, mirroring the real message.
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(crate::PARTITION_KEY_HEADER)
    }
}
