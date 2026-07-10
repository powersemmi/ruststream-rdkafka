//! The in-process subscriber and its delivery type.

use std::fmt;
use std::sync::Arc;

use futures::Stream;
use ruststream::testing::Coordinator;
use ruststream::{AckError, Headers, IncomingMessage, Partitioned, Subscriber};

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
    id: SubscriptionId,
    topic: String,
    sender: DeliverySender,
    receiver: DeliveryReceiver,
    coordinator: Option<Coordinator>,
}

impl KafkaTestSubscriber {
    pub(crate) fn open(state: &Arc<TestBrokerState>, topic: String) -> Self {
        let (id, sender, receiver) = state.router.subscribe(topic.clone());
        let coordinator = state.coordinator();
        Self {
            state: Arc::clone(state),
            id,
            topic,
            sender,
            receiver,
            coordinator,
        }
    }

    /// The topic this subscriber consumes.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl Drop for KafkaTestSubscriber {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
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
    async fn ack(mut self) -> Result<(), AckError> {
        drop(self.take());
        Ok(())
    }

    /// Re-enqueues to the same subscription (`requeue = true`) or drops (`requeue = false`).
    ///
    /// # Errors
    ///
    /// Never fails; the in-process transport has no position to store.
    async fn nack(mut self, requeue: bool) -> Result<(), AckError> {
        let delivery = self.take();
        if requeue && self.sender.send(delivery).is_ok() {
            // This bypasses the router fanout, so account for the new in-flight delivery here.
            if let Some(coordinator) = &self.coordinator {
                coordinator.enqueued();
            }
        }
        Ok(())
    }
}

impl Partitioned for KafkaTestMessage {
    /// The partition key from the `PARTITION_KEY_HEADER`, mirroring the real message.
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(crate::PARTITION_KEY_HEADER)
    }
}
