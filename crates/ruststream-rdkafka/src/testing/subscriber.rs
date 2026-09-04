//! The in-process subscriber and its delivery type.

use std::fmt;
use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Poll, ready as poll_ready};

use futures::Stream;
use ruststream::testing::Coordinator;
use ruststream::{
    AckError, BatchSubscriber, HeaderMap, IncomingMessage, Partitioned, Positioned, Seekable,
    Subscriber,
};

use super::broker::TestBrokerState;
use super::router::{DeliveryReceiver, DeliverySender, SubscriptionId, TestDelivery};
use super::seek::InProcessSeek;
use crate::error::KafkaError;
use crate::seek::{KafkaPosition, KafkaSeeker};

/// In-process subscriber on one topic name.
///
/// Yielded messages settle like the routing contract expects: ack finalizes, `nack(true)`
/// re-enqueues to this same subscription, `nack(false)` drops. The real transport's
/// committed-position semantics (holes, watermarks, redelivery on rebalance) are deliberately
/// not simulated.
///
/// # Repositioning
///
/// The transport retains every published message, so this subscription is seekable over that
/// log: [`Seekable::seeker`] mints a [`KafkaSeeker`] whose reposition re-enqueues the log suffix
/// from the target on. A delivery's offset is its message's index in its topic's log, every topic
/// has one partition numbered zero, and [`KafkaPosition::Earliest`], [`KafkaPosition::Latest`]
/// and [`KafkaPosition::Offset`] resolve against that log. [`KafkaPosition::Timestamp`] does not:
/// the transport stamps no record timestamps, so it reports
/// [`KafkaError::InvalidOptions`] rather than resolving to something invented.
pub struct KafkaTestSubscriber {
    state: Arc<TestBrokerState>,
    ids: Vec<SubscriptionId>,
    topic: String,
    sender: DeliverySender,
    receiver: DeliveryReceiver,
    coordinator: Option<Coordinator>,
    /// The read position this subscription is on. A reposition bumps it, so deliveries queued
    /// under an earlier one are dropped instead of settling into a position that replaced them.
    generation: Arc<AtomicU64>,
    /// Minted once, when the subscription opens: every delivery carries a clone, so a handler's
    /// context hands out the reposition handle for one reference-count bump.
    seeker: Arc<KafkaSeeker>,
}

impl KafkaTestSubscriber {
    pub(crate) fn open_many(state: &Arc<TestBrokerState>, topics: &[String]) -> Self {
        let generation = Arc::new(AtomicU64::new(0));
        let (ids, sender, receiver) = state.router.subscribe_many(topics, &generation);
        let coordinator = state.coordinator();
        let control = InProcessSeek::new(state, topics, sender.clone(), &generation);
        Self {
            state: Arc::clone(state),
            ids,
            topic: topics.join(","),
            sender,
            receiver,
            coordinator,
            generation,
            seeker: Arc::new(KafkaSeeker::in_process(Arc::new(control))),
        }
    }

    /// The subscribed topic name(s), joined with `,` when there are several.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Builds the delivery, or reports it as belonging to a read position this subscription no
    /// longer has. A stale delivery is accounted as consumed here: it was counted in flight when
    /// it was enqueued, and nothing else will settle it.
    fn accept(
        delivery: TestDelivery,
        sender: &DeliverySender,
        coordinator: Option<&Coordinator>,
        generation: &AtomicU64,
        seeker: &Arc<KafkaSeeker>,
    ) -> Option<KafkaTestMessage> {
        if delivery.generation < generation.load(Ordering::Acquire) {
            if let Some(coordinator) = coordinator {
                coordinator.consumed();
            }
            return None;
        }
        Some(KafkaTestMessage {
            delivery: Some(delivery),
            sender: sender.clone(),
            coordinator: coordinator.cloned(),
            seeker: Arc::clone(seeker),
        })
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
            generation,
            seeker,
            ..
        } = self;
        futures::stream::poll_fn(move |cx| {
            loop {
                match poll_ready!(receiver.poll_recv(cx)) {
                    // A delivery from before a reposition: drop it and take the next one, which
                    // is what makes the replay the only thing the handler sees.
                    Some(delivery) => {
                        if let Some(message) =
                            Self::accept(delivery, sender, coordinator.as_ref(), generation, seeker)
                        {
                            return Poll::Ready(Some(Ok(message)));
                        }
                    }
                    None => return Poll::Ready(None),
                }
            }
        })
    }
}

impl Seekable for KafkaTestSubscriber {
    type Seeker = KafkaSeeker;

    /// Hands out a handle repositioning this subscription over the transport's retained log; see
    /// the type-level documentation for which positions it resolves.
    fn seeker(&self) -> Self::Seeker {
        KafkaSeeker::clone(&self.seeker)
    }
}

impl BatchSubscriber for KafkaTestSubscriber {
    type Batch = Vec<KafkaTestMessage>;

    /// Streams non-empty pages natively: each waits for one delivery, then drains whatever
    /// else is already enqueued, up to `size` messages in total (mirroring the real
    /// subscriber's bounded drain-what-is-fetched behavior).
    ///
    /// # Cancel safety
    ///
    /// Same guarantees as [`Subscriber::stream`]: cancel safe between polls.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        let size = size.get();
        let Self {
            receiver,
            sender,
            coordinator,
            generation,
            seeker,
            ..
        } = self;
        futures::stream::poll_fn(move |cx| {
            let first = loop {
                match poll_ready!(receiver.poll_recv(cx)) {
                    Some(delivery) => {
                        if let Some(message) =
                            Self::accept(delivery, sender, coordinator.as_ref(), generation, seeker)
                        {
                            break message;
                        }
                    }
                    None => return Poll::Ready(None),
                }
            };
            let mut batch = Vec::with_capacity(size.min(64));
            batch.push(first);
            while batch.len() < size {
                let Ok(delivery) = receiver.try_recv() else {
                    break;
                };
                if let Some(message) =
                    Self::accept(delivery, sender, coordinator.as_ref(), generation, seeker)
                {
                    batch.push(message);
                }
            }
            Poll::Ready(Some(Ok(batch)))
        })
    }
}

/// One in-process delivery.
pub struct KafkaTestMessage {
    delivery: Option<TestDelivery>,
    sender: DeliverySender,
    coordinator: Option<Coordinator>,
    seeker: Arc<KafkaSeeker>,
}

impl KafkaTestMessage {
    fn take(&mut self) -> TestDelivery {
        // The settle methods consume `self`, so a second settle cannot compile; reaching this
        // twice is an internal invariant violation.
        self.delivery
            .take()
            .expect("KafkaTestMessage settled twice")
    }

    fn queued(&self) -> &TestDelivery {
        self.delivery
            .as_ref()
            .expect("message accessed after settlement")
    }

    /// The topic this delivery came from.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.queued().topic
    }

    /// The delivery's offset: its message's zero-based index in the topic's retained log.
    #[must_use]
    pub fn offset(&self) -> i64 {
        i64::try_from(self.queued().seq).unwrap_or(i64::MAX)
    }

    /// The record key, read from the partition-key header like the real delivery does.
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.queued().headers.get(crate::PARTITION_KEY_HEADER)
    }

    /// The subscription's reposition handle, for the context built off this delivery.
    pub(crate) fn seeker_handle(&self) -> Arc<KafkaSeeker> {
        Arc::clone(&self.seeker)
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
        &self.queued().payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.queued().headers
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
    /// A requeued delivery keeps the read-position generation it arrived under, so a reposition
    /// landing in between discards it: the replay already covers everything from the target on.
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

impl Positioned for KafkaTestMessage {
    type Position = KafkaPosition;

    /// This delivery's own coordinates in the retained log: seeking to them redelivers exactly
    /// this record and the ordered suffix behind it - the pinned contract the real delivery
    /// carries, over the log this transport keeps.
    fn position(&self) -> Self::Position {
        KafkaPosition::topic_offset(self.topic(), 0, self.offset())
    }
}
