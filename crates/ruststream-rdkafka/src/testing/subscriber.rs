//! The in-process subscriber and its delivery type.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, ready as poll_ready};

use bytes::Bytes;
use futures::Stream;
use ruststream::testing::Coordinator;
use ruststream::{
    AckError, BatchSubscriber, HeaderMap, IncomingMessage, Partitioned, Positioned, Seekable,
    Seeker as _, Subscriber,
};

use super::broker::TestBrokerState;
use super::router::{DeliveryReceiver, SubscriptionId, TestDelivery};
use super::seek::InProcessSeek;
use crate::error::KafkaError;
use crate::seek::{KafkaPosition, KafkaSeeker};
use crate::topic::{Commit, LaneKey};

/// Offsets handed to the application and not yet settled, per topic.
///
/// The lowest entry of a topic is that topic's committed position: everything below it is
/// settled, so a `Commit::Tracked` redelivery resumes from there. That is the same
/// contiguous-prefix rule the real subscriber's commit tracker applies.
type Unsettled = HashMap<String, BTreeSet<usize>>;

/// In-process subscriber on one topic name.
///
/// # Settlement
///
/// Settlement is a read position here, as it is on a cluster, not a per-message frame. `ack` and
/// `nack(false)` settle the delivery's offset; `nack(true)` under [`Commit::Tracked`] leaves it
/// unsettled and resumes from the committed position, so the record **and the tail behind it**
/// are delivered again, while under [`Commit::Auto`] it is advisory and brings nothing back.
/// What is not simulated is what only a cluster has: a position that survives the subscription,
/// and redelivery triggered by a rebalance.
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
    receiver: DeliveryReceiver,
    coordinator: Option<Coordinator>,
    /// The read position this subscription is on. A reposition bumps it, so deliveries queued
    /// under an earlier one are dropped instead of settling into a position that replaced them.
    generation: Arc<AtomicU64>,
    /// Minted once, when the subscription opens: every delivery carries a clone, so a handler's
    /// context hands out the reposition handle for one reference-count bump.
    seeker: Arc<KafkaSeeker>,
    /// What the descriptor asked worker lanes to be keyed by, resolved per delivery.
    lane_key: LaneKey,
    /// How the descriptor asked settlement to be committed, which is what decides whether
    /// `nack(true)` brings anything back and how much.
    commit: Commit,
    /// Shared with every live delivery, so settling one moves the committed position the next
    /// redelivery rewinds to.
    unsettled: Arc<Mutex<Unsettled>>,
}

impl KafkaTestSubscriber {
    pub(crate) fn open_many(
        state: &Arc<TestBrokerState>,
        topics: &[String],
        group: Option<&str>,
        lane_key: LaneKey,
        commit: Commit,
    ) -> Self {
        let generation = Arc::new(AtomicU64::new(0));
        let (ids, sender, receiver) = state.router.subscribe_many(topics, group, &generation);
        let coordinator = state.coordinator();
        // The seeker owns the only remaining handle on the send side: every redelivery in this
        // transport is a reposition, so nothing else needs to enqueue.
        let control = InProcessSeek::new(state, topics, sender, &generation);
        Self {
            state: Arc::clone(state),
            ids,
            topic: topics.join(","),
            receiver,
            coordinator,
            generation,
            seeker: Arc::new(KafkaSeeker::in_process(Arc::new(control))),
            lane_key,
            commit,
            unsettled: Arc::new(Mutex::new(Unsettled::new())),
        }
    }

    /// The subscribed topic name(s), joined with `,` when there are several.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

/// What turns a queued [`TestDelivery`] into a [`KafkaTestMessage`], borrowed from the
/// subscription for the life of one stream.
struct Accepting<'a> {
    coordinator: Option<&'a Coordinator>,
    generation: &'a AtomicU64,
    seeker: &'a Arc<KafkaSeeker>,
    unsettled: &'a Arc<Mutex<Unsettled>>,
    lane_key: LaneKey,
    commit: &'a Commit,
}

impl Accepting<'_> {
    /// The lane key a delivery carries, resolved the way the real subscriber resolves it.
    ///
    /// Under [`LaneKey::Partition`] every delivery here shares one lane, because the transport
    /// gives every topic exactly one partition - which is what a real single-partition topic
    /// does too, and what keeps `workers(n)` from appearing more concurrent in process than the
    /// cluster would be.
    fn lane_of(&self, headers: &HeaderMap) -> Option<Bytes> {
        match self.lane_key {
            LaneKey::RecordKey => headers
                .get(crate::PARTITION_KEY_HEADER)
                .map(Bytes::copy_from_slice),
            LaneKey::Partition => Some(Bytes::from_static(b"0")),
        }
    }

    /// Builds the delivery, or reports it as belonging to a read position this subscription no
    /// longer has. A stale delivery is accounted as consumed here: it was counted in flight when
    /// it was enqueued, and nothing else will settle it.
    fn accept(&self, delivery: TestDelivery) -> Option<KafkaTestMessage> {
        if delivery.generation < self.generation.load(Ordering::Acquire) {
            if let Some(coordinator) = self.coordinator {
                coordinator.consumed();
            }
            return None;
        }
        let lane = self.lane_of(&delivery.headers);
        // Handed to the application, so it counts against the committed position until it
        // settles - the rule that makes a `Commit::Tracked` rewind land where it should.
        self.unsettled
            .lock()
            .expect("test settlement mutex poisoned")
            .entry(delivery.topic.clone())
            .or_default()
            .insert(delivery.seq);
        Some(KafkaTestMessage {
            delivery: Some(delivery),
            coordinator: self.coordinator.cloned(),
            seeker: Arc::clone(self.seeker),
            unsettled: Arc::clone(self.unsettled),
            commit: self.commit.clone(),
            lane,
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
        let accepting = Accepting {
            coordinator: self.coordinator.as_ref(),
            generation: &self.generation,
            seeker: &self.seeker,
            unsettled: &self.unsettled,
            lane_key: self.lane_key,
            commit: &self.commit,
        };
        let receiver = &mut self.receiver;
        futures::stream::poll_fn(move |cx| {
            loop {
                match poll_ready!(receiver.poll_recv(cx)) {
                    // A delivery from before a reposition: drop it and take the next one, which
                    // is what makes the replay the only thing the handler sees.
                    Some(delivery) => {
                        if let Some(message) = accepting.accept(delivery) {
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

    /// Streams non-empty batches natively: each waits for one delivery, then drains whatever
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
        let accepting = Accepting {
            coordinator: self.coordinator.as_ref(),
            generation: &self.generation,
            seeker: &self.seeker,
            unsettled: &self.unsettled,
            lane_key: self.lane_key,
            commit: &self.commit,
        };
        let receiver = &mut self.receiver;
        futures::stream::poll_fn(move |cx| {
            let first = loop {
                match poll_ready!(receiver.poll_recv(cx)) {
                    Some(delivery) => {
                        if let Some(message) = accepting.accept(delivery) {
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
                if let Some(message) = accepting.accept(delivery) {
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
    coordinator: Option<Coordinator>,
    seeker: Arc<KafkaSeeker>,
    /// The subscription's unsettled offsets, so settling this delivery moves the committed
    /// position its siblings rewind to.
    unsettled: Arc<Mutex<Unsettled>>,
    /// The subscription's commit mode, which decides what `nack(true)` means.
    commit: Commit,
    /// The keyed-lane key, resolved from the subscription's [`LaneKey`] exactly as the real
    /// subscriber resolves it.
    lane: Option<Bytes>,
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

    /// Marks this delivery settled, so it stops holding the topic's committed position down.
    fn settle(&self, topic: &str, seq: usize) {
        let mut unsettled = self
            .unsettled
            .lock()
            .expect("test settlement mutex poisoned");
        if let Some(offsets) = unsettled.get_mut(topic) {
            offsets.remove(&seq);
        }
    }

    /// The topic's committed position: the lowest offset still unsettled, or `None` when
    /// everything handed over so far has settled.
    fn committed(&self, topic: &str) -> Option<usize> {
        self.unsettled
            .lock()
            .expect("test settlement mutex poisoned")
            .get(topic)
            .and_then(|offsets| offsets.first().copied())
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

    /// The keyed-lane key, mirroring the real message: the source partition (the default, and
    /// always `0` here because the transport gives every topic one), or the record key under
    /// [`LaneKey::RecordKey`]. The record key itself stays reachable through
    /// [`KafkaTestMessage::key`].
    fn partition_key(&self) -> Option<&[u8]> {
        self.lane.as_deref()
    }

    /// Settles the delivery, advancing the subscription's committed position over it.
    ///
    /// # Errors
    ///
    /// Never fails; settling an in-process delivery reaches no broker.
    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self.take();
        self.settle(&delivery.topic, delivery.seq);
        ready(Ok(()))
    }

    /// Settlement as Kafka defines it, which is a read position and not a per-message frame.
    ///
    /// `requeue = false` settles the offset (the drop path), so nothing comes back.
    ///
    /// `requeue = true` under [`Commit::Tracked`] leaves the offset unsettled and resumes the
    /// subscription from the committed position, so this record **and everything after it on the
    /// topic** are delivered again - the at-least-once duplication a real rewind produces, not a
    /// single re-enqueued frame. Under [`Commit::Auto`] it is advisory and nothing comes back:
    /// librdkafka stored the position when the record was handed over, so the offset is already
    /// past it.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] when the rewind cannot be applied, which in process means
    /// the transport was shut down under the subscription.
    async fn nack(mut self, requeue: bool) -> Result<(), AckError> {
        let delivery = self.take();
        // Non-exhaustive, and `Transactional` has no pipeline in process to defer the commit to,
        // so it rewinds like `Tracked` rather than inventing an exactly-once window.
        let advisory = matches!(self.commit, Commit::Auto);
        if !requeue || advisory {
            self.settle(&delivery.topic, delivery.seq);
            return Ok(());
        }
        // Left unsettled, so the committed position is at or below this record.
        let from = self.committed(&delivery.topic).unwrap_or(delivery.seq);
        let from = i64::try_from(from).unwrap_or(i64::MAX);
        self.seeker
            .seek(KafkaPosition::topic_offset(&delivery.topic, 0, from))
            .await
            .map_err(|err| AckError::Broker(Box::new(err)))
    }
}

impl Partitioned for KafkaTestMessage {
    /// The keyed-lane key (see [`IncomingMessage::partition_key`] on this type), mirroring the
    /// real message.
    fn partition_key(&self) -> Option<&[u8]> {
        self.lane.as_deref()
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
