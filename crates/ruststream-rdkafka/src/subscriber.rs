//! The subscriber: a stream of Kafka deliveries from one topic subscription.

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

#[cfg(feature = "schema-registry")]
use bytes::Bytes;
use futures::{Stream, ready};
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
#[cfg(feature = "schema-registry")]
use ruststream::IncomingMessage;
use ruststream::{BatchSubscriber, Seekable, Str, Subscriber};
use tracing::{debug, warn};

use crate::convert;
use crate::eos::EOS_SOURCE_HEADER;
use crate::error::KafkaError;
use crate::message::{KafkaMessage, Lane, Settlement};
use crate::record::{HeldRecord, SharedConsumer};
use crate::seek::KafkaSeeker;
use crate::subscription::{Commit, LaneKey};
use crate::tracker::{CommitTracker, TrackingContext};

/// Whether librdkafka is already retrying this error by itself, making a stream error item
/// noise rather than signal. The set is deliberately small and explicit; when in doubt, the
/// error is forwarded.
fn is_transient(err: &rdkafka::error::KafkaError) -> bool {
    // A subscribed topic that does not exist (yet): pending creation is routine when the
    // broker auto-creates topics, and librdkafka keeps refreshing metadata until it appears.
    err.rdkafka_error_code() == Some(RDKafkaErrorCode::UnknownTopicOrPartition)
}

/// Where a delivery's topic name comes from.
///
/// A subscription over one literal topic knows the answer before its first record, so the
/// delivery takes the name the subscription already minted. Reading it back out of librdkafka
/// costs a `CStr` walk and a UTF-8 validation of the same bytes on every delivery, and answers
/// what the descriptor said.
#[derive(Debug)]
pub(crate) enum DeliveredTopic {
    /// Every record of this subscription comes from this topic: one literal name, or the topic
    /// whose partitions were assigned by hand.
    One(Str),
    /// Several names or a pattern: only the record knows which topic it came from. The last one
    /// is kept because a fetch hands a consumer one topic's records at a time, so a whole run
    /// answers without minting the name again.
    PerRecord(Option<Str>),
}

impl DeliveredTopic {
    /// The topic of this delivery, as the shared string every delivery of that topic carries.
    fn of(&mut self, record: &HeldRecord) -> Str {
        match self {
            Self::One(name) => name.clone(),
            Self::PerRecord(last) => {
                let name = record.topic();
                if let Some(known) = last
                    && &**known == name
                {
                    return known.clone();
                }
                let minted = Str::from(name);
                *last = Some(minted.clone());
                minted
            }
        }
    }
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
    /// Where a delivery's topic name comes from; see [`DeliveredTopic`].
    delivered_topic: DeliveredTopic,
    commit: Commit,
    tracker: Arc<CommitTracker>,
    lane_key: LaneKey,
    /// Minted once, when the subscription opens: every delivery carries a clone so a handler's
    /// context can hand out the reposition handle for one reference-count bump.
    seeker: Arc<KafkaSeeker>,
    #[cfg(feature = "schema-registry")]
    schema_registry: Option<crate::schema_registry::SchemaRegistry>,
    #[cfg(feature = "schema-registry")]
    schema_prefetch: Option<crate::schema_registry::SchemaPrefetch>,
    /// Whether the subscriber is inside an episode of transient consume errors; the first
    /// error of an episode warns, repeats are debug, recovery closes the episode.
    in_transient_episode: bool,
}

impl KafkaSubscriber {
    pub(crate) fn new(
        consumer: Arc<StreamConsumer<TrackingContext>>,
        topic: String,
        delivered_topic: DeliveredTopic,
        commit: Commit,
        tracker: Arc<CommitTracker>,
        lane_key: LaneKey,
    ) -> Self {
        let seeker = Arc::new(KafkaSeeker::new(
            Arc::clone(&consumer),
            Arc::clone(&tracker),
        ));
        Self {
            consumer,
            topic,
            delivered_topic,
            commit,
            tracker,
            lane_key,
            seeker,
            #[cfg(feature = "schema-registry")]
            schema_registry: None,
            #[cfg(feature = "schema-registry")]
            schema_prefetch: None,
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

    #[cfg(feature = "schema-registry")]
    pub(crate) fn with_schema_prefetch(
        mut self,
        prefetch: Option<crate::schema_registry::SchemaPrefetch>,
    ) -> Self {
        self.schema_prefetch = prefetch;
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

    /// Turns a fetched record into a delivery. The record travels into the delivery whole: what
    /// is read here is what a delivery cannot answer from the record later, or what a later read
    /// could no longer see.
    fn map_delivery(&mut self, record: HeldRecord) -> KafkaMessage {
        // Minted once per topic and shared from here on: the delivery carries it, and the
        // tracker opens this partition's slot under it.
        let topic = self.delivered_topic.of(&record);
        let partition = record.partition();
        let offset = record.offset();
        // The headers stay unread unless the delivery is asked for them - except under
        // transactional commits, where the source coordinates ride them so the reply path can
        // pair a publishing handler's reply with its consumed offset (see EosPipeline::replies);
        // stripped from every outgoing publish, so they never hit the wire.
        let headers = OnceLock::new();
        if matches!(self.commit, Commit::Transactional(_)) {
            let mut map = convert::headers_from_message(&record);
            map.insert(
                Str::from_static(EOS_SOURCE_HEADER),
                crate::eos::encode_source(&topic, partition, offset),
            );
            headers.set(map).expect("the cell was just created");
        }
        // The generation is captured here, where the delivery is pulled, never where it settles:
        // that is what lets a reposition landing in between tell a delivery of the replaced read
        // position apart from one the new position produced.
        let settlement = match &self.commit {
            Commit::Auto => Settlement::Advisory,
            Commit::Tracked => Settlement::Tracked {
                tracker: Arc::clone(&self.tracker),
                slot: self.tracker.delivered(&topic, partition, offset),
            },
            Commit::Transactional(_) => Settlement::Transactional {
                tracker: Arc::clone(&self.tracker),
                slot: self.tracker.delivered(&topic, partition, offset),
            },
        };
        // The key itself is not built here: a subscription with no keyed lanes is never asked
        // for one, and both forms answer out of what the delivery already holds.
        let lane = match self.lane_key {
            LaneKey::RecordKey => Lane::RecordKey,
            LaneKey::Partition => Lane::Partition(OnceLock::new()),
        };
        KafkaMessage::new(
            record,
            headers,
            topic,
            partition,
            offset,
            settlement,
            lane,
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
        // Cloned once per stream and never per delivery: a wait owns the handles it needs, so
        // it borrows nothing from the stream that holds it.
        let context = Arc::clone(self.consumer.context());
        let consumer = Arc::clone(&self.consumer);
        KafkaStream {
            #[cfg(feature = "schema-registry")]
            registry: self.schema_registry.clone(),
            #[cfg(feature = "schema-registry")]
            prefetch: self.schema_prefetch.clone(),
            #[cfg(feature = "schema-registry")]
            finishing: None,
            sub: self,
            context,
            consumer,
            waiting: None,
        }
    }
}

/// The stream [`KafkaSubscriber::stream`] hands out.
///
/// Written out rather than unfolded from a closure: a combinator carries the delivery out of a
/// future's output, through a tuple and into the stream's own return, and this type is 216 bytes,
/// so the compiler moved it three times per message before the caller ever saw it.
pub(crate) struct KafkaStream<'a> {
    sub: &'a mut KafkaSubscriber,
    context: Arc<TrackingContext>,
    consumer: SharedConsumer,
    /// The wait, kept across polls for as long as the fetch queue stays dry: the registration
    /// `recv` makes with the consumer's queue dies with the future that made it, so re-arming it
    /// per poll would drop the wakeup it was waiting for. Boxed because an `async fn` future
    /// cannot be named, and built once per idle episode - a subscription keeping up with its
    /// topic never reaches it.
    waiting: Option<Pin<Box<dyn Future<Output = ColdTurn> + Send>>>,
    /// The registry middleware, held here rather than reached through the subscriber, so the
    /// future below owns what it reads.
    #[cfg(feature = "schema-registry")]
    registry: Option<crate::schema_registry::SchemaRegistry>,
    #[cfg(feature = "schema-registry")]
    prefetch: Option<crate::schema_registry::SchemaPrefetch>,
    /// The registry's own async step, for the deliveries that have one. A subscription with no
    /// registry attached never builds it.
    #[cfg(feature = "schema-registry")]
    finishing: Option<Pin<Box<dyn Future<Output = KafkaMessage> + Send>>>,
}

/// What one turn of the cold path resolved to.
enum ColdTurn {
    /// A record the wait took, or what the consumer reported instead of one.
    Record(Result<HeldRecord, rdkafka::error::KafkaError>),
    /// A start position the rebalance could not apply, taken while the wait was being armed.
    Failed(KafkaError),
    /// The start-failure waiter fired; the flag is read at the top of the next turn.
    Woken,
}

/// Waits for a record, or for a start position the rebalance could not apply.
///
/// The waiter is enabled before the flag is read, so a failure landing between the two wakes this
/// wait instead of being missed - which is what makes it reach a subscription whose topic may
/// never deliver anything.
///
/// # Cancel safety
///
/// Cancel safe: dropping this future before it resolves takes no record.
async fn cold_turn(context: Arc<TrackingContext>, consumer: SharedConsumer) -> ColdTurn {
    let mut recorded = pin!(context.start_failure_waiter());
    recorded.as_mut().enable();
    if let Some(err) = context.take_start_failure() {
        return ColdTurn::Failed(err);
    }
    tokio::select! {
        biased;
        () = recorded => ColdTurn::Woken,
        received = HeldRecord::next(&consumer) => ColdTurn::Record(received),
    }
}

/// Runs the registry middleware over a delivery.
///
/// The prefetch first: it reads the envelope, which the transcode would have replaced with a
/// JSON document. The two are alternatives anyway.
#[cfg(feature = "schema-registry")]
async fn finish_delivery(
    registry: Option<crate::schema_registry::SchemaRegistry>,
    prefetch: Option<crate::schema_registry::SchemaPrefetch>,
    mut item: KafkaMessage,
) -> KafkaMessage {
    if let Some(prefetch) = &prefetch {
        prefetch
            .warm_delivery(IncomingMessage::payload(&item))
            .await;
    }
    if let Some(registry) = &registry
        && let Some(json) = registry
            .incoming_to_json(IncomingMessage::payload(&item))
            .await
    {
        item.replace_payload(Bytes::from(json));
    }
    item
}

impl KafkaStream<'_> {
    /// Hands the delivery on, through the registry middleware when one is attached.
    fn deliver(&mut self, record: HeldRecord) -> Poll<Option<Result<KafkaMessage, KafkaError>>> {
        self.sub.note_recovered();
        let item = self.sub.map_delivery(record);
        #[cfg(feature = "schema-registry")]
        if self.registry.is_some() || self.prefetch.is_some() {
            self.finishing = Some(Box::pin(finish_delivery(
                self.registry.clone(),
                self.prefetch.clone(),
                item,
            )));
            return Poll::Pending;
        }
        Poll::Ready(Some(Ok(item)))
    }
}

impl Stream for KafkaStream<'_> {
    type Item = Result<KafkaMessage, KafkaError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            #[cfg(feature = "schema-registry")]
            if let Some(finishing) = &mut this.finishing {
                let item = ready!(finishing.as_mut().poll(cx));
                this.finishing = None;
                return Poll::Ready(Some(Ok(item)));
            }
            // A start position the rebalance could not apply preempts whatever was fetched: a
            // subscription that was not opened where it was asked must not pass for a working
            // one.
            if let Some(err) = this.context.take_start_failure() {
                return Poll::Ready(Some(Err(err)));
            }
            if let Some(waiting) = &mut this.waiting {
                let turn = ready!(waiting.as_mut().poll(cx));
                this.waiting = None;
                match turn {
                    // The waiter fired: the flag is read at the top of the next turn.
                    ColdTurn::Woken => continue,
                    ColdTurn::Failed(err) => return Poll::Ready(Some(Err(err))),
                    ColdTurn::Record(Ok(record)) => {
                        if let Poll::Ready(item) = this.deliver(record) {
                            return Poll::Ready(item);
                        }
                        continue;
                    }
                    ColdTurn::Record(Err(err)) if is_transient(&err) => {
                        this.sub.note_transient(&err);
                        continue;
                    }
                    ColdTurn::Record(Err(err)) => {
                        return Poll::Ready(Some(Err(KafkaError::consume(err))));
                    }
                }
            }
            // A record already fetched needs no waiter: the failure flag above is sticky, so it
            // is read on the turn that finds nothing to deliver.
            match HeldRecord::ready(&this.consumer) {
                Some(Ok(record)) => {
                    if let Poll::Ready(item) = this.deliver(record) {
                        return Poll::Ready(item);
                    }
                }
                Some(Err(err)) if is_transient(&err) => this.sub.note_transient(&err),
                Some(Err(err)) => return Poll::Ready(Some(Err(KafkaError::consume(err)))),
                // Nothing fetched, so the wait begins on the next turn of this loop.
                None => {
                    this.waiting = Some(Box::pin(cold_turn(
                        Arc::clone(&this.context),
                        Arc::clone(&this.consumer),
                    )));
                }
            }
        }
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

    /// Streams non-empty batches natively: each waits for one delivery, then drains what
    /// librdkafka has already fetched, up to `size` messages in total. The batch never carries
    /// more than the registration's `batch(n)` asked for, and carries fewer whenever the fetch
    /// queue holds less; how much librdkafka keeps queued locally stays a consumer setting
    /// (`queued.max.messages.kbytes` and friends, settable through
    /// [`KafkaTopic::config`](crate::KafkaTopic::config)). A consumer error inside an open batch
    /// yields the batch first; the error (if it persists) surfaces on the next poll.
    ///
    /// # Cancel safety
    ///
    /// Same guarantees as [`Subscriber::stream`]: cancel safe between polls, no delivery is
    /// lost by dropping the stream.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        let size = size.get();
        let context = Arc::clone(self.consumer.context());
        // See `stream`: the consumer is held beside the subscriber, not through it.
        let consumer = Arc::clone(&self.consumer);
        futures::stream::unfold(
            (self, context, consumer),
            move |(sub, context, consumer)| async move {
                // Wait for the batch's first delivery, or for a start position the rebalance could
                // not apply - see `stream` for why the wait is a select and not a check.
                let first = loop {
                    let recorded = context.start_failure_waiter();
                    if let Some(err) = context.take_start_failure() {
                        drop(recorded);
                        return Some((Err(err), (sub, context, consumer)));
                    }
                    let received = tokio::select! {
                        biased;
                        () = recorded => continue,
                        received = HeldRecord::next(&consumer) => received,
                    };
                    match received {
                        Ok(record) => break sub.map_delivery(record),
                        Err(err) if is_transient(&err) => sub.note_transient(&err),
                        Err(err) => {
                            return Some((Err(KafkaError::consume(err)), (sub, context, consumer)));
                        }
                    }
                };
                sub.note_recovered();

                let mut batch = Vec::with_capacity(size.min(64));
                batch.push(first);
                // Drain what is already fetched, stopping at the batch size; recv is cancel safe,
                // so dropping the probe future loses nothing.
                while batch.len() < size {
                    let Some(result) = HeldRecord::ready(&consumer) else {
                        break;
                    };
                    match result {
                        Ok(record) => {
                            let item = sub.map_delivery(record);
                            batch.push(item);
                        }
                        Err(err) if is_transient(&err) => sub.note_transient(&err),
                        // Yield what was collected; a persistent error re-surfaces on the next
                        // batch's first recv.
                        Err(_) => break,
                    }
                }
                #[cfg(feature = "schema-registry")]
                for item in &mut batch {
                    sub.transcode(item).await;
                }
                Some((Ok(batch), (sub, context, consumer)))
            },
        )
    }
}
