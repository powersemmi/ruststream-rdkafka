//! The in-process Kafka test broker, and the application-level scenarios it carries.
//!
//! Anything whose subject is a service - a handler reading its broker context, repositioning its
//! own subscription, publishing through an `Out` slot - runs here on `TestApp`, because that is
//! the level it lives at: real handlers, the real dispatch path, harness assertions, and no
//! cluster. The cases that drive `KafkaTestBroker` / `KafkaTestPublisher` /
//! `KafkaTestSubscriber` directly are the ones whose subject IS that transport (its routing
//! contract, its settlement, what its seeker refuses).
//!
//! Real Kafka semantics - consumer groups, partitions, committed positions across restarts,
//! transactions and the exactly-once pipeline - live in `tests/integration_rdkafka.rs` against a
//! live cluster.

#![cfg(feature = "testing")]

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::codec::{Codec as _, DefaultCodec};
use ruststream::nonzero;
use ruststream::runtime::{
    AppInfo, Ctx, DefaultSlot, HandlerOutcome, Out, Reply, RustStream, SubscriberSettings as _,
};
use ruststream::subscriber;
use ruststream::testing::{TestApp, TestableBroker as _, expect_published};
use ruststream::{
    Broker, ConnectedBroker, DescribeServer, HeaderMap, IncomingMessage, OutSlot, Outgoing,
    OutgoingMessage, Partitioned, PublishPolicy as _, Publisher, Seeker as _, Subscriber,
    TransactionalPublisher,
};
use ruststream_rdkafka::context::keys::{Partition, Position, SeekHandle};
use ruststream_rdkafka::context::{KafkaBatchContext, KafkaContext};
use ruststream_rdkafka::testing::{ConnectedKafkaTestBroker, KafkaTestBroker, KafkaTestMessage};
use ruststream_rdkafka::{
    KafkaError, KafkaPosition, KafkaPublish, KafkaTopic, PARTITION_KEY_HEADER, PartitionLanes,
};
use serde::{Deserialize, Serialize};

const WAIT: Duration = Duration::from_secs(1);

/// The in-process ladder every test starts from: synchronous construction, then the consuming
/// `connect`, exactly like the real broker.
async fn connected() -> ConnectedKafkaTestBroker {
    KafkaTestBroker::new().connect().await.expect("connect")
}

async fn next_payload<S>(stream: &mut S) -> Vec<u8>
where
    S: Stream<Item = Result<KafkaTestMessage, KafkaError>> + Unpin,
{
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok");
    let payload = msg.payload().to_vec();
    msg.ack().await.expect("ack");
    payload
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pub_sub_round_trip_through_broker_traits() {
    let broker = connected().await;

    let mut subscriber = broker.subscribe_with("orders").await.expect("subscribe");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("orders", b"o1"))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    assert_eq!(next_payload(&mut stream).await, b"o1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_topic_name_is_rejected() {
    let broker = connected().await;

    let subscribe_err = broker
        .subscribe_with("")
        .await
        .expect_err("empty subscribe");
    assert!(matches!(subscribe_err, KafkaError::InvalidOptions(_)));

    let publish_err = broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("", b"x"))
        .await
        .expect_err("empty publish");
    assert!(matches!(publish_err, KafkaError::InvalidOptions(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topics_are_isolated() {
    let broker = connected().await;
    let mut orders = broker.subscribe_with("orders").await.expect("subscribe");
    let mut payments = broker.subscribe_with("payments").await.expect("subscribe");

    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("orders", b"o1"))
        .await
        .expect("publish");

    let mut orders_stream = Box::pin(orders.stream());
    assert_eq!(next_payload(&mut orders_stream).await, b"o1");

    let mut payments_stream = Box::pin(payments.stream());
    let silence = tokio::time::timeout(Duration::from_millis(100), payments_stream.next()).await;
    assert!(silence.is_err(), "other topics must stay silent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_requeue_redelivers_and_drop_drops() {
    let broker = connected().await;
    let mut subscriber = broker.subscribe_with("retry").await.expect("subscribe");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("retry", b"again"))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let first = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("next")
        .expect("ok");
    first.nack(true).await.expect("requeue");

    let second = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("redelivery")
        .expect("next")
        .expect("ok");
    assert_eq!(second.payload(), b"again");
    second.nack(false).await.expect("drop");

    let silence = tokio::time::timeout(Duration::from_millis(100), stream.next()).await;
    assert!(silence.is_err(), "nack(false) must not redeliver");
}

/// Publishes one keyed and one keyless record to `topic`.
async fn publish_keyed_pair(broker: &ConnectedKafkaTestBroker, topic: &str) {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert(PARTITION_KEY_HEADER, "k-1");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(topic, b"{}").with_headers(headers))
        .await
        .expect("publish");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(topic, b"plain"))
        .await
        .expect("publish");
}

// `partition_key` is the keyed-lane key on both brokers, not the record key: the real
// subscriber resolves it from the descriptor's `LaneKey`, and so must this one, or a
// `workers(n, by_key)` handler lanes differently in process than it does on a cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_propagate_and_lanes_follow_the_descriptor_lane_key() {
    let broker = connected().await;
    let mut subscriber = broker.subscribe_with("keyed").await.expect("subscribe");
    publish_keyed_pair(&broker, "keyed").await;

    let mut stream = Box::pin(subscriber.stream());
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("next")
        .expect("ok");
    assert_eq!(
        msg.headers().get_str("content-type"),
        Some("application/json")
    );
    // The record key itself is still reachable, under its own name.
    assert_eq!(msg.key(), Some(b"k-1".as_slice()));
    // Under the default `LaneKey::Partition` the lane is the source partition, which this
    // transport numbers zero for every topic - so a partition's records share one lane, as they
    // do on a single-partition topic upstream.
    assert_eq!(Partitioned::partition_key(&msg), Some(b"0".as_slice()));
    assert_eq!(IncomingMessage::partition_key(&msg), Some(b"0".as_slice()));
    msg.ack().await.expect("ack");

    // A keyless record shares that lane too, which is exactly why partition lanes keep a
    // partition's order for keyless traffic.
    let keyless = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("next")
        .expect("ok");
    assert_eq!(Partitioned::partition_key(&keyless), Some(b"0".as_slice()));
    keyless.ack().await.expect("ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_key_lane_descriptor_lanes_by_the_record_key() {
    use ruststream::SubscriptionSource as _;
    use ruststream_rdkafka::LaneKey;

    let broker = connected().await;
    let mut subscriber = KafkaTopic::new("keyed-lanes")
        .lane_key(LaneKey::RecordKey)
        .subscribe(&broker)
        .await
        .expect("subscribe");
    publish_keyed_pair(&broker, "keyed-lanes").await;

    let mut stream = Box::pin(subscriber.stream());
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("next")
        .expect("ok");
    assert_eq!(Partitioned::partition_key(&msg), Some(b"k-1".as_slice()));
    msg.ack().await.expect("ack");

    // Keyless deliveries carry no lane key under this mode, so they rotate across lanes - the
    // ordering trade-off `LaneKey::RecordKey` documents.
    let keyless = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("next")
        .expect("ok");
    assert!(Partitioned::partition_key(&keyless).is_none());
    keyless.ack().await.expect("ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_server_reports_in_process_kafka() {
    // `DescribeServer` describes the configuration, so it sits on the unconnected form.
    let spec = KafkaTestBroker::new().describe_server();
    assert_eq!(spec.protocol, "kafka");
    assert!(spec.host.is_none(), "the in-process broker has no host");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_log_observes_every_publish() {
    let broker = connected().await;
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("audit", b"first"))
        .await
        .expect("publish");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("audit", b"second"))
        .await
        .expect("publish");

    let observed = expect_published(&broker, "audit", 2, WAIT).await;
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].payload(), b"first");
    assert_eq!(observed[1].payload(), b"second");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_can_be_reentered_without_losing_deliveries() {
    let broker = connected().await;
    let mut subscriber = broker.subscribe_with("reenter").await.expect("subscribe");

    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("reenter", b"one"))
        .await
        .expect("publish");
    {
        let mut stream = Box::pin(subscriber.stream());
        assert_eq!(next_payload(&mut stream).await, b"one");
    }

    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("reenter", b"two"))
        .await
        .expect("publish");
    let mut stream = Box::pin(subscriber.stream());
    assert_eq!(next_payload(&mut stream).await, b"two");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_topic_descriptor_mounts_on_the_test_broker() {
    use ruststream::SubscriptionSource as _;

    let broker = connected().await;
    let def = KafkaTopic::new("orders").and_topic("cancellations");
    let mut subscriber = def.subscribe(&broker).await.expect("subscribe");

    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("orders", b"o1"))
        .await
        .expect("publish");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("cancellations", b"c1"))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let mut payloads = vec![
        next_payload(&mut stream).await,
        next_payload(&mut stream).await,
    ];
    payloads.sort();
    assert_eq!(payloads, vec![b"c1".to_vec(), b"o1".to_vec()]);

    // Patterns are real-cluster behavior: the exact-name router refuses them loudly.
    let err = KafkaTopic::pattern("^orders\\..*")
        .subscribe(&broker)
        .await
        .expect_err("patterns must be rejected in-process");
    assert!(matches!(err, KafkaError::InvalidOptions(_)));
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn ack_order(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

// The descriptor form must mount against the test broker through the testing-gated
// `SubscriptionSource<ConnectedKafkaTestBroker>` impl on `KafkaTopic`.
#[subscriber(KafkaTopic::new("payments"))]
async fn ack_payment(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// Counts how many times the retry handler ran, so the test can wire it as typed app state.
#[derive(Clone, Default)]
struct Attempts(Arc<AtomicUsize>);

#[subscriber(KafkaTopic::new("retry"))]
async fn retry_then_ack(order: &Order, ctx: &mut Context<'_, (), Attempts>) -> HandlerOutcome {
    let _ = order;
    // Requeue once, then acknowledge: exercises the `nack(requeue = true)` -> `enqueued`
    // re-count balanced against the delivery's `Drop` -> `consumed` decrement.
    if ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
    }
}

// The harness installs its coordinator into `KafkaTestBroker`, so `publish` must drive the
// in-process reaction to quiescence (every `enqueued` balanced by a `consumed`) before
// returning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_drives_kafka_test_broker_to_quiescence() {
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(KafkaTestBroker::new(), |b| {
            b.include(ack_order);
            b.include(ack_payment);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaTestBroker>()
        .publish("orders", &Order { id: 1 })
        .await
        .expect("publish must drive the reaction to quiescence");
    tb.broker::<KafkaTestBroker>()
        .publish("payments", &Order { id: 2 })
        .await
        .expect("publish must drive the descriptor-mounted reaction to quiescence");

    tb.broker::<KafkaTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
    tb.broker::<KafkaTestBroker>()
        .subscriber("payments")
        .assert_called_once()
        .with(&Order { id: 2 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

// A requeue re-enqueues a fresh delivery, so the harness must still reach quiescence: the
// second delivery's ack balances the count. The handler is called exactly twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_requeue_stays_balanced() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(|()| async { Ok::<_, Infallible>(Attempts::default()) })
        .with_broker(KafkaTestBroker::new(), |b| {
            b.include(retry_then_ack);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaTestBroker>()
        .publish("retry", &Order { id: 7 })
        .await
        .expect("publish must drive the requeue reaction to quiescence");

    tb.broker::<KafkaTestBroker>()
        .subscriber("retry")
        .assert_called(2)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

#[derive(Debug, Serialize, Deserialize)]
struct PlanOrder {
    id: u64,
}

#[derive(Debug, Serialize)]
struct PlanItem {
    order_id: u64,
}

#[subscriber("plan-orders", publish("work-items"))]
async fn plan(order: &PlanOrder) -> PlanItem {
    PlanItem { order_id: order.id }
}

#[subscriber("keyed-orders", publish("keyed-items"))]
async fn plan_keyed(order: &PlanOrder) -> PlanItem {
    PlanItem { order_id: order.id }
}

/// Stamps the reply with a record key, standing in for a handler that picked its placement.
struct KeyStamp;

impl<C> ruststream::runtime::PublishTransform<C> for KeyStamp {
    fn apply(
        &self,
        out: &mut ruststream::runtime::Outgoing<'_>,
        _cx: &ruststream::runtime::PublishContext<'_, C>,
    ) {
        out.headers_mut().insert(PARTITION_KEY_HEADER, "tenant-1");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_robin_stamps_cycling_partitions() {
    use ruststream_rdkafka::{PARTITION_HEADER, RoundRobin};

    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(KafkaTestBroker::new(), |b| {
            b.include(plan)
                .out(Reply, KafkaPublish::default())
                .transform(RoundRobin::partitions(2));
        });
    let tb = TestApp::start(app).await.expect("start");

    for id in 0..4 {
        tb.broker::<KafkaTestBroker>()
            .publish("plan-orders", &PlanOrder { id })
            .await
            .expect("publish");
    }

    let published = tb
        .broker::<KafkaTestBroker>()
        .published::<PlanItem>("work-items");
    let stamped: Vec<String> = published
        .messages()
        .iter()
        .map(|msg| {
            msg.headers()
                .get_str(PARTITION_HEADER)
                .expect("stamped partition")
                .to_owned()
        })
        .collect();
    assert_eq!(
        stamped,
        ["0", "1", "0", "1"],
        "the cycle targets one partition per message",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_robin_leaves_keyed_replies_alone() {
    use ruststream_rdkafka::{PARTITION_HEADER, RoundRobin};

    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(KafkaTestBroker::new(), |b| {
            // KeyStamp runs first (added first): the reply is keyed by the time RoundRobin
            // sees it, so the cycle must not override the placement the key implies.
            b.include(plan_keyed)
                .out(Reply, KafkaPublish::default())
                .transform(KeyStamp)
                .transform(RoundRobin::partitions(2));
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaTestBroker>()
        .publish("keyed-orders", &PlanOrder { id: 1 })
        .await
        .expect("publish");

    let published = tb
        .broker::<KafkaTestBroker>()
        .published::<PlanItem>("keyed-items");
    let messages = published.messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0].headers().get_str(PARTITION_KEY_HEADER),
        Some("tenant-1")
    );
    assert!(
        messages[0].headers().get(PARTITION_HEADER).is_none(),
        "a keyed reply keeps its key-implied placement",
    );
}

#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "slot-work-items")]
struct SlotItem {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(SlotItem)]
struct Work;

// A publisher-shaped slot: the handler sends through the slot entry itself, so the harness
// attributes the publish to the marker. This is the near side of the capture boundary that
// `PartitionLanes` sits on the far side of - a lane hands out a publisher of its own, and what
// that publisher sends reaches the broker's publish log without a slot record.
#[subscriber("slot-orders")]
async fn plan_through_slot(
    order: &PlanOrder,
    Out(out): Out<impl Publisher, Work>,
) -> HandlerOutcome {
    if out
        .message(&SlotItem { order_id: order.id })
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publisher_shaped_slot_is_captured_against_its_marker() {
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(KafkaTestBroker::new(), |b| {
            b.include(plan_through_slot)
                .out(Work, KafkaPublish::default())
                .build();
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaTestBroker>()
        .publish("slot-orders", &PlanOrder { id: 5 })
        .await
        .expect("publish");

    // Through the slot: recorded against the marker, and visible on the wire.
    tb.out::<Work>().assert_called_once();
    tb.broker::<KafkaTestBroker>()
        .published::<SlotItem>("slot-work-items")
        .assert_called_once();

    tb.shutdown().await.expect("shutdown");
}

// ------------------------------------------------------------------ repositioning a service

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Job {
    id: u64,
}

/// The handler's own rewind budget: one replay of a stuck record, then let it through. A service
/// spends a budget like this for real, and holding it in typed app state is what keeps the
/// handler under test a plain handler.
#[derive(Clone, Default)]
struct Rewinds(Arc<AtomicUsize>);

/// Reads both delivery-context keys the seek contract publishes: `Position` names where this
/// record sits, and `SeekHandle` moves the subscription there.
#[subscriber(KafkaTopic::new("seek-jobs"))]
async fn rewind_stuck_job(
    job: &Job,
    ctx: &mut Context<'_, KafkaContext, Rewinds>,
    Ctx(here): Ctx<Position>,
    Ctx(seeker): Ctx<SeekHandle>,
) -> HandlerOutcome {
    if job.id == 1 && ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0 {
        // The delivery's own coordinates: seeking to them redelivers exactly this record.
        if seeker.seek(here).await.is_err() {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_replays_its_own_delivery_position_through_the_context() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(|()| async { Ok::<_, Infallible>(Rewinds::default()) })
        .with_broker(KafkaTestBroker::new(), |b| {
            b.include(rewind_stuck_job);
        });
    let tb = TestApp::start(app).await.expect("start");

    for id in 0..3 {
        tb.broker::<KafkaTestBroker>()
            .publish("seek-jobs", &Job { id })
            .await
            .expect("publish drives the reaction, replay included, to quiescence");
    }

    // Job 1 rewound to itself, so it and everything behind it on the log came back once.
    let seen: Vec<u64> = tb
        .broker::<KafkaTestBroker>()
        .subscriber("seek-jobs")
        .received::<Job>()
        .into_iter()
        .map(|job| job.id)
        .collect();
    assert_eq!(
        seen,
        vec![0, 1, 1, 2],
        "seeking to a delivery's own position must redeliver it and the log suffix behind it",
    );
    tb.broker::<KafkaTestBroker>()
        .subscriber("seek-jobs")
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

/// A batch body gets the subscription-scoped context: the same `SeekHandle` key and no position,
/// because a batch spans many records. Where to resume rides the elements, and the budget bounds
/// the replay the way a service's would.
#[subscriber(KafkaTopic::new("seek-batches"))]
async fn drain_batches(
    cursors: &[Cursor],
    ctx: &mut Context<'_, KafkaBatchContext, Rewinds>,
) -> HandlerOutcome {
    let resume_at = cursors.iter().find_map(|entry| entry.resume_at);
    if let Some(offset) = resume_at
        && ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0
        && ctx
            .context(SeekHandle)
            .seek(KafkaPosition::offset(0, offset))
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The producer's cursor contract: an element carrying `resume_at` asks the consumer to
/// reposition the subscription there once the batch is settled.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Cursor {
    id: u64,
    resume_at: Option<i64>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_body_repositions_through_its_subscription_context() {
    let broker = KafkaTestBroker::new();
    let seeded = broker.clone().connect().await.expect("connect");
    // The whole run is in the log before the subscription opens, so the opening replay is what
    // the body batches over. The marker asks to resume from offset 0, so whatever the window
    // makes of the run, the record the target names is delivered again - and exactly once more,
    // because the budget is spent by then.
    for (id, resume_at) in [(0, Some(0)), (1, None)] {
        seeded
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(
                "seek-batches",
                DefaultCodec::default()
                    .encode(&Cursor { id, resume_at })
                    .expect("serializable")
                    .as_ref(),
            ))
            .await
            .expect("seed");
    }

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(|()| async { Ok::<_, Infallible>(Rewinds::default()) })
        .with_broker(broker, |b| {
            // The mount site names the batch size; the transport honours it, so a batch holds at
            // most that many records and however few it had ready.
            b.include(
                drain_batches
                    .start_at(KafkaPosition::earliest())
                    .batch(nonzero!(8)),
            );
        });
    let tb = TestApp::start(app).await.expect("start");
    tb.settle().await.expect("the batch and its replay settle");

    let seen: Vec<u64> = tb
        .broker::<KafkaTestBroker>()
        .subscriber("seek-batches")
        .received::<Cursor>()
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    // How the run is split into batches is the window's business, so the assertion is on the
    // reposition itself: the sought record came back, once, and the rest of the log kept flowing.
    assert_eq!(
        seen.iter().filter(|id| **id == 0).count(),
        2,
        "the batch's reposition must replay the record its elements named, got {seen:?}",
    );
    assert!(
        seen.contains(&1),
        "the log behind the target must keep flowing, got {seen:?}",
    );

    tb.shutdown().await.expect("shutdown");
}

/// Batches a replayed log, so the batches the transport builds are the only thing under test.
#[subscriber(KafkaTopic::new("batch-sizes"), start_at(KafkaPosition::earliest()))]
async fn count_batches(jobs: &[Job]) -> HandlerOutcome {
    let _ = jobs;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_transport_cuts_batches_at_the_size_the_mount_named() {
    let broker = KafkaTestBroker::new();
    let seeded = broker.clone().connect().await.expect("connect");
    // The whole run is on the log before the subscription opens, so the replay hands the
    // transport more than one batch's worth at once - which is what a batch size has to cut.
    for id in 0..5u64 {
        seeded
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(
                "batch-sizes",
                DefaultCodec::default()
                    .encode(&Job { id })
                    .expect("serializable")
                    .as_ref(),
            ))
            .await
            .expect("seed");
    }

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(broker, |b| b.include(count_batches.batch(nonzero!(2))));
    let tb = TestApp::start(app).await.expect("start");
    tb.settle().await.expect("the replayed batches settle");

    tb.broker::<KafkaTestBroker>()
        .subscriber("batch-sizes")
        // Two, two, then the remainder: never more than the mount site asked for.
        .assert_batch_sizes(&[2, 2, 1])
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

/// Opens at a fixed log position on every startup, whatever was published before.
#[subscriber(KafkaTopic::new("audit"), start_at(KafkaPosition::earliest()))]
async fn replay_audit(job: &Job) -> HandlerOutcome {
    let _ = job;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_at_opens_a_subscription_on_the_retained_log() {
    let broker = KafkaTestBroker::new();
    let seeded = broker.clone().connect().await.expect("connect");
    // Published before the app exists: only the start position makes these visible.
    for id in 0..2 {
        seeded
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(
                "audit",
                DefaultCodec::default()
                    .encode(&Job { id })
                    .expect("serializable")
                    .as_ref(),
            ))
            .await
            .expect("seed");
    }

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(replay_audit);
    });
    let tb = TestApp::start(app).await.expect("start");
    tb.settle().await.expect("the opening replay settles");

    let seen: Vec<u64> = tb
        .broker::<KafkaTestBroker>()
        .subscriber("audit")
        .received::<Job>()
        .into_iter()
        .map(|job| job.id)
        .collect();
    assert_eq!(
        seen,
        vec![0, 1],
        "start_at(earliest) must replay what the log held before the service started",
    );

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn positions_the_transport_cannot_resolve_are_refused() {
    use ruststream::Seekable as _;

    let broker = connected().await;
    let subscriber = broker
        .subscribe_with("seek-vocab")
        .await
        .expect("subscribe");
    let seeker = subscriber.seeker();

    // No record timestamps in-process, and one partition per topic: both are refused rather than
    // silently resolved to something the transport made up.
    let by_time = seeker
        .seek(KafkaPosition::timestamp(1_767_000_000_000))
        .await
        .expect_err("a timestamp position must be refused in-process");
    assert!(
        matches!(by_time, KafkaError::InvalidOptions(_)),
        "{by_time}"
    );

    let other_partition = seeker
        .seek(KafkaPosition::offset(3, 0))
        .await
        .expect_err("a partition other than 0 must be refused in-process");
    assert!(
        matches!(other_partition, KafkaError::InvalidOptions(_)),
        "{other_partition}",
    );

    let other_topic = seeker
        .seek(KafkaPosition::topic_offset("elsewhere", 0, 0))
        .await
        .expect_err("a topic this subscription does not read must be refused");
    assert!(
        matches!(other_topic, KafkaError::InvalidOptions(_)),
        "{other_topic}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_assignment_is_rejected_in_process() {
    use ruststream::SubscriptionSource as _;

    let broker = connected().await;

    let err = KafkaTopic::new("orders")
        .partitions([0])
        .subscribe(&broker)
        .await
        .expect_err("partitions need a real cluster");
    assert!(matches!(err, KafkaError::InvalidOptions(_)));
}

// ----------------------------------------------------------- transactions in process
//
// The stand-in reproduces the client-visible half of a Kafka transaction and nothing more: a
// commit releases what was held, an abort discards it, and misuse errors. Atomic
// `read_committed` visibility, zombie fencing and the exactly-once offset coupling are cluster
// behavior and live in `tests/integration_rdkafka.rs`.

#[derive(Debug, Serialize, Deserialize)]
struct Fanout {
    id: u64,
    items: u64,
    /// Whether the handler commits its fan-out or aborts it, so one mount covers both paths.
    commit: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
    item: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

// The routes file writes the production policy, and the handler names only the capability: this
// is the wiring a service ships, mounted unchanged on the stand-in.
#[subscriber("fanout-orders")]
async fn fan_out(
    order: &Fanout,
    Out(shipments): Out<impl TransactionalPublisher, Shipments>,
) -> HandlerOutcome {
    if shipments.begin_transaction().await.is_err() {
        return HandlerOutcome::retry();
    }
    for item in 0..order.items {
        let line = Shipment {
            order_id: order.id,
            item,
        };
        if shipments.message(&line).publish().await.is_err() {
            shipments.abort().await.ok();
            return HandlerOutcome::retry();
        }
    }
    let settled = if order.commit {
        shipments.commit().await
    } else {
        shipments.abort().await
    };
    if settled.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transactional_slot_mounts_and_publishes_only_what_it_commits() {
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(KafkaTestBroker::new(), |b| {
            b.include(fan_out)
                .out(
                    Shipments,
                    KafkaPublish::default().transactional_id("shipments-svc-1"),
                )
                .build();
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaTestBroker>()
        .publish(
            "fanout-orders",
            &Fanout {
                id: 1,
                items: 2,
                commit: false,
            },
        )
        .await
        .expect("publish");
    tb.broker::<KafkaTestBroker>()
        .published::<Shipment>("shipments")
        .assert_not_called();
    // The slot still recorded both sends: a slot records what the handler put through it, and the
    // broker's log is what actually became visible. The transaction is the difference.
    tb.out::<Shipments>().assert_called(2);

    tb.broker::<KafkaTestBroker>()
        .publish(
            "fanout-orders",
            &Fanout {
                id: 2,
                items: 2,
                commit: true,
            },
        )
        .await
        .expect("publish");
    tb.broker::<KafkaTestBroker>()
        .published::<Shipment>("shipments")
        .assert_called(2)
        .with(&Shipment {
            order_id: 2,
            item: 1,
        });

    tb.broker::<KafkaTestBroker>()
        .subscriber("fanout-orders")
        .assert_called(2)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_transaction_holds_its_publishes_until_commit() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe_with("txn-shipments")
        .await
        .expect("subscribe");
    let publisher = broker
        .transactional_publisher(KafkaPublish::default().transactional_id("txn-1"))
        .await
        .expect("transactional publisher");

    // With nothing open the publisher routes straight away, as the real one does through the
    // broker's plain producer.
    publisher
        .publish(OutgoingMessage::new("txn-shipments", b"plain"))
        .await
        .expect("publish");
    let mut stream = Box::pin(subscriber.stream());
    assert_eq!(next_payload(&mut stream).await, b"plain");

    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("txn-shipments", b"held"))
        .await
        .expect("publish");
    let silence = tokio::time::timeout(Duration::from_millis(100), stream.next()).await;
    assert!(
        silence.is_err(),
        "an open transaction must hold its publishes back",
    );

    publisher.commit().await.expect("commit");
    assert_eq!(next_payload(&mut stream).await, b"held");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aborted_transaction_routes_nothing_and_frees_the_handle() {
    let broker = connected().await;
    let publisher = broker
        .transactional_publisher(KafkaPublish::default().transactional_id("txn-abort"))
        .await
        .expect("transactional publisher");

    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("txn-dropped", b"gone"))
        .await
        .expect("publish");
    publisher.abort().await.expect("abort");
    assert!(
        broker.published("txn-dropped").is_empty(),
        "an aborted transaction must reach the transport with nothing",
    );

    // The abort released the claim, so the handle takes another transaction.
    publisher.begin_transaction().await.expect("begin again");
    publisher
        .publish(OutgoingMessage::new("txn-dropped", b"kept"))
        .await
        .expect("publish");
    publisher.commit().await.expect("commit");
    let landed: Vec<Vec<u8>> = broker
        .published("txn-dropped")
        .iter()
        .map(|msg| msg.payload().to_vec())
        .collect();
    assert_eq!(landed, vec![b"kept".to_vec()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_misuse_errors_instead_of_silently_succeeding() {
    let broker = connected().await;
    let publisher = broker
        .transactional_publisher(KafkaPublish::default().transactional_id("txn-misuse"))
        .await
        .expect("transactional publisher");

    let no_commit = publisher
        .commit()
        .await
        .expect_err("committing with nothing open must error");
    assert!(
        matches!(&no_commit, KafkaError::NoTransaction { id } if id == "txn-misuse"),
        "{no_commit}",
    );
    let no_abort = publisher
        .abort()
        .await
        .expect_err("aborting with nothing open must error");
    assert!(
        matches!(&no_abort, KafkaError::NoTransaction { id } if id == "txn-misuse"),
        "{no_abort}",
    );

    publisher.begin_transaction().await.expect("begin");
    // Clones share the handle's one transaction, as clones of the real publisher share one
    // producer: the second begin is refused rather than silently merging two flows.
    let busy = publisher
        .clone()
        .begin_transaction()
        .await
        .expect_err("a second begin must error");
    assert!(
        matches!(&busy, KafkaError::TransactionBusy { id } if id == "txn-misuse"),
        "{busy}",
    );

    // And the refused begin left the open transaction untouched.
    publisher
        .publish(OutgoingMessage::new("txn-misuse-out", b"kept"))
        .await
        .expect("publish");
    publisher.commit().await.expect("commit");
    assert_eq!(broker.published("txn-misuse-out").len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transactional_publisher_errors_after_shutdown() {
    let broker = connected().await;
    let publisher = broker
        .transactional_publisher(KafkaPublish::default().transactional_id("txn-closed"))
        .await
        .expect("transactional publisher");
    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("txn-late", b"never"))
        .await
        .expect("buffered");

    broker.shutdown().await.expect("shutdown");

    let err = publisher
        .commit()
        .await
        .expect_err("committing into a closed transport must error");
    assert!(
        matches!(&err, KafkaError::Closed { topic } if topic == "txn-closed"),
        "a transaction control call must name its transactional id, got: {err}",
    );
    let begun = publisher
        .begin_transaction()
        .await
        .expect_err("beginning on a closed transport must error");
    assert!(matches!(&begun, KafkaError::Closed { .. }), "{begun}");
    // Abort refuses too rather than reporting a local success, as the real publisher's does.
    let aborted = publisher
        .abort()
        .await
        .expect_err("aborting on a closed transport must error");
    assert!(matches!(&aborted, KafkaError::Closed { .. }), "{aborted}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_lanes_hand_out_independent_transactions() {
    let broker = connected().await;
    let lanes = KafkaPublish::default()
        .transactional_id("lanes-svc")
        .per_partition()
        .pair(&broker)
        .await
        .expect("pair");

    let p0 = lanes.for_partition(0).await.expect("lane 0");
    let p1 = lanes.for_partition(1).await.expect("lane 1");
    assert_eq!(p0.id(), "lanes-svc-p0");
    assert_eq!(p1.id(), "lanes-svc-p1");

    p0.begin_transaction().await.expect("begin p0");
    // The cache hands the same handle back rather than minting a second one, so its transaction
    // is already claimed - which is what keeps a lane's transaction the lane's own.
    let again = lanes.for_partition(0).await.expect("lane 0 again");
    let busy = again
        .begin_transaction()
        .await
        .expect_err("a cached lane is the same handle");
    assert!(
        matches!(&busy, KafkaError::TransactionBusy { id } if id == "lanes-svc-p0"),
        "{busy}",
    );

    // Two lanes hold open transactions at once, which one shared publisher could not.
    p1.begin_transaction().await.expect("begin p1");
    p0.publish(OutgoingMessage::new("lane-out", b"p0"))
        .await
        .expect("publish p0");
    p1.publish(OutgoingMessage::new("lane-out", b"p1"))
        .await
        .expect("publish p1");
    p0.commit().await.expect("commit p0");
    p1.abort().await.expect("abort p1");

    let landed: Vec<Vec<u8>> = broker
        .published("lane-out")
        .iter()
        .map(|msg| msg.payload().to_vec())
        .collect();
    assert_eq!(
        landed,
        vec![b"p0".to_vec()],
        "the aborted lane must leave nothing behind",
    );
}

/// Reads the delivery's source partition and publishes inside that lane's own transaction, the
/// production shape of a `workers(n)` transactional handler.
#[subscriber("lane-orders")]
async fn bill_lane(
    order: &PlanOrder,
    Ctx(partition): Ctx<Partition>,
    Out(lanes): Out<impl PartitionLanes>,
) -> HandlerOutcome {
    let Ok(publisher) = lanes.for_partition(partition).await else {
        return HandlerOutcome::retry();
    };
    if publisher.begin_transaction().await.is_err() {
        return HandlerOutcome::retry();
    }
    let payload = DefaultCodec::default()
        .encode(&PlanItem { order_id: order.id })
        .expect("serializable");
    if publisher
        .publish(OutgoingMessage::new("lane-items", payload.as_ref()))
        .await
        .is_err()
    {
        publisher.abort().await.ok();
        return HandlerOutcome::retry();
    }
    if publisher.commit().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lanes_slot_mounts_and_publishes_through_its_partition_transaction() {
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(KafkaTestBroker::new(), |b| {
            b.include(bill_lane)
                .out(
                    DefaultSlot,
                    KafkaPublish::default()
                        .transactional_id("lane-svc")
                        .per_partition(),
                )
                .build();
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaTestBroker>()
        .publish("lane-orders", &PlanOrder { id: 9 })
        .await
        .expect("publish");

    // A lane publishes through a publisher of its own, so its traffic lands in the broker's log
    // rather than in the slot's record - the capture boundary `PartitionLanes` documents.
    tb.broker::<KafkaTestBroker>()
        .published::<PlanItem>("lane-items")
        .assert_called_once();
    tb.broker::<KafkaTestBroker>()
        .subscriber("lane-orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_errors_after_shutdown() {
    let broker = connected().await;
    let publisher = broker.publisher(KafkaPublish::default());
    publisher
        .publish(OutgoingMessage::new("orders", b"before"))
        .await
        .expect("publish before shutdown");

    broker.shutdown().await.expect("shutdown");

    let err = publisher
        .publish(OutgoingMessage::new("orders", b"after"))
        .await
        .expect_err("publishing through a handle aliasing a closed transport must error");
    assert!(
        matches!(&err, KafkaError::Closed { topic } if topic == "orders"),
        "the error must name the topic it could not reach, got: {err}",
    );
}
