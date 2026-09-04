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
use ruststream::runtime::{AppInfo, Ctx, HandlerOutcome, Out, RustStream, SubscriberSettings as _};
use ruststream::subscriber;
use ruststream::testing::{TestApp, expect_published};
use ruststream::{
    Broker, ConnectedBroker, DescribeServer, HeaderMap, IncomingMessage, OutSlot, Outgoing,
    OutgoingMessage, Partitioned, Publisher, Seeker as _, Subscriber,
};
use ruststream_rdkafka::context::keys::{Position, SeekHandle};
use ruststream_rdkafka::context::{KafkaBatchContext, KafkaContext};
use ruststream_rdkafka::testing::{ConnectedKafkaTestBroker, KafkaTestBroker, KafkaTestMessage};
use ruststream_rdkafka::{
    KafkaError, KafkaPosition, KafkaPublish, KafkaTopic, PARTITION_KEY_HEADER,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_and_partition_key_propagate() {
    let broker = connected().await;
    let mut subscriber = broker.subscribe_with("keyed").await.expect("subscribe");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert(PARTITION_KEY_HEADER, "k-1");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("keyed", b"{}").with_headers(headers))
        .await
        .expect("publish");

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
    assert_eq!(Partitioned::partition_key(&msg), Some(b"k-1".as_slice()));
    assert_eq!(
        IncomingMessage::partition_key(&msg),
        Some(b"k-1".as_slice())
    );
    msg.ack().await.expect("ack");

    // And a keyless message reports no partition key.
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("keyed", b"plain"))
        .await
        .expect("publish");
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
    use ruststream::runtime::TypedPublisher;
    use ruststream_rdkafka::{PARTITION_HEADER, RoundRobin};

    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(KafkaTestBroker::new(), |b| {
            let work_items =
                TypedPublisher::new(KafkaPublish::default()).transform(RoundRobin::partitions(2));
            b.include(plan).publisher(work_items);
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
    use ruststream::runtime::TypedPublisher;
    use ruststream_rdkafka::{PARTITION_HEADER, RoundRobin};

    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(KafkaTestBroker::new(), |b| {
            // KeyStamp runs first (added first): the reply is keyed by the time RoundRobin
            // sees it, so the cycle must not override the placement the key implies.
            let keyed_items = TypedPublisher::new(KafkaPublish::default())
                .transform(KeyStamp)
                .transform(RoundRobin::partitions(2));
            b.include(plan_keyed).publisher(keyed_items);
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

/// A page body gets the subscription-scoped context: the same `SeekHandle` key and no position,
/// because a page spans many records. Where to resume rides the elements, and the budget bounds
/// the replay the way a service's would.
#[subscriber(KafkaTopic::new("seek-pages"))]
async fn drain_pages(
    page: &[Cursor],
    ctx: &mut Context<'_, KafkaBatchContext, Rewinds>,
) -> HandlerOutcome {
    let resume_at = page.iter().find_map(|entry| entry.resume_at);
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
/// reposition the subscription there once the page is settled.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Cursor {
    id: u64,
    resume_at: Option<i64>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_body_repositions_through_its_subscription_context() {
    let broker = KafkaTestBroker::new();
    let seeded = broker.clone().connect().await.expect("connect");
    // The whole run is in the log before the subscription opens, so the opening replay is what
    // the body pages over. The marker asks to resume from offset 0, so whatever the window makes
    // of the run, the record the target names is delivered again - and exactly once more,
    // because the budget is spent by then.
    for (id, resume_at) in [(0, Some(0)), (1, None)] {
        seeded
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(
                "seek-pages",
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
            // The mount site names the page size; the transport honours it, so a page holds at
            // most that many records and however few it had ready.
            b.include(
                drain_pages
                    .start_at(KafkaPosition::earliest())
                    .batch(nonzero!(8)),
            );
        });
    let tb = TestApp::start(app).await.expect("start");
    tb.settle().await.expect("the page and its replay settle");

    let seen: Vec<u64> = tb
        .broker::<KafkaTestBroker>()
        .subscriber("seek-pages")
        .received::<Cursor>()
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    // How the run is split into pages is the window's business, so the assertion is on the
    // reposition itself: the sought record came back, once, and the rest of the log kept flowing.
    assert_eq!(
        seen.iter().filter(|id| **id == 0).count(),
        2,
        "the page's reposition must replay the record its elements named, got {seen:?}",
    );
    assert!(
        seen.contains(&1),
        "the log behind the target must keep flowing, got {seen:?}",
    );

    tb.shutdown().await.expect("shutdown");
}

/// Pages a replayed log, so the pages the transport builds are the only thing under test.
#[subscriber(KafkaTopic::new("page-sizes"), start_at(KafkaPosition::earliest()))]
async fn count_pages(page: &[Job]) -> HandlerOutcome {
    let _ = page;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_transport_cuts_pages_at_the_size_the_mount_named() {
    let broker = KafkaTestBroker::new();
    let seeded = broker.clone().connect().await.expect("connect");
    // The whole run is on the log before the subscription opens, so the replay hands the
    // transport more than one page's worth at once - which is what a page size has to cut.
    for id in 0..5u64 {
        seeded
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(
                "page-sizes",
                DefaultCodec::default()
                    .encode(&Job { id })
                    .expect("serializable")
                    .as_ref(),
            ))
            .await
            .expect("seed");
    }

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(broker, |b| b.include(count_pages.batch(nonzero!(2))));
    let tb = TestApp::start(app).await.expect("start");
    tb.settle().await.expect("the replayed pages settle");

    tb.broker::<KafkaTestBroker>()
        .subscriber("page-sizes")
        // Two, two, then the remainder: never more than the mount site asked for.
        .assert_page_sizes(&[2, 2, 1])
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
