//! Integration tests for the in-process Kafka test broker.
//!
//! Most cases drive the public surface (`KafkaTestBroker`, `KafkaTestPublisher`,
//! `KafkaTestSubscriber`) directly, to keep failures localised; the `TestApp`-driven cases at
//! the end exercise the `TestableBroker` quiescence wiring (coordinator install,
//! `enqueued`/`consumed`) through the harness. Real Kafka semantics (groups, partitions,
//! committed positions, start offsets) live in `tests/integration_rdkafka.rs` against a live
//! cluster.

#![cfg(feature = "testing")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream::testing::{TestApp, expect_published};
use ruststream::{
    Broker, ConnectedBroker, DescribeServer, HeaderMap, IncomingMessage, OutgoingMessage,
    Partitioned, Publisher, Subscriber,
};
use ruststream_rdkafka::testing::{ConnectedKafkaTestBroker, KafkaTestBroker, KafkaTestMessage};
use ruststream_rdkafka::{KafkaError, KafkaPublish, KafkaTopic, PARTITION_KEY_HEADER};
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
async fn ack_order(order: &Order) -> HandlerResult {
    let _ = order;
    HandlerResult::Ack
}

// The descriptor form must mount against the test broker through the testing-gated
// `SubscriptionSource<ConnectedKafkaTestBroker>` impl on `KafkaTopic`.
#[subscriber(KafkaTopic::new("payments"))]
async fn ack_payment(order: &Order) -> HandlerResult {
    let _ = order;
    HandlerResult::Ack
}

/// Counts how many times the retry handler ran, so the test can wire it as typed app state.
#[derive(Clone, Default)]
struct Attempts(Arc<AtomicUsize>);

#[subscriber(KafkaTopic::new("retry"))]
async fn retry_then_ack(order: &Order, ctx: &mut Context<'_, (), Attempts>) -> HandlerResult {
    let _ = order;
    // Requeue once, then acknowledge: exercises the `nack(requeue = true)` -> `enqueued`
    // re-count balanced against the delivery's `Drop` -> `consumed` decrement.
    if ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerResult::retry()
    } else {
        HandlerResult::Ack
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
        .settled(HandlerResult::Ack);
    tb.broker::<KafkaTestBroker>()
        .subscriber("payments")
        .assert_called_once()
        .with(&Order { id: 2 })
        .settled(HandlerResult::Ack);

    tb.shutdown().await.expect("shutdown");
}

// A requeue re-enqueues a fresh delivery, so the harness must still reach quiescence: the
// second delivery's ack balances the count. The handler is called exactly twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_requeue_stays_balanced() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(|()| async { Ok::<_, std::convert::Infallible>(Attempts::default()) })
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
        .settled(HandlerResult::Ack);

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
