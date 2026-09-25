//! Services under `TestApp`: the production app on `KafkaBroker`, connected in process.
//!
//! Each test builds its app the way `main` would, hands it to `TestApp::start`, publishes the
//! input through the harness and asserts on what the handlers received, how they settled and
//! what they published. The broker is addressed by its production type throughout.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::nonzero;
use ruststream::runtime::{
    AppInfo, ContextKind, ForReply, HandlerOutcome, Out, Outgoing as OutgoingRecord,
    PublishContext, PublishTransform, RETRY_COUNT_HEADER, Reads, Reply, RustStream,
};
use ruststream::subscriber;
use ruststream::testing::{Outcome, TestApp};
use ruststream::{OutSlot, Outgoing, Publisher};
use ruststream_rdkafka::context::KafkaContext;
use ruststream_rdkafka::context::keys::Topic;
use ruststream_rdkafka::{
    Commit, KafkaBroker, KafkaOptions, KafkaPublish, KafkaPublishSteps as _, KafkaTopic,
    KafkaTopics, PARTITION_KEY_HEADER, RoundRobin, ToSourceTopic,
};
use serde::{Deserialize, Serialize};

/// The broker every app here runs on, configured as a service configures it.
fn broker() -> KafkaBroker {
    KafkaBroker::new(["kafka:9092"]).default_group("svc")
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn ack_order(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(KafkaTopic::new("payments"))]
async fn ack_payment(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_drives_the_reaction_to_a_standstill() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(ack_order);
        b.include(ack_payment);
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<KafkaBroker>()
        .message(&Order { id: 2 })
        .to("payments")
        .publish()
        .await
        .expect("publish");

    tb.broker::<KafkaBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
    tb.broker::<KafkaBroker>()
        .subscriber("payments")
        .assert_called_once()
        .with(&Order { id: 2 })
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown");
}

// ------------------------------------------------------------------------- consumer groups

#[subscriber(KafkaTopic::new("shared").group("billing"))]
async fn bill(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(KafkaTopic::new("shared").group("audit"))]
async fn audit(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// Two replicas of one group read a record once between them; another group reads its own copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_group_reads_a_record_once() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(bill);
        b.include(bill);
        b.include(audit);
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&Order { id: 1 })
        .to("shared")
        .publish()
        .await
        .expect("publish");

    // All three subscriptions are named after the topic: two calls are one per group.
    tb.broker::<KafkaBroker>()
        .subscriber("shared")
        .assert_called(2);
    tb.shutdown().await.expect("shutdown");
}

#[subscriber(KafkaTopics::pattern("^orders\\..*").group("regions"))]
async fn regional(order: &Order, ctx: &mut Context<'_, KafkaContext>) -> HandlerOutcome {
    let _ = (order, ctx);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pattern_subscription_reads_every_topic_it_matches() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        // A topic set addresses no retry copy of its own, so the mount site says where one goes.
        b.include(regional)
            .out_retry(KafkaPublish::default())
            .transform(ToSourceTopic);
    });
    let tb = TestApp::start(app).await.expect("start");

    for (id, topic) in [(1, "orders.eu"), (2, "orders.us"), (3, "orders")] {
        tb.broker::<KafkaBroker>()
            .message(&Order { id })
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }

    let seen: Vec<u64> = tb
        .broker::<KafkaBroker>()
        .subscriber("^orders\\..*")
        .received::<Order>()
        .into_iter()
        .map(|order| order.id)
        .collect();
    assert_eq!(
        seen,
        [1, 2],
        "the pattern matches the regional topics and not `orders`"
    );
    tb.shutdown().await.expect("shutdown");
}

/// Where a handler says its record came from.
#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct Origin {
    topic: String,
}

#[subscriber(KafkaTopics::new(["origin-eu", "origin-us"]).group("origins"), publish("origins"))]
async fn name_a_listed_topic(order: &Order, ctx: &mut Context<'_, KafkaContext>) -> Origin {
    let _ = order;
    Origin {
        topic: ctx.context(Topic).to_string(),
    }
}

#[subscriber(KafkaTopic::new("origin-one").group("origins"), publish("origins"))]
async fn name_the_one_topic(order: &Order, ctx: &mut Context<'_, KafkaContext>) -> Origin {
    let _ = order;
    Origin {
        topic: ctx.context(Topic).to_string(),
    }
}

/// A context names the topic its record came from, whether the subscription reads one topic or
/// several.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_context_names_the_topic_of_its_record() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        // A topic set addresses no retry copy of its own, so the mount site says where one goes.
        b.include(name_a_listed_topic)
            .out_retry(KafkaPublish::default())
            .transform(ToSourceTopic);
        b.include(name_the_one_topic);
    });
    let tb = TestApp::start(app).await.expect("start");

    for (id, topic) in [(1, "origin-eu"), (2, "origin-us"), (3, "origin-one")] {
        tb.broker::<KafkaBroker>()
            .message(&Order { id })
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }

    let mut named: Vec<String> = tb
        .broker::<KafkaBroker>()
        .published::<Origin>("origins")
        .assert_called(3)
        .decoded()
        .into_iter()
        .map(|origin| origin.topic)
        .collect();
    named.sort();
    assert_eq!(named, ["origin-eu", "origin-one", "origin-us"]);
    tb.shutdown().await.expect("shutdown");
}

// -------------------------------------------------------------------------------- retries

#[subscriber(KafkaTopic::new("retry").commit(Commit::Tracked))]
async fn retry_once(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry()
}

/// A tracked retry leaves the offset unsettled and the consumer reads on: Kafka delivers the
/// record again when the partition is next fetched from the committed offset, which is a restart
/// or a rebalance, not this session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracked_retry_waits_for_the_next_fetch_of_its_partition() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(retry_once);
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&Order { id: 7 })
        .to("retry")
        .publish()
        .await
        .expect("publish");

    tb.broker::<KafkaBroker>()
        .subscriber("retry")
        .assert_called_once()
        .settled(HandlerOutcome::retry());
    tb.shutdown().await.expect("shutdown");
}

/// How long a not-ready-yet delivery waits before it comes back.
const DEFER: Duration = Duration::from_secs(5);

/// Stamps every copy with the subscription the delivery came from.
struct DeferredStamp;

impl<C, Options> PublishTransform<ForReply<C>, Options> for DeferredStamp {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut OutgoingRecord<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut()
            .insert("x-retried-from", cx.name().to_owned());
    }
}

#[subscriber(KafkaTopic::new("deferred").commit(Commit::Tracked))]
async fn defer_then_ack(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let _ = order;
    if ctx.headers().get(RETRY_COUNT_HEADER).is_none() {
        HandlerOutcome::retry_after(DEFER)
    } else {
        HandlerOutcome::ack()
    }
}

/// Kafka has no delayed redelivery of its own, so the runtime republishes a delayed copy through
/// the retry position, and the position's transform stamps it on the way.
#[tokio::test(start_paused = true)]
async fn a_deferred_copy_travels_the_retry_position() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(defer_then_ack)
            .out_retry(KafkaPublish::default())
            .transform(DeferredStamp);
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&Order { id: 3 })
        .to("deferred")
        .publish()
        .await
        .expect("publish");
    tb.broker::<KafkaBroker>()
        .subscriber("deferred")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(DEFER));

    tb.advance(DEFER).await.expect("the delay elapses");

    assert_eq!(
        tb.broker::<KafkaBroker>().subscriber("deferred").outcomes(),
        [Outcome::Nack, Outcome::Ack],
    );
    tb.broker::<KafkaBroker>()
        .published::<Order>("deferred")
        .with_header("x-retried-from", "deferred");
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Charge {
    id: u64,
}

#[subscriber(KafkaTopic::new("charges").commit(Commit::Tracked))]
async fn defer_forever(charge: &Charge) -> HandlerOutcome {
    let _ = charge;
    HandlerOutcome::retry_after(DEFER)
}

/// Kafka counts no deliveries, so the cap counts the framework's header and the spent delivery
/// leaves through the dead-letter topic.
#[tokio::test(start_paused = true)]
async fn a_capped_registration_dead_letters_a_spent_delivery() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(defer_forever)
            .max_attempts(nonzero!(3u32))
            .dead_letter("charges.dlq");
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&Charge { id: 7 })
        .to("charges")
        .publish()
        .await
        .expect("publish");
    tb.advance(DEFER).await.expect("the first delay elapses");
    tb.advance(DEFER).await.expect("the second delay elapses");

    tb.broker::<KafkaBroker>()
        .subscriber("charges")
        .assert_called(3);
    tb.broker::<KafkaBroker>()
        .published::<Charge>("charges.dlq")
        .assert_called_once()
        .with(&Charge { id: 7 })
        .with_header(RETRY_COUNT_HEADER, "3");
}

#[subscriber(KafkaTopics::new(["refunds", "refunds.retry"]).commit(Commit::Tracked))]
async fn retry_forever(charge: &Charge) -> HandlerOutcome {
    let _ = charge;
    HandlerOutcome::retry()
}

/// A topic set addresses no copy of its own, so the mount site names the topic its copies go to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_named_retry_destination_carries_the_copies() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(retry_forever)
            .max_attempts(nonzero!(2u32))
            .dead_letter("refunds.dlq")
            .out_retry(KafkaPublish::default())
            .to("refunds.retry");
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&Charge { id: 9 })
        .to("refunds")
        .publish()
        .await
        .expect("publish");

    tb.broker::<KafkaBroker>()
        .published::<Charge>("refunds.retry")
        .assert_called_once()
        .with_header(RETRY_COUNT_HEADER, "1");
    tb.broker::<KafkaBroker>()
        .published::<Charge>("refunds.dlq")
        .assert_called_once()
        .with(&Charge { id: 9 });
}

#[subscriber(
    KafkaTopics::new(["orders-eu", "orders-us"])
        .group("orders-svc")
        .commit(Commit::Tracked)
)]
async fn regional_order(charge: &Charge, ctx: &mut Context<'_, KafkaContext>) -> HandlerOutcome {
    let _ = charge;
    if ctx.headers().get(RETRY_COUNT_HEADER).is_none() {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
    }
}

/// A copy belongs on the topic its delivery arrived on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_copy_returns_to_the_topic_it_arrived_on() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(regional_order)
            .max_attempts(nonzero!(3u32))
            .out_retry(KafkaPublish::default())
            .transform(ToSourceTopic);
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&Charge { id: 4 })
        .to("orders-eu")
        .publish()
        .await
        .expect("publish");

    tb.broker::<KafkaBroker>()
        .published::<Charge>("orders-eu")
        .assert_called(2)
        .with(&Charge { id: 4 })
        .with_header(RETRY_COUNT_HEADER, "1");
    tb.broker::<KafkaBroker>()
        .published::<Charge>("orders-us")
        .assert_not_called();
    assert_eq!(
        tb.broker::<KafkaBroker>()
            .subscriber("orders-eu,orders-us")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
    );
}

// ------------------------------------------------------------------ where replies land

#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct PlanOrder {
    id: u64,
}

#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct PlanItem {
    order_id: u64,
}

#[subscriber("keyed-orders", publish("keyed-items"))]
async fn plan_keyed(order: &PlanOrder) -> PlanItem {
    PlanItem { order_id: order.id }
}

/// Stamps the reply with a record key, standing in for a handler that picked its placement.
struct KeyStamp;

impl<K: ContextKind, Options> PublishTransform<K, Options> for KeyStamp {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut OutgoingRecord<'_>,
        _options: &mut Option<Options>,
        _cx: &K::View<'_>,
    ) {
        out.headers_mut().insert(PARTITION_KEY_HEADER, "tenant-1");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_robin_leaves_a_keyed_reply_where_its_key_sends_it() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(plan_keyed)
            .out_reply(KafkaPublish::default())
            .transform(KeyStamp)
            .transform(RoundRobin::partitions(2));
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&PlanOrder { id: 1 })
        .to("keyed-orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<KafkaBroker>()
        .published::<PlanItem>("keyed-items")
        .assert_called_once()
        .with_header(PARTITION_KEY_HEADER, "tenant-1")
        .assert_options_default();
}

#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct ReceiptRequest {
    id: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Acknowledgement {
    id: u64,
}

#[subscriber("receipt-requests", publish)]
async fn issue_receipt(req: &ReceiptRequest) -> Receipt {
    Receipt { id: req.id }
}

#[subscriber("ack-requests", publish("acknowledgements"))]
async fn acknowledge(req: &ReceiptRequest) -> Acknowledgement {
    Acknowledgement { id: req.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_reply_lands_at_the_topic_its_type_names_with_its_key() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(issue_receipt)
            .out(Reply, KafkaPublish::default())
            .transform(KeyStamp);
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&ReceiptRequest { id: 7 })
        .to("receipt-requests")
        .publish()
        .await
        .expect("publish");

    tb.broker::<KafkaBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 7 })
        .with_header(PARTITION_KEY_HEADER, "tenant-1");
    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_undeclared_reply_lands_at_the_topic_the_mount_site_names() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(acknowledge).out(Reply, KafkaPublish::default());
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&ReceiptRequest { id: 3 })
        .to("ack-requests")
        .publish()
        .await
        .expect("publish");

    tb.broker::<KafkaBroker>()
        .published::<Acknowledgement>("acknowledgements")
        .assert_called_once()
        .with(&Acknowledgement { id: 3 });
    tb.shutdown().await.expect("shutdown");
}

// ------------------------------------------------------------------ slots and settings

#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "slot-work-items")]
struct SlotItem {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(SlotItem)]
struct Work;

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
async fn a_slot_publish_is_recorded_against_its_marker_and_lands_on_the_topic() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(plan_through_slot)
            .out(Work, KafkaPublish::default())
            .build();
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&PlanOrder { id: 5 })
        .to("slot-orders")
        .publish()
        .await
        .expect("publish");

    tb.out::<Work>().assert_called_once();
    tb.broker::<KafkaBroker>()
        .published::<SlotItem>("slot-work-items")
        .assert_called_once();
    tb.shutdown().await.expect("shutdown");
}

#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "placed-items")]
struct PlacedItem {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(PlacedItem)]
struct Placement;

#[subscriber("placement-orders")]
async fn place(
    order: &PlanOrder,
    Out(out): Out<impl Publisher<Options = KafkaOptions>, Placement>,
) -> HandlerOutcome {
    let item = PlacedItem { order_id: order.id };
    if out.message(&item).publish().await.is_err()
        || out.message(&item).partition(0).publish().await.is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The slot view is where a per-record setting is read back; the cluster placed both records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partition_step_is_recorded_against_the_slot_it_left() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(place)
            .out(Placement, KafkaPublish::default())
            .build();
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&PlanOrder { id: 4 })
        .to("placement-orders")
        .publish()
        .await
        .expect("publish");

    tb.out::<Placement>()
        .assert_called(2)
        .with_options(&KafkaOptions::default().partition(0));
    tb.broker::<KafkaBroker>()
        .published::<PlacedItem>("placed-items")
        .assert_called(2);
    tb.broker::<KafkaBroker>()
        .subscriber("placement-orders")
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown");
}

#[subscriber("misplaced-orders")]
async fn misplace(
    order: &PlanOrder,
    Out(out): Out<impl Publisher<Options = KafkaOptions>, Placement>,
) -> HandlerOutcome {
    let item = PlacedItem { order_id: order.id };
    if out.message(&item).partition(3).publish().await.is_err() {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

/// A topic comes into being with one partition, so a record pinned to another is refused, as the
/// cluster refuses it: the handler sees the failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_pinned_to_a_partition_the_topic_lacks_is_refused() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(misplace)
            .out(Placement, KafkaPublish::default())
            .build();
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&PlanOrder { id: 1 })
        .to("misplaced-orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<KafkaBroker>()
        .subscriber("misplaced-orders")
        .settled(HandlerOutcome::drop());
    tb.broker::<KafkaBroker>()
        .published::<PlacedItem>("placed-items")
        .assert_not_called();
    tb.shutdown().await.expect("shutdown");
}

// ------------------------------------------------------------------------------- startup

#[derive(Debug, Deserialize)]
struct Refused {
    id: u64,
}

#[subscriber("refused-orders")]
async fn bare_refused(order: &Refused) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(KafkaTopic::new("refused-orders"))]
async fn groupless_refused(order: &Refused) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// Kafka reads a topic only through a consumer group, so a subscription that names none on a
/// broker that names no default is refused at startup, in process as on a cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_with_no_group_is_refused_at_startup() {
    for app in [
        RustStream::new(AppInfo::new("groupless", "0.1.0")).with_broker(
            KafkaBroker::new(["kafka:9092"]),
            |b| {
                b.include(bare_refused);
            },
        ),
        RustStream::new(AppInfo::new("groupless", "0.1.0")).with_broker(
            KafkaBroker::new(["kafka:9092"]),
            |b| {
                b.include(groupless_refused);
            },
        ),
    ] {
        let failed = TestApp::start(app)
            .await
            .expect_err("a subscription with no group must not start");
        let message = failed.to_string();
        assert!(message.contains("no consumer group"), "{message}");
        assert!(message.contains("default_group"), "{message}");
    }
}
