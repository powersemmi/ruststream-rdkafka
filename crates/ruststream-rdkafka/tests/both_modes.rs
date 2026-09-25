//! One test body, two modes: the production app in process under `just test`, and the same app
//! against the stand under `just test-brokers`, where `RUSTSTREAM_REQUIRE_LIVE` makes a skip a
//! failure.
//!
//! The body publishes an order its handler defers once. Kafka has no delayed redelivery of its
//! own, so the runtime republishes a copy to the topic after the delay, and the handler accepts
//! the copy. In process the delay passes on the paused clock; live it passes for real.

#![cfg(feature = "testing")]

use std::process;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use ruststream::runtime::{App, AppInfo, HandlerOutcome, RETRY_COUNT_HEADER, RustStream};
use ruststream::testing::{Outcome, TestApp};
use ruststream::{Outgoing, subscriber};
use ruststream_rdkafka::{Commit, KafkaBroker, KafkaTopic, StartOffset};
use serde::{Deserialize, Serialize};

mod live;

/// The topic the handler reads; fixed, because the subscription names it in the attribute.
const ORDERS: &str = "both-modes-orders";

/// How long the handler defers an order before it takes it.
const DEFER: Duration = Duration::from_millis(300);

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "both-modes-orders")]
struct Order {
    id: u64,
}

#[subscriber(
    KafkaTopic::new("both-modes-orders")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
)]
async fn accept_later(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let _ = order;
    if ctx.headers().get(RETRY_COUNT_HEADER).is_none() {
        HandlerOutcome::retry_after(DEFER)
    } else {
        HandlerOutcome::ack()
    }
}

/// The service's app, on the cluster at `servers` in the consumer group `group`.
fn app(servers: &str, group: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new([servers]).default_group(group),
        |b| {
            b.include(accept_later);
        },
    )
}

/// The body both modes run.
async fn a_deferred_order_is_taken_when_its_copy_comes_back(tb: TestApp<()>) {
    tb.broker::<KafkaBroker>()
        .message(&Order { id: 1 })
        .publish()
        .await
        .expect("publish");
    tb.broker::<KafkaBroker>()
        .subscriber(ORDERS)
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(DEFER));

    tb.advance(DEFER).await.expect("the delay passes");

    assert_eq!(
        tb.broker::<KafkaBroker>().subscriber(ORDERS).outcomes(),
        [Outcome::Nack, Outcome::Ack],
    );
    tb.broker::<KafkaBroker>()
        .published::<Order>(ORDERS)
        .assert_called(2)
        .with(&Order { id: 1 })
        .with_header(RETRY_COUNT_HEADER, "1");
    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn in_process() {
    let tb = TestApp::start(app("kafka:9092", "orders-svc"))
        .await
        .expect("start");
    a_deferred_order_is_taken_when_its_copy_comes_back(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live() {
    let Some(url) = live::url("KAFKA_TEST_URL") else {
        return;
    };
    // A fresh topic and a group of this run's own: nothing an earlier run left is read.
    recreate_topic(&url, ORDERS).await;
    let group = format!("both-modes-{}", process::id());
    // A group forms on the stand in seconds, which the default deadline leaves too little room.
    let tb = TestApp::start_live_within(app(&url, &group), Duration::from_secs(30))
        .await
        .expect("start live");
    a_deferred_order_is_taken_when_its_copy_comes_back(tb).await;
}

/// Deletes and recreates `topic`, so the run reads only what it publishes.
async fn recreate_topic(url: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("admin client");
    let _ = admin
        .delete_topics(&[topic], &AdminOptions::new())
        .await
        .expect("delete_topics call");
    // Deletion completes asynchronously; each metadata fetch waits for the cluster's answer, so
    // the loop is paced by the condition it waits for.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let metadata = admin
            .inner()
            .fetch_metadata(None, Duration::from_millis(500))
            .expect("fetch_metadata call");
        let present = metadata
            .topics()
            .iter()
            .any(|known| known.name() == topic && known.error().is_none());
        if !present {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "topic {topic} outlived its delete request",
        );
    }
    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await
        .expect("create_topics call");
    match &results[0] {
        Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
        Err((name, code)) => panic!("recreating {name} failed: {code}"),
    }
}
