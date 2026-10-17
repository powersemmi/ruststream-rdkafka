//! Subscriptions of one service that share a consumer group, against a real Kafka.
//!
//! Each subscription joining the group rebalances it, and the members that joined earlier get
//! their partitions back; every record published once the service is up still reaches its
//! subscription. Every test is a no-op unless `KAFKA_TEST_URL` points at a cluster (see
//! `just test-brokers`).

#![cfg(feature = "testing")]

use std::process;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream, SubscriberSettings as _};
use ruststream::testing::TestApp;
use ruststream::{Outgoing, subscriber};
use ruststream_rdkafka::{KafkaBroker, KafkaTopic};
use serde::{Deserialize, Serialize};

mod live;

/// A group forms on the stand in seconds, and each join rebalances it again.
const DEADLINE: Duration = Duration::from_secs(30);

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber(KafkaTopic)]
async fn take_order(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(KafkaTopic)]
async fn take_refund(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

async fn create_topic(url: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("admin client");
    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    for result in admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await
        .expect("create_topics call")
    {
        match result {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => panic!("creating topic {name} failed: {code}"),
        }
    }
}

/// Two subscriptions in the broker's default group, on a fresh group with librdkafka's default
/// `latest` reset: the second one joining hands the first its partition back, and a record
/// published to either topic once the service is up is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriptions_sharing_a_group_read_what_follows_startup() {
    let Some(url) = live::url("KAFKA_TEST_URL") else {
        return;
    };
    let run = process::id();
    let orders = format!("shared-group-orders-{run}");
    let refunds = format!("shared-group-refunds-{run}");
    create_topic(&url, &orders).await;
    create_topic(&url, &refunds).await;
    let (orders_source, refunds_source) = (KafkaTopic::new(&orders), KafkaTopic::new(&refunds));
    let app = RustStream::new(AppInfo::new("shared", "0.1.0")).with_broker(
        KafkaBroker::new([url.clone()]).default_group(format!("shared-group-{run}")),
        |b| {
            b.include(take_order.map_source(move |_| orders_source));
            b.include(take_refund.map_source(move |_| refunds_source));
        },
    );
    let tb = TestApp::start_live_within(app, DEADLINE)
        .await
        .expect("start live");

    for (topic, id) in [(&orders, 1), (&refunds, 2)] {
        tb.broker::<KafkaBroker>()
            .message(&Order { id })
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }

    tb.broker::<KafkaBroker>()
        .subscriber(&orders)
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
    tb.broker::<KafkaBroker>()
        .subscriber(&refunds)
        .assert_called_once()
        .with(&Order { id: 2 })
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown");
}
