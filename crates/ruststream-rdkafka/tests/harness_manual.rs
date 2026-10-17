//! Services under `TestApp` whose handlers are mounted by hand: a `Handle` body bound to a Kafka
//! descriptor with `subscriber(source, body)`, the manual twin of `#[subscriber(..)]`.

#![cfg(feature = "testing")]

use std::future::{Future, ready};

use ruststream::Outgoing;
use ruststream::runtime::{AppInfo, Context, Handle, HandlerOutcome, RustStream, subscriber};
use ruststream::testing::TestApp;
use ruststream_rdkafka::{KafkaBroker, KafkaPartitions, KafkaPublish, KafkaTopic, KafkaTopics};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Order {
    id: u64,
}

/// Takes every order it is handed.
struct TakeOrder;

impl Handle<Order> for TakeOrder {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = order.id;
        ready(Ok(()))
    }
}

/// Each descriptor mounts as it is, the way the attribute takes it, and its subscription reads
/// what is published to its topic. A topic set and a partition reader address no retry copy of
/// their own, so their mount sites name where one goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handle_body_mounts_on_each_kafka_descriptor() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        KafkaBroker::new(["kafka:9092"]).default_group("svc"),
        |b| {
            b.include(
                subscriber(KafkaTopic::new("orders"), TakeOrder)
                    .undocumented()
                    .build(),
            );
            b.include(
                subscriber(KafkaTopics::pattern("^refunds\\..*"), TakeOrder)
                    .undocumented()
                    .build(),
            )
            .out_retry(KafkaPublish::default())
            .to("refunds.retry");
            b.include(
                subscriber(KafkaPartitions::new("audit", [0]), TakeOrder)
                    .undocumented()
                    .build(),
            )
            .out_retry(KafkaPublish::default())
            .to("audit.retry");
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    for (topic, id) in [("orders", 1), ("refunds.eu", 2), ("audit", 3)] {
        tb.broker::<KafkaBroker>()
            .message(&Order { id })
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }

    tb.broker::<KafkaBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
    tb.broker::<KafkaBroker>()
        .subscriber("^refunds\\..*")
        .assert_called_once()
        .with(&Order { id: 2 })
        .settled(HandlerOutcome::ack());
    tb.broker::<KafkaBroker>()
        .subscriber("audit")
        .assert_called_once()
        .with(&Order { id: 3 })
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown");
}
