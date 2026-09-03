//! In-process unit-testing: the same handlers and descriptors, no Kafka cluster.
//!
//! The `testing` feature ships `KafkaTestBroker`, an in-process stand-in for Kafka. Build the
//! app around it exactly as in production, drive publishes with `TestApp::publish` (which
//! waits for the handlers to settle), and assert on what they did.
//!
//! ```text
//! cargo run --example kafka_testing --features testing
//! ```

use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::subscriber;
use ruststream::testing::TestApp;
use ruststream_rdkafka::KafkaTopic;
use ruststream_rdkafka::testing::KafkaTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Payment {
    amount: u64,
}

// --8<-- [start:handler]
#[subscriber(KafkaTopic::new("payments"))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    // --8<-- [start:testapp]
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        KafkaTestBroker::new(),
        |b| {
            b.include(accept);
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaTestBroker>()
        .publish("payments", &Payment { amount: 100 })
        .await
        .expect("publish drives the handler to quiescence");

    tb.broker::<KafkaTestBroker>()
        .subscriber("payments")
        .assert_called_once()
        .with(&Payment { amount: 100 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
    // --8<-- [end:testapp]

    println!("all in-process checks passed");
}
