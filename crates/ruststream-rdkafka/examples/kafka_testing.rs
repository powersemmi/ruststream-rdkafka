//! In-process unit-testing: the same handlers and descriptors, no Kafka cluster.
//!
//! The `testing` feature ships `KafkaTestBroker`, an in-process stand-in for Kafka. Build the
//! app around it exactly as in production, drive publishes with `TestApp::publish` (which
//! waits for the handlers to settle), and assert on what they did.
//!
//! ```text
//! cargo run --example kafka_testing --features testing
//! ```

use ruststream::codec::{Codec as _, DefaultCodec};
use ruststream::runtime::{AppInfo, Ctx, HandlerOutcome, RustStream, SubscriberSettings as _};
use ruststream::testing::TestApp;
use ruststream::{Broker, OutgoingMessage, Publisher, Seeker as _, subscriber};
use ruststream_rdkafka::context::keys::SeekHandle;
use ruststream_rdkafka::testing::KafkaTestBroker;
use ruststream_rdkafka::{KafkaPosition, KafkaPublish, KafkaTopic};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Payment {
    amount: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Batch {
    id: u64,
    /// Set by the producer when a run of records is known to be unprocessable; the handler jumps
    /// the subscription past it instead of failing through record by record.
    resume_at: Option<i64>,
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

// A handler that repositions its own subscription is an ordinary handler here: the in-process
// transport retains what it routes, so the `SeekHandle` key hands out a seeker that really
// replays that log.
#[subscriber(KafkaTopic::new("batches"))]
async fn settle_batch(batch: &Batch, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    if let Some(resume_at) = batch.resume_at
        && seeker
            .seek(KafkaPosition::offset(0, resume_at))
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    // --8<-- [start:testapp]
    let broker = KafkaTestBroker::new();
    // Seeded before the app exists, so the subscription's opening replay hands the handler a log
    // it can actually skip through.
    seed_batches(&broker).await;

    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(accept);
        b.include(settle_batch.start_at(KafkaPosition::earliest()));
    });
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
    // --8<-- [end:testapp]

    // --8<-- [start:seek]
    // The subscription opened at the start of the retained log, so the reposition has somewhere
    // to go: batch 0 carries the marker and jumps to offset 2, and batch 1 is never handled.
    // The replay settles inside the harness, so the assertion reads finished state - no sleep.
    tb.settle().await.expect("the replay settles");

    let handled: Vec<u64> = tb
        .broker::<KafkaTestBroker>()
        .subscriber("batches")
        .received::<Batch>()
        .into_iter()
        .map(|batch| batch.id)
        .collect();
    assert_eq!(handled, [0, 2], "the poisoned run must be skipped whole");
    // --8<-- [end:seek]

    tb.shutdown().await.expect("shutdown");

    println!("all in-process checks passed");
}

/// Publishes the batch run through the broker's own publisher, before the service starts.
async fn seed_batches(broker: &KafkaTestBroker) {
    let connected = broker.clone().connect().await.expect("connect");
    let publisher = connected.publisher(KafkaPublish::default());
    for (id, resume_at) in [(0, Some(2)), (1, None), (2, None)] {
        let payload = DefaultCodec::default()
            .encode(&Batch { id, resume_at })
            .expect("serializable");
        publisher
            .publish(OutgoingMessage::new("batches", payload.as_ref()))
            .await
            .expect("seed");
    }
}
