//! One test body, two modes: in process under `TestApp::start`, and against a running Kafka under
//! `TestApp::start_live`. The live test skips without `KAFKA_TEST_URL`, and
//! `RUSTSTREAM_REQUIRE_LIVE=1` turns that skip into a failure.
//!
//! ```text
//! cargo test --example kafka_testing_live --features testing
//! just brokers-up
//! KAFKA_TEST_URL=127.0.0.1:9092 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example kafka_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream_rdkafka::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "testing-live-payments")]
struct Payment {
    amount: u64,
}

#[subscriber("testing-live-payments")]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

/// The service on the cluster at `servers`, reading in the consumer group `group`.
fn app(servers: &str, group: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        KafkaBroker::new([servers]).default_group(group),
        |b| {
            b.include(accept);
        },
    )
}

#[ruststream::app]
fn service() -> impl App<State = ()> {
    app("localhost:9092", "payments-svc")
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;
    use std::time::Duration;

    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::error::RDKafkaErrorCode;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Payment, app, live};

    // --8<-- [start:start-live]
    /// The body both modes run; only the start call differs.
    async fn accepts_a_payment(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<KafkaBroker>()
            .message(&Payment { amount: 100 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("testing-live-payments")
            .assert_called_once()
            .with(&Payment { amount: 100 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        accepts_a_payment(TestApp::start(app("kafka:9092", "payments-svc")).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(servers) = live::url("KAFKA_TEST_URL") else {
            return Ok(());
        };
        ensure_topic(&servers, "testing-live-payments").await?;
        // A group of this run's own, so nothing an earlier run committed is read.
        let group = format!("payments-{}", process::id());
        // A group forms on a cluster in seconds, more than the default ten-second bound allows for.
        let tb = TestApp::start_live_within(app(&servers, &group), Duration::from_secs(30)).await?;
        accepts_a_payment(tb).await
    }
    // --8<-- [end:start-live]

    /// Creates `topic` unless the cluster has it: a group assigned a topic created after it
    /// subscribed reads it from `auto.offset.reset`, the end of the log by default.
    async fn ensure_topic(servers: &str, topic: &str) -> Result<(), Box<dyn Error>> {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", servers)
            .create()?;
        let topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
        for created in admin.create_topics([&topic], &AdminOptions::new()).await? {
            match created {
                Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((name, code)) => return Err(format!("creating {name} failed: {code}").into()),
            }
        }
        Ok(())
    }
}
