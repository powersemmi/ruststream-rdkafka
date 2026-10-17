//! The descriptor's passthrough: `config(key, value)` sets any librdkafka consumer property the
//! descriptor has no typed option for. Each scenario is a test.
//!
//! ```text
//! cargo test --example kafka_subscriptions_passthrough --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
}

// --8<-- [start:config]
/// Settles payments; librdkafka pauses a full partition for 10 ms instead of a second.
#[subscriber(
    KafkaTopic::new("payments")
        .group("payments-svc")
        .commit(Commit::Tracked)
        .config("fetch.queue.backoff.ms", "10")
)]
async fn settle(payment: &Payment) -> HandlerOutcome {
    tracing::info!(payment.id, "settled");
    HandlerOutcome::ack()
}
// --8<-- [end:config]

fn app() -> impl App<State = ()> {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(settle);
    })
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn the_service_reads_with_its_passthrough() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<KafkaBroker>()
            .message(&Payment { id: 7 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&Payment { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    /// A passthrough that hands the commit back to librdkafka undoes `Commit::Tracked`.
    #[subscriber(
        KafkaTopic::new("payments")
            .group("payments-svc")
            .commit(Commit::Tracked)
            .config("enable.auto.offset.store", "true")
    )]
    async fn settle_untracked(payment: &Payment) -> HandlerOutcome {
        let _ = payment;
        HandlerOutcome::ack()
    }

    #[tokio::test]
    async fn a_property_the_commit_mode_owns_is_refused_at_startup() {
        let clashing = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
            KafkaBroker::new(["localhost:9092"]),
            |b| {
                b.include(settle_untracked);
            },
        );

        let refused = TestApp::start(clashing)
            .await
            .expect_err("the passthrough would undo the commit mode");
        assert!(
            refused.to_string().contains("enable.auto.offset.store"),
            "{refused}"
        );
    }
}
