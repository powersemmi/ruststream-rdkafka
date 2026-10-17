//! The service of `examples/kafka_subscriptions_passthrough.rs` written without the macros: the
//! passthrough is a setting of the descriptor the mount site hands to the subscription.
//!
//! ```text
//! cargo test --example manual_kafka_subscriptions_passthrough --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::schemars::{self, JsonSchema};
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Payment {
    id: u64,
}

// `#[outgoing(name = "payments")]` by hand: the type fixes its destination.
impl OutgoingDestination for Payment {
    type Form = FixedName;

    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

// --8<-- [start:config]
/// Settles every payment it reads.
struct Settle;

impl Handle<Payment> for Settle {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(payment.id, "settled");
        ready(Ok(()))
    }
}

fn app() -> impl App<State = ()> {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        // librdkafka pauses a full partition for 10 ms instead of a second.
        let payments = KafkaTopic::new("payments")
            .group("payments-svc")
            .commit(Commit::Tracked)
            .config("fetch.queue.backoff.ms", "10");
        b.include(
            subscriber("payments", Settle)
                .build()
                .map_source(|_| payments),
        );
    })
}
// --8<-- [end:config]

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

    #[tokio::test]
    async fn a_property_the_commit_mode_owns_is_refused_at_startup() {
        let clashing = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
            KafkaBroker::new(["localhost:9092"]),
            |b| {
                // A passthrough that hands the commit back to librdkafka undoes `Commit::Tracked`.
                let untracked = KafkaTopic::new("payments")
                    .group("payments-svc")
                    .commit(Commit::Tracked)
                    .config("enable.auto.offset.store", "true");
                b.include(
                    subscriber("payments", Settle)
                        .build()
                        .map_source(|_| untracked),
                );
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
