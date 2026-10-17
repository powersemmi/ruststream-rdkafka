//! The service of `examples/kafka_subscriptions_start_offset.rs` written without the macros:
//! the descriptor carries the start offset, and the mount site hands it to the subscription.
//!
//! ```text
//! KAFKA_TEST_URL=127.0.0.1:9092 cargo test --example manual_kafka_subscriptions_start_offset --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::schemars::{self, JsonSchema};
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Invoice {
    id: u64,
}

// `#[outgoing(name = "ledger.invoices")]` by hand: the type fixes its destination.
impl OutgoingDestination for Invoice {
    type Form = FixedName;

    const DESTINATION: &'static str = "ledger.invoices";
}

impl MessageHeaders for Invoice {
    type Contract = NoHeaders;
}

// --8<-- [start:start]
/// Archives every invoice it reads.
struct Archive;

impl Handle<Invoice> for Archive {
    async fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        tracing::info!(invoice.id, "archived");
        Ok(())
    }
}

fn app(servers: &str, group: &str) -> impl App {
    let broker = KafkaBroker::new([servers]).default_group(group);
    RustStream::new(AppInfo::new("archive", "0.1.0")).with_broker(broker, |b| {
        // A new archive group starts at the oldest record the topic retains.
        let invoices = KafkaTopic::new("ledger.invoices").start(StartOffset::Earliest);
        b.include(
            subscriber("ledger.invoices", Archive)
                .build()
                .map_source(|_| invoices),
        );
    })
}
// --8<-- [end:start]

#[cfg(test)]
#[path = "../support/kafka_subscriptions_live.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_new_group_reads_what_the_topic_held_before_it() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url() else {
            return Ok(());
        };
        live::recreate_topic(&url, "ledger.invoices", 1).await;

        // The first invoice is read and committed by a group that existed before the archive.
        let before =
            TestApp::start_live_within(app(&url, &live::group("billing")), live::START).await?;
        before
            .broker::<KafkaBroker>()
            .message(&Invoice { id: 1 })
            .publish()
            .await?;
        before.shutdown().await?;

        let tb =
            TestApp::start_live_within(app(&url, &live::group("archive")), live::START).await?;
        tb.broker::<KafkaBroker>()
            .message(&Invoice { id: 2 })
            .publish()
            .await?;

        // A live publish returns once the subscription has handled as many records as the test
        // published, so the first of them is what decides: the record older than the group.
        let archived = tb
            .broker::<KafkaBroker>()
            .subscriber("ledger.invoices")
            .received::<Invoice>();
        assert_eq!(archived.first(), Some(&Invoice { id: 1 }));
        tb.shutdown().await?;
        Ok(())
    }
}

// The scenario is the test above; the command in the header runs it.
fn main() {}
