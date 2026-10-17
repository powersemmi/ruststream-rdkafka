//! Where a new group starts: `StartOffset` applies to a partition the group has committed
//! nothing on. The scenario is a test against a cluster, because what a new group finds is what
//! the topic held before it existed.
//!
//! ```text
//! KAFKA_TEST_URL=127.0.0.1:9092 cargo test --example kafka_subscriptions_start_offset --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger.invoices")]
struct Invoice {
    id: u64,
}

// --8<-- [start:start]
/// Archives every invoice: a new archive group starts at the oldest record the topic retains.
#[subscriber(KafkaTopic::new("ledger.invoices").start(StartOffset::Earliest))]
async fn archive(invoice: &Invoice) -> HandlerOutcome {
    tracing::info!(invoice.id, "archived");
    HandlerOutcome::ack()
}
// --8<-- [end:start]

fn app(servers: &str, group: &str) -> impl App {
    let broker = KafkaBroker::new([servers]).default_group(group);
    RustStream::new(AppInfo::new("archive", "0.1.0")).with_broker(broker, |b| {
        b.include(archive);
    })
}

// The scenario is the test above; the command in the header runs it.
fn main() {}

#[cfg(test)]
#[path = "support/kafka_subscriptions_live.rs"]
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
