//! A partition reader: `KafkaPartitions` reads exactly the partitions it names, joins no
//! consumer group and never rebalances. The scenario is a test.
//!
//! ```text
//! cargo test --example kafka_subscriptions_partitions --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_rdkafka::context::keys::Offset;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:reader]
/// Inspects partition 0 of `orders` from its first record, with no group of its own.
#[subscriber(KafkaPartitions::new("orders", [0]).start(StartOffset::Earliest))]
async fn inspect(order: &Order, Ctx(offset): Ctx<Offset>) -> HandlerOutcome {
    tracing::info!(order.id, offset, "partition 0");
    HandlerOutcome::ack()
}

fn app() -> impl App {
    // The broker names no group, and the reader needs none.
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("inspector", "0.1.0")).with_broker(broker, |b| {
        // No publish reaches exactly these partitions, so a retried order goes to a topic of
        // its own.
        b.include(inspect)
            .out_retry(Publish::default())
            .to("orders.retry");
    })
}
// --8<-- [end:reader]

// The scenario is the test above; the command in the header runs it.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn the_reader_reads_its_partition_without_a_group() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for id in [1, 2] {
            tb.broker::<KafkaBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }

        // The subscription's name is its topic.
        let inspected = tb
            .broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called(2)
            .received::<Order>();
        assert_eq!(inspected, [Order { id: 1 }, Order { id: 2 }]);
        Ok(())
    }
}
