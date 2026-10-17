//! The service of `examples/kafka_subscriptions_partitions.rs` written without the macros: a
//! `KafkaPartitions` descriptor is the source `subscriber(..)` takes.
//!
//! ```text
//! cargo test --example manual_kafka_subscriptions_partitions --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::schemars::{self, JsonSchema};
use ruststream_rdkafka::context::KafkaContext;
use ruststream_rdkafka::context::keys::Offset;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:reader]
/// Logs each order with its offset in the partition.
struct Inspect;

impl Handle<Order, (), (), KafkaContext> for Inspect {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, KafkaContext>,
    ) -> Result<(), HandlerOutcome> {
        tracing::info!(order.id, offset = ctx.context(Offset), "partition 0");
        Ok(())
    }
}

fn app() -> impl App {
    // The broker names no group, and the reader needs none.
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("inspector", "0.1.0")).with_broker(broker, |b| {
        // Partition 0 of `orders`, from its first record.
        let partition = KafkaPartitions::new("orders", [0]).start(StartOffset::Earliest);
        // No publish reaches exactly these partitions, so a retried order goes to a topic of
        // its own.
        b.include(
            subscriber("orders", Inspect)
                .build()
                .map_source(|_| partition),
        )
        .out_retry(Publish::default())
        .to("orders.retry");
    })
}
// --8<-- [end:reader]

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

// The scenario is the test above; the command in the header runs it.
fn main() {}
