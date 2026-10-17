//! A subscription by topic name: `#[subscriber("orders")]` reads the topic `orders` in the
//! broker's default consumer group. Each scenario is a test.
//!
//! ```text
//! cargo test --example kafka_subscriptions_name --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:name]
/// Reads the topic `orders`; the group comes from the broker.
#[subscriber("orders")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("orders-svc");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place);
    })
}
// --8<-- [end:name]

// The scenarios are the tests above; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn the_topic_is_read_in_the_default_group() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<KafkaBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_service_with_no_group_does_not_start() {
        let groupless = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            KafkaBroker::new(["localhost:9092"]),
            |b| {
                b.include(place);
            },
        );

        let refused = TestApp::start(groupless)
            .await
            .expect_err("Kafka reads a topic only through a consumer group");
        assert!(
            refused.to_string().contains("no consumer group"),
            "{refused}"
        );
    }
}
