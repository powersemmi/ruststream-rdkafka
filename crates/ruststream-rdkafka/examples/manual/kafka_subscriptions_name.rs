//! The service of `examples/kafka_subscriptions_name.rs` written without the macros: a topic
//! name in `subscriber(..)` reads that topic in the broker's default consumer group.
//!
//! ```text
//! cargo test --example manual_kafka_subscriptions_name --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::schemars::{self, JsonSchema};
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

// --8<-- [start:name]
/// Reads the topic `orders`; the group comes from the broker.
struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order placed");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("orders-svc");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("orders", Place).build());
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
                b.include(subscriber("orders", Place).build());
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
