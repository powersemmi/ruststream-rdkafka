//! The service of `examples/kafka_topics.rs` written without the macros: the `KafkaTopic`
//! descriptor is the source `subscriber(..)` takes.
//!
//! ```text
//! cargo test --example manual_kafka_topics --features testing
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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Confirmation {
    id: u64,
    accepted: bool,
}

// `#[outgoing(name = "confirmations")]` by hand: the reply type declares where it goes.
impl OutgoingDestination for Confirmation {
    type Form = FixedName;

    const DESTINATION: &'static str = "confirmations";
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

// --8<-- [start:descriptor]
/// Confirms every order it reads.
struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation {
            id: order.id,
            accepted: true,
        }))
    }
}

fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // The topic `orders`, read as a member of the group `orders-workers`.
        let orders = KafkaTopic::new("orders")
            .group("orders-workers")
            .start(StartOffset::Earliest)
            .commit(Commit::Tracked)
            .assignment(Assignment::CooperativeSticky)
            .config("fetch.min.bytes", "1024");
        b.include(
            subscriber("orders", Confirm)
                .reply()
                .build()
                .map_source(|_| orders),
        );
    })
}
// --8<-- [end:descriptor]

// The scenario is the test above; the command in the header runs it.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn an_order_is_confirmed_by_the_group() -> Result<(), Box<dyn Error>> {
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
        tb.broker::<KafkaBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation {
                id: 7,
                accepted: true,
            });
        Ok(())
    }
}
