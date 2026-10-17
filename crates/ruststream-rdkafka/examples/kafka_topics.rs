//! A topic descriptor: `KafkaTopic` reads one topic in one consumer group and carries every
//! other setting of the subscription. The scenario is a test.
//!
//! ```text
//! cargo test --example kafka_topics --features testing
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

// The reply topic is a property of the confirmation itself, so the type declares it and the
// subscriber's clause names none.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
    accepted: bool,
}

// --8<-- [start:descriptor]
/// Confirms every order of the topic `orders`, as a member of the group `orders-workers`.
#[subscriber(
    KafkaTopic::new("orders")
        .group("orders-workers")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
        .assignment(Assignment::CooperativeSticky)
        .config("fetch.min.bytes", "1024"),
    reply
)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: true,
    }
}
// --8<-- [end:descriptor]

fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(confirm);
    })
}

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

// The scenario is the test above; the command in the header runs it.
fn main() {}
