//! An exactly-once pipeline: a handler's replies and the offsets of the deliveries they answer
//! commit in one producer transaction. The scenarios are tests.
//!
//! ```text
//! cargo test --example kafka_eos --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "raw-orders")]
struct Order {
    id: u64,
    cents: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "priced-orders")]
struct PricedOrder {
    id: u64,
    cents: u64,
    tax_cents: u64,
}

// --8<-- [start:handler]
/// Prices an order; the subscription commits its offsets through the `pricing-1` pipeline.
#[subscriber(
    KafkaTopic::new("raw-orders").commit(Commit::Transactional("pricing-1".into())),
    reply
)]
async fn price(order: &Order) -> PricedOrder {
    PricedOrder {
        id: order.id,
        cents: order.cents,
        tax_cents: order.cents / 5,
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    // The policy's id is the pipeline the subscription names, and the producer's transactional id.
    let pipeline = EosPublish::new("pricing-1").commit_interval(Duration::from_millis(100));
    RustStream::new(AppInfo::new("pricing", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("pricing-svc"),
        |b| {
            b.include(price).out_reply(pipeline).transform(EosReplies);
        },
    )
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Order, PricedOrder, app};

    /// The reply becomes visible when its window commits, together with the consumed offset;
    /// the paused clock lets the commit interval pass.
    #[tokio::test(start_paused = true)]
    async fn a_reply_lands_when_its_window_commits() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<KafkaBroker>();
        broker
            .message(&Order { id: 1, cents: 1000 })
            .publish()
            .await?;

        broker
            .published::<PricedOrder>("priced-orders")
            .assert_called_once()
            .with(&PricedOrder {
                id: 1,
                cents: 1000,
                tax_cents: 200,
            });
        broker
            .subscriber("raw-orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
