//! The first service of the Getting started guide: one handler on the `orders` topic, in the
//! consumer group `orders-svc`.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_getting_started -- run
//! ```

use ruststream_rdkafka::prelude::*;
use serde::Deserialize;

// --8<-- [start:service]
#[derive(Debug, PartialEq, Deserialize)]
struct Order {
    id: u64,
    amount_cents: u64,
}

#[subscriber("orders")]
async fn take_order(order: &Order) -> HandlerOutcome {
    if order.amount_cents == 0 {
        tracing::warn!(order.id, "order without an amount dropped");
        return HandlerOutcome::drop();
    }
    tracing::info!(order.id, order.amount_cents, "order taken");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
        |b| {
            b.include(take_order);
        },
    )
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;
    use serde::Serialize;

    use super::{Order, app};

    /// An order as the shop that places it publishes it.
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "orders")]
    struct Placed {
        id: u64,
        amount_cents: u64,
    }

    // --8<-- [start:first_test]
    #[tokio::test]
    async fn an_order_without_an_amount_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<KafkaBroker>()
            .message(&Placed {
                id: 7,
                amount_cents: 0,
            })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order {
                id: 7,
                amount_cents: 0,
            })
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:first_test]

    #[tokio::test]
    async fn an_order_with_an_amount_is_taken() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<KafkaBroker>()
            .message(&Placed {
                id: 8,
                amount_cents: 1250,
            })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order {
                id: 8,
                amount_cents: 1250,
            })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
