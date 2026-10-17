//! The first service of the Getting started guide, written without the macros: the handler is a
//! type with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by
//! hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_getting_started -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::prelude::*;
use serde::Deserialize;

// --8<-- [start:service]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
struct Order {
    id: u64,
    amount_cents: u64,
}

struct TakeOrder;

impl Handle<Order> for TakeOrder {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if order.amount_cents == 0 {
            tracing::warn!(order.id, "order without an amount dropped");
            return ready(Err(HandlerOutcome::drop()));
        }
        tracing::info!(order.id, order.amount_cents, "order taken");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
        |b| {
            b.include(subscriber("orders", TakeOrder).build());
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
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
    #[derive(Serialize)]
    struct Placed {
        id: u64,
        amount_cents: u64,
    }

    impl OutgoingDestination for Placed {
        type Form = FixedName;

        const DESTINATION: &'static str = "orders";
    }

    impl MessageHeaders for Placed {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Placed {
        const NAME: &'static str = "Placed";
        const DESCRIPTION: Option<&'static str> = None;
    }

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
