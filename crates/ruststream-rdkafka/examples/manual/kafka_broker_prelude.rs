//! The service of `examples/kafka_broker_prelude.rs` without the macros: the handler is a named
//! type, the descriptor is mounted with `map_source`, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_kafka_broker_prelude --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_rdkafka::prelude::*;

struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation { id: order.id }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]),
        |b| {
            let confirm = subscriber("orders", Confirm).map_source(|_| {
                KafkaTopic::new("orders")
                    .group("orders-svc")
                    .commit(Commit::Tracked)
                    .start(StartOffset::Earliest)
            });
            b.include(confirm.reply().build())
                .out_reply(Publish::default());
        },
    )
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Confirmation {
    id: u64,
}

// `#[outgoing(name = "confirmations")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Confirmation {
    type Form = FixedName;
    const DESTINATION: &'static str = "confirmations";
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Confirmation, Order, app};

    #[tokio::test]
    async fn an_order_is_confirmed_on_the_reply_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<KafkaBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 7 });
        Ok(())
    }
}
