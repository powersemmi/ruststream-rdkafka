//! The service of `examples/kafka_eos_pipeline.rs` with its handler written as a `Handle` impl
//! instead of `#[subscriber]`.
//!
//! ```text
//! cargo test --example manual_kafka_eos_pipeline --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::OutgoingMessage;
use ruststream::schemars::{self, JsonSchema};
use ruststream_rdkafka::EosPipeline;
use ruststream_rdkafka::context::KafkaContext;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[outgoing(name = "carts")]
struct Cart {
    id: u64,
    items: Vec<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct CartLine {
    cart_id: u64,
    item: String,
}

// --8<-- [start:handler]
/// Splits a cart into one record per item, all committed with the cart's offset.
struct Split;

impl<W> Handle<Cart, (), Outs<(W,)>, KafkaContext> for Split
where
    W: OutEntry<DefaultSlot, Wire = EosPipeline>,
{
    async fn handle(
        &self,
        cart: &Cart,
        outs: &Outs<(W,)>,
        ctx: &mut Context<'_, KafkaContext>,
    ) -> Result<(), HandlerOutcome> {
        let pipeline: &EosPipeline = outs.get(DefaultSlot);
        let source = ctx.context(Source);
        for item in &cart.items {
            let line = CartLine {
                cart_id: cart.id,
                item: item.clone(),
            };
            let Ok(payload) = serde_json::to_vec(&line) else {
                return Err(HandlerOutcome::drop());
            };
            let record = OutgoingMessage::new("cart-lines", payload.as_slice());
            // A failed publish aborts the window, which redelivers the cart.
            if pipeline.publish(&source, record, None).await.is_err() {
                return Err(HandlerOutcome::retry());
            }
        }
        Ok(())
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("checkout-svc"),
        |b| {
            let carts = subscriber("carts", Split).map_source(|_| {
                KafkaTopic::new("carts").commit(Commit::Transactional("checkout-1".into()))
            });
            b.include(carts.build())
                .out(DefaultSlot, EosPublish::new("checkout-1"))
                .build();
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

    use super::{Cart, CartLine, app};

    #[tokio::test(start_paused = true)]
    async fn every_line_commits_with_the_carts_offset() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<KafkaBroker>();
        broker
            .message(&Cart {
                id: 4,
                items: vec!["tea".to_owned(), "cups".to_owned(), "tray".to_owned()],
            })
            .publish()
            .await?;

        broker
            .published::<CartLine>("cart-lines")
            .assert_called(3)
            .with(&CartLine {
                cart_id: 4,
                item: "tray".to_owned(),
            });
        broker
            .subscriber("carts")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
