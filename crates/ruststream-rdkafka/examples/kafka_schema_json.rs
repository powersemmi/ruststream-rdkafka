//! A JSON model checked against its schema migrations when the crate compiles: `Cart` matches the
//! JSON Schema of `schemas/carts-value/` in both directions, so a field that drifts from it fails
//! the build.
//!
//! ```text
//! cargo test --example kafka_schema_json --features schema-check,testing
//! ```

use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:model]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema, Schema)]
#[serde(rename_all = "camelCase")]
#[schema(namespace = "com.acme.carts")] // the anchor: com.acme.carts.Cart
struct Cart {
    cart_id: i64,
    items: Vec<Item>,
    note: Option<String>,
    #[serde(default)]
    priority: i32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema, Schema)]
#[schema(namespace = "com.acme.carts")]
struct Item {
    sku: String,
    qty: u32,
}
// --8<-- [end:model]

#[subscriber("carts")]
async fn on_cart(cart: &Cart) -> HandlerOutcome {
    tracing::info!(cart.cart_id, items = cart.items.len(), "cart received");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("carts-svc");
    RustStream::new(AppInfo::new("carts", "0.1.0")).with_broker(broker, |b| {
        b.include(on_cart);
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Cart, Item, app};

    /// Bytes published as they are: what another JSON producer puts on the topic.
    #[derive(Serialized, Outgoing)]
    struct Wire(Vec<u8>);

    fn cart() -> Cart {
        Cart {
            cart_id: 7,
            items: vec![Item {
                sku: "kettle".to_owned(),
                qty: 2,
            }],
            note: None,
            priority: 0,
        }
    }

    #[tokio::test]
    async fn a_checked_model_reaches_the_handler() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(br#"{"cartId":7,"items":[{"sku":"kettle","qty":2}]}"#.to_vec()))
            .to("carts")
            .publish()
            .await?;

        kafka
            .subscriber("carts")
            .assert_called_once()
            .with(&cart())
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
