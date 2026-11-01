//! The service of `examples/kafka_schema_check.rs` written without the macros.
//!
//! ```text
//! cargo test --example manual_kafka_schema_check --features avro,schema-check,testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::avro::{AvroCodec, AvroSchema};
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema::Schema;
use ruststream_rdkafka::schema_registry::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, JsonSchema, Schema)]
#[avro(namespace = "com.acme.orders")] // the record is com.acme.orders.Order
struct Order {
    id: i64,
    item: Item,
    note: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, JsonSchema, Schema)]
#[avro(namespace = "com.acme.orders")]
struct Item {
    sku: String,
}

/// The definition `#[subscriber("orders")]` would have minted.
struct OnOrder;

impl Handle<Order> for OnOrder {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, order.item.sku, "order received");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let codec = AvroCodec::local(Order::get_schema()).expect("a derived schema resolves");
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("orders-svc");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_codec(broker, codec, |b| {
        b.include(subscriber("orders", OnOrder).build());
    })
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::avro::{self, AvroCodec, AvroSchema};
    use ruststream_rdkafka::prelude::*;

    use super::{Item, Order, app};

    /// Bytes published as they are: what another Avro producer puts on the topic.
    #[derive(Serialized, Outgoing)]
    struct Wire(Vec<u8>);

    fn order() -> Order {
        Order {
            id: 7,
            item: Item {
                sku: "kettle".to_owned(),
            },
            note: None,
        }
    }

    #[tokio::test]
    async fn a_checked_model_reaches_the_handler() -> Result<(), Box<dyn Error>> {
        let mut datum = BytesMut::new();
        avro::encode(&order(), &mut datum)?;
        let tb = TestApp::start(app()).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(datum.to_vec()))
            .to("orders")
            .publish()
            .await?;

        let codec = AvroCodec::local(Order::get_schema())?;
        kafka
            .subscriber("orders")
            .assert_called_once()
            .with_codec(&codec, &order())
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
