//! The service of `examples/kafka_publishing_keys.rs` written without `#[subscriber]`: the body
//! declares its publishing slot in the injections arena and publishes the keyed shipment there.
//!
//! ```text
//! cargo test --example manual_kafka_publishing_keys --features testing
//! cargo run --example manual_kafka_publishing_keys -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    customer: String,
}

// --8<-- [start:keys]
/// The header a shipment carries: `kafka-partition-key`, which becomes the record key.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct ShipmentKey {
    #[serde(rename = "kafka-partition-key")]
    customer: String,
}

/// Every shipment of one customer lands on one partition, so its consumer sees them in order.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shipments", headers = ShipmentKey)]
struct Shipment {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

struct Ship;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
where
    Entry: OutEntry<Shipments, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let key = ShipmentKey {
            customer: order.customer.clone(),
        };
        let shipment = Shipment { order_id: order.id };
        outs.get(Shipments)
            .message(&shipment)
            .with_headers(&key)
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}

fn orders() -> KafkaTopic {
    KafkaTopic::new("orders").group("shipping")
}
// --8<-- [end:keys]

fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]),
        |b| {
            let ship = subscriber("orders", Ship).map_source(|_| orders());
            b.include(ship.build())
                .out(Shipments, Publish::default())
                .build();
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::PARTITION_KEY_HEADER;
    use ruststream_rdkafka::prelude::*;

    use super::{Order, Shipment, Shipments, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shipment_is_keyed_by_its_customer() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order {
                id: 7,
                customer: "customer-42".to_owned(),
            })
            .publish()
            .await?;

        tb.out::<Shipments>().assert_called_once();
        tb.broker::<KafkaBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 7 })
            .with_header(PARTITION_KEY_HEADER, "customer-42");
        tb.shutdown().await?;
        Ok(())
    }
}
