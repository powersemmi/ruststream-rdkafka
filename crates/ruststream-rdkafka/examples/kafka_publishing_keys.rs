//! Record keys on Kafka: the partition-key header of a published message becomes the record's
//! native key, so every record of one key lands on one partition, in order.
//!
//! ```text
//! cargo test --example kafka_publishing_keys --features testing
//! cargo run --example kafka_publishing_keys -- run
//! ```

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    customer: String,
}

// --8<-- [start:keys]
/// The header a shipment carries: `kafka-partition-key`, which becomes the record key.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct ShipmentKey {
    #[serde(rename = "kafka-partition-key")]
    customer: String,
}

/// Every shipment of one customer lands on one partition, so its consumer sees them in order.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments", headers = ShipmentKey)]
struct Shipment {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

#[subscriber(KafkaTopic::new("orders").group("shipping"))]
async fn ship(order: &Order, Out(shipments): Out<impl Publisher, Shipments>) -> HandlerOutcome {
    let key = ShipmentKey {
        customer: order.customer.clone(),
    };
    let shipment = Shipment { order_id: order.id };
    if shipments
        .message(&shipment)
        .with_headers(&key)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:keys]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]),
        |b| {
            b.include(ship).out(Shipments, Publish::default()).build();
        },
    )
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
