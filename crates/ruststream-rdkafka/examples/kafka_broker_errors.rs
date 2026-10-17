//! `KafkaError` where a service meets it: a publish the cluster refuses, and a startup that fails.
//!
//! ```text
//! cargo test --example kafka_broker_errors --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_rdkafka::prelude::*;

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

// --8<-- [start:handler]
#[subscriber("orders")]
async fn ship(order: &Order, Out(shipments): Out<impl Publisher, Shipments>) -> HandlerOutcome {
    let shipment = Shipment {
        order_id: order.id,
        labels: (1..=order.parcels)
            .map(|parcel| format!("{}-{parcel:05}", order.id))
            .collect(),
    };
    if let Err(err) = shipments.message(&shipment).publish().await {
        // The error names the topic and the cluster's reason.
        tracing::warn!(subscription = "orders", order.id, error = %err, "shipment not published");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("orders-svc")
        .producer_config("message.max.bytes", "65536");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(ship).out(Shipments, Publish::default()).build();
    })
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    parcels: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
    labels: Vec<String>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use ruststream_rdkafka::KafkaError;

    use super::{Order, Shipment, Shipments, app, ship};

    #[tokio::test]
    async fn a_shipment_the_cluster_takes_acks_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 7, parcels: 1 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<KafkaBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment {
                order_id: 7,
                labels: vec!["7-00001".to_owned()],
            });
        Ok(())
    }

    #[tokio::test]
    async fn a_shipment_the_cluster_refuses_retries_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order {
                id: 8,
                parcels: 10_000,
            })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        tb.broker::<KafkaBroker>()
            .published::<Shipment>("shipments")
            .assert_not_called();
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_startup_carries_the_kafka_error() {
        // No consumer group anywhere: Kafka cannot read the topic.
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            KafkaBroker::new(["localhost:9092"]),
            |b| {
                b.include(ship).out(Shipments, Publish::default()).build();
            },
        );

        let failed = TestApp::start(app).await.err();
        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(kafka) = err.downcast_ref::<KafkaError>() {
                assert!(matches!(kafka, KafkaError::InvalidOptions(_)), "{kafka}");
                return;
            }
            cause = err.source();
        }
        panic!("startup did not fail with a KafkaError: {failed:?}");
    }
}
