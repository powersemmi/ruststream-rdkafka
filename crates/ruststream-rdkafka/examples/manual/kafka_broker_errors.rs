//! The service of `examples/kafka_broker_errors.rs` without the macros: the handler is a named type, the slot a marker type,
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_kafka_broker_errors --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_rdkafka::prelude::*;

/// The slot shipments leave through.
struct Shipments;

impl OutSlot for Shipments {
    const NAME: &'static str = "Shipments";
    // `Shipment` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Shipments> for Shipment {}

// --8<-- [start:handler]
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
        let shipment = Shipment {
            order_id: order.id,
            labels: (1..=order.parcels)
                .map(|parcel| format!("{}-{parcel:05}", order.id))
                .collect(),
        };
        if let Err(err) = outs.get(Shipments).message(&shipment).publish().await {
            // The error names the topic and the cluster's reason.
            tracing::warn!(subscription = "orders", order.id, error = %err, "shipment not published");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("orders-svc")
        .producer_config("message.max.bytes", "65536");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("orders", Ship).build())
            .out(Shipments, Publish::default())
            .build();
    })
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    parcels: u32,
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
struct Shipment {
    order_id: u64,
    labels: Vec<String>,
}

// `#[outgoing(name = "shipments")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Shipment {
    type Form = FixedName;
    const DESTINATION: &'static str = "shipments";
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use ruststream_rdkafka::KafkaError;

    use super::{Order, Ship, Shipment, Shipments, app};

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
                b.include(subscriber("orders", Ship).build())
                    .out(Shipments, Publish::default())
                    .build();
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
