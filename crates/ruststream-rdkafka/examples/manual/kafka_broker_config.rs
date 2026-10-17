//! The service of `examples/kafka_broker_config.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_kafka_broker_config --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_rdkafka::prelude::*;

struct Ship;

impl Handle<Order, Shipment> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Shipment, HandlerOutcome>> {
        ready(Ok(Shipment {
            order_id: order.id,
            labels: (1..=order.parcels)
                .map(|parcel| format!("{}-{parcel:05}", order.id))
                .collect(),
        }))
    }
}

// --8<-- [start:app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("orders-svc")
        // Every client: the consumers and the producer.
        .config("client.id", "orders-svc")
        .config("socket.keepalive.enable", "true")
        // The producer only.
        .producer_config("compression.type", "lz4")
        .producer_config("message.max.bytes", "65536");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("orders", Ship).reply().build());
    })
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
    parcels: u32,
}

// `#[outgoing(name = "orders")]` by hand: an order names the topic it is published to.
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

// `#[outgoing(name = "shipments")]` by hand: the reply names its topic.
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

    use super::{Order, Ship, Shipment, app};

    #[tokio::test]
    async fn a_shipment_within_the_producer_limit_is_published() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order { id: 7, parcels: 2 };
        tb.broker::<KafkaBroker>().message(&order).publish().await?;

        tb.broker::<KafkaBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment {
                order_id: 7,
                labels: vec!["7-00001".to_owned(), "7-00002".to_owned()],
            });
        Ok(())
    }

    #[tokio::test]
    async fn a_shipment_over_the_producer_limit_is_refused() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 8,
            parcels: 10_000,
        };
        tb.broker::<KafkaBroker>().message(&order).publish().await?;

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
    async fn a_property_librdkafka_refuses_fails_startup() {
        let broker = KafkaBroker::new(["localhost:9092"])
            .default_group("orders-svc")
            .producer_config("acks", "sometimes");
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            b.include(subscriber("orders", Ship).reply().build());
        });

        let refused = TestApp::start(app).await.err().map(|err| err.to_string());
        assert!(
            refused.as_deref().is_some_and(|err| err.contains("acks")),
            "{refused:?}"
        );
    }
}
