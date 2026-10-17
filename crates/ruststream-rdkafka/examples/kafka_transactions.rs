//! A transactional slot: an order fans out into one shipment per item, and the shipments become
//! visible together at the commit, or not at all. The scenarios are tests.
//!
//! ```text
//! cargo test --example kafka_transactions --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    items: Vec<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
    item: String,
}

// --8<-- [start:handler]
#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

/// One shipment per item, published in one transaction.
#[subscriber("orders")]
async fn ship(
    order: &Order,
    Out(shipments): Out<impl TransactionalPublisher, Shipments>,
) -> HandlerOutcome {
    if shipments.begin_transaction().await.is_err() {
        return HandlerOutcome::retry();
    }
    for item in &order.items {
        if item.is_empty() {
            // The shipments already sent belong to the transaction, so the abort discards them.
            if shipments.abort().await.is_err() {
                return HandlerOutcome::retry();
            }
            tracing::warn!(
                order = order.id,
                "an item has no name, the order ships nothing"
            );
            return HandlerOutcome::drop();
        }
        let shipment = Shipment {
            order_id: order.id,
            item: item.clone(),
        };
        if shipments.message(&shipment).publish().await.is_err() {
            shipments.abort().await.ok();
            return HandlerOutcome::retry();
        }
    }
    if shipments.commit().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    let shipments = Publish::default()
        .transactional_id("shipments-svc-1")
        .transaction_timeout(Duration::from_secs(10));
    RustStream::new(AppInfo::new("shipments", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("shipments-svc"),
        |b| {
            b.include(ship).out(Shipments, shipments).build();
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

    use super::{Order, Shipment, Shipments, app};

    #[tokio::test]
    async fn an_order_ships_every_item_at_the_commit() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<KafkaBroker>();
        broker
            .message(&Order {
                id: 1,
                items: vec!["anvil".to_owned(), "rope".to_owned()],
            })
            .publish()
            .await?;

        broker
            .published::<Shipment>("shipments")
            .assert_called(2)
            .with(&Shipment {
                order_id: 1,
                item: "rope".to_owned(),
            });
        broker
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_aborted_order_ships_nothing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<KafkaBroker>();
        broker
            .message(&Order {
                id: 2,
                items: vec!["anvil".to_owned(), String::new()],
            })
            .publish()
            .await?;

        // The first shipment went into the transaction, and the abort discarded it.
        tb.out::<Shipments>().assert_called_once();
        broker
            .published::<Shipment>("shipments")
            .assert_not_called();
        broker
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
