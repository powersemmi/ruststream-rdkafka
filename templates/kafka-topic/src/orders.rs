//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The first parameter is the decoded payload; the macro turns each function into a mountable
//! definition (a value named after the function) that `routes` collects into a `Router`. The
//! `KafkaTopic` descriptor form names the subscription's options; `Commit::Tracked` makes each
//! `Ack` a precise per-message acknowledgement backed by the group's committed position. The
//! bare-string form rides on the broker's `default_group` with librdkafka defaults. How many
//! deliveries a message gets, and where it goes when they run out, is declared in `routes`. For
//! transactional publishing and exactly-once pipelines, see the publishing guide.

use ruststream_rdkafka::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An order consumed from the `orders` topic.
///
/// `JsonSchema` lets `asyncapi gen` emit this payload's schema into the generated document.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Order {
    pub id: u64,
    pub item: String,
    pub quantity: u32,
}

/// The reply published to the `confirmations` topic for each order.
///
/// `#[outgoing(name = "confirmations")]` is the topic every confirmation lands on, wherever it
/// is published from.
#[derive(Debug, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "confirmations")]
pub struct Confirmation {
    pub id: u64,
    pub accepted: bool,
}

/// Confirms an incoming order and publishes a `Confirmation` to the `confirmations` topic.
///
/// The `publish` clause makes the runtime encode the `Ok` value and publish it through the
/// publisher wired in `routes`, at the topic the reply type declares; an `Err` settles the
/// delivery by its `HandlerOutcome` instead.
///
/// A `retry()` comes back to this handler: Kafka holds no record back, so the runtime
/// republishes the delivery to the `orders` topic with the retry count incremented, and the cap
/// declared in `routes` is what ends a message that never settles. `drop()` takes the
/// dead-letter path at once.
#[subscriber(KafkaTopic::new("orders").commit(Commit::Tracked), publish)]
pub async fn confirm(order: &Order) -> Result<Confirmation, HandlerOutcome> {
    if order.quantity == 0 {
        // Malformed input is not worth retrying: drop() dead-letters it right away.
        return Err(HandlerOutcome::drop());
    }
    Ok(Confirmation {
        id: order.id,
        accepted: true,
    })
}

/// Logs cancellations from the `cancellations` topic. No reply, so it returns a plain
/// `HandlerOutcome`; under the default auto-commit mode the `Ack` is advisory.
#[subscriber("cancellations")]
pub async fn on_cancel(order: &Order) -> HandlerOutcome {
    println!("order {} ({}) cancelled", order.id, order.item);
    HandlerOutcome::ack()
}
