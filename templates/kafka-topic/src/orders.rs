//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The first parameter is the decoded payload; the macro turns each function into a mountable
//! definition (a value named after the function) that `routes` collects into a `Router`. The
//! `KafkaTopic` descriptor form names the subscription's options; `Commit::Tracked` makes each
//! `Ack` a precise per-message acknowledgement backed by the group's committed position, and
//! the retry options give `retry()` an immediate meaning (republish to the retry topic, then
//! dead-letter once the attempts cap out). The bare-string form rides on the broker's
//! `default_group` with librdkafka defaults. For transactional publishing and exactly-once
//! pipelines, see the publishing guide.

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
#[derive(Debug, Serialize, JsonSchema)]
pub struct Confirmation {
    pub id: u64,
    pub accepted: bool,
}

/// Confirms an incoming order and publishes a `Confirmation` to the `confirmations` topic.
///
/// The `publish("confirmations")` clause makes the runtime encode the `Ok` value and publish
/// it through the publisher wired in `routes` (the outgoing message name is the destination
/// topic); an `Err` settles the delivery by its `HandlerOutcome` instead.
///
/// The descriptor wires the retry pipeline: `and_topic` puts the retry topic on the same
/// subscription, so a `retry()` republishes there (with an attempt count riding in a header)
/// and the copy comes back to this handler; once the next retry would exceed
/// `max_deliveries`, the message dead-letters to `orders.dlq` instead. `drop()` takes the
/// dead-letter path immediately.
#[subscriber(
    KafkaTopic::new("orders")
        .and_topic("orders.retry")
        .commit(Commit::Tracked)
        .retry(Retry::Topic("orders.retry".into()))
        .max_deliveries(5)
        .dead_letter("orders.dlq"),
    publish("confirmations")
)]
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
