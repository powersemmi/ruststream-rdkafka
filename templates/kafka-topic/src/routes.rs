//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it.

use ruststream::runtime::{Router, RouterDef, TypedPublisher};
use ruststream_rdkafka::{KafkaBroker, KafkaPublish};

use crate::orders;

/// Builds the orders router: a publishing handler (replies to the `confirmations` topic) plus a
/// plain one.
///
/// `confirm` needs a publisher for its reply; `KafkaPublish` is the publish policy - pure
/// declaration, holding no connection - and `TypedPublisher::new` puts the default codec on it,
/// reused to decode the order. The runtime pairs the policy into a live publisher once the broker
/// connects, so the router takes no broker at all. `on_cancel` has no reply, so it is mounted with
/// `include`. The router is a consuming builder, so the calls chain; the registration list is
/// opaque, hence `impl RouterDef`.
pub fn orders() -> impl RouterDef<KafkaBroker> {
    let confirmations = TypedPublisher::new(KafkaPublish::default());

    Router::new()
        .include_publishing(orders::confirm, confirmations)
        .include(orders::on_cancel)
}
