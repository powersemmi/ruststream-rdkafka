//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it.

// `RouterDef` names a router builder's return type and is not in the prelude.
use ruststream::runtime::RouterDef;
use ruststream_rdkafka::prelude::*;

use crate::orders;

/// Builds the orders router: a publishing handler (replies to the `confirmations` topic) plus a
/// plain one.
///
/// `confirm` needs a publisher for its reply; `Publish` is the publish policy - pure
/// declaration, holding no connection - and `TypedPublisher::new` puts the default codec on it,
/// reused to decode the order. The runtime pairs the policy into a live publisher once the broker
/// connects, so the router takes no broker at all. `on_cancel` has no reply, so its `include`
/// registers on its own. The router is a consuming builder, so a registration that takes an
/// attachment commits through an explicit terminal (`.publisher(..)`, or `.mount()` for the
/// broker's default policy) and the calls chain; the registration list is opaque, hence
/// `impl RouterDef`.
pub fn orders() -> impl RouterDef<KafkaBroker> {
    let confirmations = TypedPublisher::new(Publish::default());

    Router::new()
        .include(orders::confirm)
        .publisher(confirmations)
        .include(orders::on_cancel)
}
