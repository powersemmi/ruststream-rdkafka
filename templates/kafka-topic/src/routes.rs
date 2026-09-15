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
/// declaration, holding no connection - and `.out_reply(..)` is the chain step that names it.
/// `max_attempts` and `dead_letter` come first: they say how many deliveries one order gets and
/// where it goes when they run out, and they read the same on every broker.
/// Nothing names a codec here, so the default one encodes the reply and decodes the order; a
/// `.codec(..)` step after `.out_reply(..)` would name another. `.build()` seals the
/// registration and hands the router back, so the next `include` chains off it. The runtime
/// pairs the policy into a live publisher once the broker connects, so the router takes no
/// broker at all. `on_cancel` has no reply, so its `include` registers on its own; the
/// registration list is opaque, hence `impl RouterDef`.
///
/// The policy arrives under its concept name because this file globs the broker prelude; the
/// handlers in `orders` glob the core one and name capabilities instead, so the two vocabularies
/// stay in separate files.
pub fn orders() -> impl RouterDef<KafkaBroker> {
    Router::new()
        .include(orders::confirm)
        .max_attempts(nonzero!(5u32))
        .dead_letter("orders.dlq")
        .out_reply(Publish::default())
        .build()
        .include(orders::on_cancel)
}
