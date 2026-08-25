//! {{project-name}} - a RustStream service over Kafka topics.
//!
//! Handlers live in `orders`, wiring in `routes`; `#[ruststream::app]` generates `main`, so there
//! is no runtime boilerplate to maintain:
//!
//! - `cargo run -- run` (or `ruststream run`) starts the service until interrupted.
//! - `cargo run -- asyncapi gen` (or `ruststream asyncapi gen`) prints the AsyncAPI document.
//!
//! `KafkaBroker::new` only records configuration, so it slots into the synchronous builder; the
//! runtime climbs the lifecycle ladder around it (`connect` once at startup, subscriptions and
//! publishers off the connected form, `shutdown` at the end). Start a broker first, for example
//! a single-node KRaft container, with topic auto-creation or the `orders`, `orders.retry`,
//! `orders.dlq`, `cancellations`, and `confirmations` topics created up front.

mod orders;
mod routes;

use ruststream_rdkafka::prelude::*;

/// Builds the service: one Kafka broker with the orders router mounted.
///
/// `default_group` is the consumer group for every subscription that does not name its own -
/// Kafka cannot subscribe without one.
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("{{project-name}}", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("{{project-name}}"),
        |b| {
            b.include_router(routes::orders());
        },
    )
}
