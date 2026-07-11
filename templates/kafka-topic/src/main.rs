//! {{project-name}} - a RustStream service over Kafka topics.
//!
//! Handlers live in `orders`, wiring in `routes`; `#[ruststream::app]` generates `main`, so there
//! is no runtime boilerplate to maintain:
//!
//! - `cargo run -- run` (or `ruststream run`) starts the service until interrupted.
//! - `cargo run -- asyncapi gen` (or `ruststream asyncapi gen`) prints the AsyncAPI document.
//!
//! `KafkaBroker::new` is synchronous and does no I/O, so it slots into the builder; the runtime
//! connects once at startup, before the subscriptions consume. Start a broker first, for example
//! a single-node KRaft container, with topic auto-creation or the `orders`, `orders.retry`,
//! `orders.dlq`, `cancellations`, and `confirmations` topics created up front.

mod orders;
mod routes;

use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream_rdkafka::KafkaBroker;

/// Builds the service: one Kafka broker with the orders router mounted.
///
/// `default_group` is the consumer group for every subscription that does not name its own -
/// Kafka cannot subscribe without one.
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("{{project-name}}", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("{{project-name}}"),
        |b| {
            let router = routes::orders(b.broker());
            b.include_router(router);
        },
    )
}
