//! Batch consumption: the handler receives a whole decoded page per invocation. librdkafka
//! already fetches batches on the wire and hands them over without waiting, so a page is one
//! delivery plus everything already fetched, and the mount caps how large a page the body sees.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_batches -- run
//! ```

use std::time::Duration;

use ruststream::runtime::{App, AppInfo, HandlerOutcome, RustStream};
use ruststream::subscriber;
use ruststream_rdkafka::{Commit, KafkaBroker, KafkaTopic};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize)]
struct Payment {
    id: u64,
    settled: bool,
}

// --8<-- [start:handler]
// The slice parameter is what says "a page at a time"; nothing in the attribute repeats it.
// Batching is native here - a page is one delivery plus everything librdkafka has already
// fetched, bounded by librdkafka's own fetch-queue limits. Returning one HandlerOutcome settles
// the whole page uniformly. `workers(4)` keeps up to four pages in flight at once; `by_key` does
// not apply to batches (a keyed policy here behaves like a plain pool - per-key lanes exist for
// single-message handlers, see the keyed lanes example).
#[subscriber(
    KafkaTopic::new("orders").group("orders-svc").commit(Commit::Tracked),
    workers(4)
)]
async fn handle_page(orders: &[Order]) -> HandlerOutcome {
    let first = orders.first().map(|order| order.id);
    println!(
        "processing a page of {} orders, first id {first:?}",
        orders.len()
    );
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:selective]
// Per-element settlement: entry `i` settles page element `i`. This works, but read the docs on
// how it maps onto Kafka's one-position-per-partition commits: under `Commit::Tracked` the
// committed position only advances up to the first non-acked element, and `retry_after` runs
// through the runtime's deferred-republish fallback (`retry_via` below), not a native delay.
// --8<-- [start:size]
// A page is bounded by librdkafka's own fetch queue, so the size lives in the descriptor's
// config passthrough rather than in a framework window.
#[subscriber(
    KafkaTopic::new("payments")
        .group("payments-svc")
        .commit(Commit::Tracked)
        .config("queued.max.messages.kbytes", "1024")
)]
// --8<-- [end:size]
async fn reconcile_page(payments: &[Payment]) -> Vec<HandlerOutcome> {
    payments
        .iter()
        .map(|payment| {
            if payment.settled {
                HandlerOutcome::ack()
            } else {
                println!("payment {} not settled yet; retrying later", payment.id);
                HandlerOutcome::retry_after(Duration::from_secs(30))
            }
        })
        .collect()
}
// --8<-- [end:selective]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // Kafka has no native delayed redelivery: retry_after re-publishes a delayed copy
        // through this publisher (and settles the original, so the committed position moves).
        // `retry_via` takes a live publisher, not a policy, so the broker mints its early
        // publisher for it - the one handle that resolves the connection at startup.
        let retries = b.broker().retry_publisher();
        b.retry_via(retries);
        b.include(handle_page);
        b.include(reconcile_page);
    })
}
// --8<-- [end:app]
