//! Batch consumption through the core `Buffered` window: the handler receives a whole decoded
//! page per invocation. librdkafka already fetches batches on the wire and hands them over
//! without waiting, so the client-side window drains what is ready and only `max_wait` bounds
//! the tail latency of an under-filled page.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_batches -- run
//! ```

use std::time::Duration;

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream::{Buffered, nonzero};
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
// The `batch(..)` marker wraps the usual source: batching is native - a page is one delivery
// plus everything librdkafka has already fetched, bounded by librdkafka's own fetch-queue
// limits. Returning one HandlerResult settles the whole page uniformly. `workers(4)` keeps up
// to four pages in flight at once; `by_key` does not apply to batches (a keyed policy here
// behaves like a plain pool - per-key lanes exist for single-message handlers, see the keyed
// lanes example).
#[subscriber(
    batch(KafkaTopic::new("orders").group("orders-svc").commit(Commit::Tracked)),
    workers(4)
)]
async fn handle_page(orders: &[Order]) -> HandlerResult {
    let first = orders.first().map(|order| order.id);
    println!(
        "processing a page of {} orders, first id {first:?}",
        orders.len()
    );
    HandlerResult::Ack
}
// --8<-- [end:handler]

// --8<-- [start:selective]
// Per-element settlement: entry `i` settles page element `i`. This works, but read the docs on
// how it maps onto Kafka's one-position-per-partition commits: under `Commit::Tracked` the
// committed position only advances up to the first non-acked element, and Kafka has no native
// delayed redelivery, so `retry_after` degrades to a plain `retry()` hole.
// Wrapping the source in the core `Buffered` adapter is the opt-in for an explicit
// size/deadline page window on top of the native batching.
#[subscriber(batch(
    Buffered::<KafkaTopic>::new(
        KafkaTopic::new("payments").group("payments-svc").commit(Commit::Tracked)
    )
    .max_size(nonzero!(50))
    .max_wait(Duration::from_millis(20))
))]
async fn reconcile_page(payments: &[Payment]) -> Vec<HandlerResult> {
    payments
        .iter()
        .map(|payment| {
            if payment.settled {
                HandlerResult::Ack
            } else {
                println!("payment {} not settled yet; retrying later", payment.id);
                HandlerResult::retry_after(Duration::from_secs(30))
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
        b.include_batch(handle_page);
        b.include_batch(reconcile_page);
    })
}
// --8<-- [end:app]
