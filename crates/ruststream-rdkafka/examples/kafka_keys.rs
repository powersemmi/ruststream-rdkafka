//! Keyed worker lanes: several lanes process one topic in parallel, but deliveries that share a
//! record key stay on the same lane (ordered per key). Kafka partitions by the native record
//! key, and this crate surfaces it through `IncomingMessage::partition_key`, so `by_key` works
//! end to end (see the `kafka_producer` example for the publish side).
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_keys -- run
//! ```

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_rdkafka::{KafkaBroker, KafkaTopic, LaneKey};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    tenant: String,
}

// --8<-- [start:consumer]
// Eight lanes by record key (the opt-in): two orders for the same tenant never process
// concurrently (per-tenant ordering), while different tenants run in parallel - even inside
// one partition.
#[subscriber(
    KafkaTopic::new("orders").group("orders-workers").lane_key(LaneKey::RecordKey),
    workers(8, by_key)
)]
async fn on_order(order: &Order) -> HandlerResult {
    println!("order {} for tenant {}", order.id, order.tenant);
    HandlerResult::Ack
}
// --8<-- [end:consumer]

// --8<-- [start:partition_lanes]
// The default lanes by the source partition, Kafka's own ordering unit: everything one
// partition delivers (keyless audit events included) processes in order on one lane.
#[subscriber(KafkaTopic::new("audit").group("audit-workers"), workers(8, by_key))]
async fn on_audit(order: &Order) -> HandlerResult {
    println!("audit entry {} for tenant {}", order.id, order.tenant);
    HandlerResult::Ack
}
// --8<-- [end:partition_lanes]

#[ruststream::app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(on_order);
        b.include(on_audit);
    })
}
