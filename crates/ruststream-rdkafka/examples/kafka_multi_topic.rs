//! One subscription over several topics, and a pattern subscription: one consumer, one group,
//! one handler per subscription - all matched topics share the handler's payload type, and
//! each delivery still reports the topic it came from.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_multi_topic -- run
//! ```

use ruststream_rdkafka::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct OrderEvent {
    id: u64,
}

// --8<-- [start:multi]
// `and_topic` adds topics to the same subscription: one consumer joins the group for both.
#[subscriber(KafkaTopic::new("orders").and_topic("cancellations").group("orders-svc"))]
async fn on_order_event(event: &OrderEvent) -> HandlerResult {
    println!("order event {}", event.id);
    HandlerResult::Ack
}
// --8<-- [end:multi]

// --8<-- [start:pattern]
// A `^`-anchored librdkafka regex subscribes to every matching topic; topics created later are
// picked up on the next metadata refresh.
#[subscriber(KafkaTopic::pattern("^audit\\..*").group("audit-svc").start(StartOffset::Earliest))]
async fn on_audit(event: &OrderEvent) -> HandlerResult {
    println!("audit event {}", event.id);
    HandlerResult::Ack
}
// --8<-- [end:pattern]

#[ruststream::app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(on_order_event);
        b.include(on_audit);
    })
}
