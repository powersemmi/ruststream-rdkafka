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
// `KafkaTopics` names the set the subscription reads: one consumer joins the group for both.
// Each delivery still says which topic it came from, through the `Topic` context key.
#[subscriber(KafkaTopics::new(["orders", "cancellations"]).group("orders-svc"))]
async fn on_order_event(event: &OrderEvent, Ctx(topic): Ctx<Topic>) -> HandlerOutcome {
    println!("order event {} from {topic}", event.id);
    HandlerOutcome::ack()
}
// --8<-- [end:multi]

// --8<-- [start:pattern]
// A `^`-anchored librdkafka regex subscribes to every matching topic; topics created later are
// picked up on the next metadata refresh.
#[subscriber(KafkaTopics::pattern("^audit\\..*").group("audit-svc").start(StartOffset::Earliest))]
async fn on_audit(event: &OrderEvent, Ctx(topic): Ctx<Topic>) -> HandlerOutcome {
    println!("audit event {} from {topic}", event.id);
    HandlerOutcome::ack()
}
// --8<-- [end:pattern]

// --8<-- [start:naming_transform]
// A subscription over a set of topics addresses no retry copy, so every registration over one
// names where a copy goes. `ToSourceTopic` names the topic the delivery itself arrived on, so a
// retried order event comes back on its own topic rather than on a shared one.
#[ruststream::app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(on_order_event)
            .out_retry(Publish::default())
            .transform(ToSourceTopic);
        b.include(on_audit)
            .out_retry(Publish::default())
            .transform(ToSourceTopic);
    })
}
// --8<-- [end:naming_transform]
