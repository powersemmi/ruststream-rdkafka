//! Round-robin reply distribution: librdkafka has no round-robin partitioner, and keyless
//! distribution may batch-stick to one partition - with long, near-constant per-message
//! processing that means one hot consumer and idle peers. The `RoundRobin` transform stamps
//! every reply with the next partition in the cycle, the evenest possible spread.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_distribution -- run
//! ```

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize)]
struct WorkItem {
    order_id: u64,
}

// A keyless reply: nothing pins it to a partition, so distribution is the publisher's call.
#[subscriber("orders", publish("work-items"))]
async fn plan(order: &Order) -> WorkItem {
    WorkItem { order_id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("planner");
    RustStream::new(AppInfo::new("planner", "0.1.0")).with_broker(broker, |b| {
        // --8<-- [start:round_robin]
        // Every reply targets the next partition of the cycle (0, 1, ..., 7, 0, ...), one
        // message each. Replies that already carry an explicit partition or a record key are
        // left alone - keys exist for ordering, and the transform must not break placement
        // the handler chose. The count is explicit and must match the destination topic.
        // The transform is a step of the mount site's chain, right after the policy; the
        // runtime pairs the whole chain into the live publisher once the broker is connected.
        b.include(plan)
            .publisher(Publish::default())
            .transform(RoundRobin::partitions(8));
        // --8<-- [end:round_robin]
    })
}
