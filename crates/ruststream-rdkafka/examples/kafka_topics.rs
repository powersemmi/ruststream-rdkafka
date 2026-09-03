//! Topic descriptors: consumer group, start offset, commit mode, and raw config passthrough,
//! plus a publishing handler whose return value lands on another topic.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_topics -- run
//! ```

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize)]
struct Confirmation {
    id: u64,
    accepted: bool,
}

// --8<-- [start:descriptor]
// Everything besides the topic is optional: unset options mean the librdkafka defaults. The
// group overrides the broker-wide `default_group`; `Commit::Tracked` turns `ack` into a precise
// per-message acknowledgement backed by the stored position; `config` reaches any consumer
// property this crate does not surface as a typed option.
#[subscriber(
    KafkaTopic::new("orders")
        .group("orders-workers")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
        .assignment(Assignment::CooperativeSticky)
        .config("fetch.min.bytes", "1024"),
    publish("confirmations")
)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: true,
    }
}
// --8<-- [end:descriptor]

// --8<-- [start:assign]
// Manual assignment: consume exactly these partitions - no group membership, no rebalancing.
// This reader names no group, so it cannot commit and the start offset must be explicit; add
// `.group("...")` to commit positions into a group without joining it.
#[subscriber(
    KafkaTopic::new("orders")
        .partitions([0])
        .start(StartOffset::Earliest)
)]
async fn audit_partition_zero(order: &Order) -> HandlerOutcome {
    println!("partition 0 saw order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:assign]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    // `KafkaBroker::new` records configuration and does no I/O; the runtime connects it once
    // at startup, then opens these subscriptions against the connected form.
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // `confirm` replies through the broker's default publish policy, so the include site
        // names no publisher; the explicit spelling is
        // `.publisher(TypedPublisher::new(Publish::default()))`.
        b.include(confirm);
        b.include(audit_partition_zero);
    })
}
// --8<-- [end:app]
