//! Topic descriptors: consumer group, start offset, commit mode, and raw config passthrough,
//! plus a publishing handler whose return value lands on another topic.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_topics -- run
//! ```

use ruststream::runtime::{App, AppInfo, RustStream, TypedPublisher};
use ruststream::subscriber;
use ruststream_rdkafka::{Assignment, Commit, KafkaBroker, KafkaTopic, StartOffset};
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

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        let confirmations = TypedPublisher::new(b.broker().publisher());
        b.include_publishing(confirm, confirmations);
    })
}
// --8<-- [end:app]
