//! Round-robin reply distribution: librdkafka has no round-robin partitioner, and keyless
//! records may stick to one partition for a whole batch. With long, near-constant processing
//! per message that means one busy consumer and idle peers. The `RoundRobin` transform pins
//! every reply to the next partition in the cycle, the evenest spread there is.
//!
//! ```text
//! cargo test --example kafka_distribution --features testing
//! cargo run --example kafka_distribution -- run
//! ```

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "planner.orders")]
struct Order {
    id: u64,
}

// The work item declares the topic it lands on, so the subscriber's clause names none.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "planner.work-items")]
struct WorkItem {
    order_id: u64,
}

// A keyless reply: nothing pins it to a partition, so distribution is the publisher's call.
#[subscriber(KafkaTopic::new("planner.orders").group("planner"), reply)]
async fn plan(order: &Order) -> WorkItem {
    WorkItem { order_id: order.id }
}

// --8<-- [start:round_robin]
/// The service on the cluster `broker` connects to.
fn service(broker: KafkaBroker) -> impl App {
    RustStream::new(AppInfo::new("planner", "0.1.0")).with_broker(broker, |b| {
        // Every reply targets the next partition of the cycle (0, 1, ..., 7, 0, ...), one
        // record each. A reply that already names a partition or carries a record key keeps
        // its placement. The count matches the partitions of `planner.work-items`.
        b.include(plan)
            .out_reply(Publish::default())
            .transform(RoundRobin::partitions(8));
    })
}
// --8<-- [end:round_robin]

#[ruststream::app]
fn app() -> impl App {
    service(KafkaBroker::new(["localhost:9092"]))
}

#[cfg(test)]
#[path = "support/kafka_publishing_live.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Order, WorkItem, app, live, service};

    /// In process a topic has one partition, so the cycle shows on its first step.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_first_reply_is_pinned_to_the_first_partition() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .published::<WorkItem>("planner.work-items")
            .assert_called_once()
            .with(&WorkItem { order_id: 1 })
            .with_options(&KafkaOptions::default().partition(0));
        tb.shutdown().await?;
        Ok(())
    }

    /// On a topic with eight partitions, eight replies land one on each.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_eight_replies_land_one_on_each_partition() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url() else {
            return Ok(());
        };
        live::recreate_topic(&url, "planner.orders", 1).await;
        live::recreate_topic(&url, "planner.work-items", 8).await;
        // A group forms on the stand in seconds, which the default deadline leaves too little
        // room for.
        let broker = KafkaBroker::new([url.as_str()]);
        let tb = TestApp::start_live_within(service(broker), Duration::from_secs(30)).await?;
        for id in 0..8 {
            tb.broker::<KafkaBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }
        tb.settle().await?;

        tb.broker::<KafkaBroker>()
            .published::<WorkItem>("planner.work-items")
            .assert_called(8);
        tb.shutdown().await?;
        assert_eq!(
            live::records_per_partition(&url, "planner.work-items", 8),
            [1; 8]
        );
        Ok(())
    }
}
