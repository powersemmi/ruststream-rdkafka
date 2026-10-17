//! The service of `examples/kafka_distribution.rs` written without `#[subscriber]`: the reply
//! body implements `Handle`, and the reply position takes the same `RoundRobin` transform.
//!
//! ```text
//! cargo test --example manual_kafka_distribution --features testing
//! cargo run --example manual_kafka_distribution -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "planner.orders")]
struct Order {
    id: u64,
}

// The work item declares the topic it lands on, so the mount site names none.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "planner.work-items")]
struct WorkItem {
    order_id: u64,
}

// A keyless reply: nothing pins it to a partition, so distribution is the publisher's call.
struct Plan;

impl Handle<Order, WorkItem> for Plan {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<WorkItem, HandlerOutcome>> {
        ready(Ok(WorkItem { order_id: order.id }))
    }
}

fn orders() -> KafkaTopic {
    KafkaTopic::new("planner.orders").group("planner")
}

// --8<-- [start:round_robin]
/// The service on the cluster `broker` connects to.
fn service(broker: KafkaBroker) -> impl App {
    RustStream::new(AppInfo::new("planner", "0.1.0")).with_broker(broker, |b| {
        let plan = subscriber("planner.orders", Plan).map_source(|_| orders());
        // Every reply targets the next partition of the cycle (0, 1, ..., 7, 0, ...), one
        // record each. A reply that already names a partition or carries a record key keeps
        // its placement. The count matches the partitions of `planner.work-items`.
        b.include(plan.reply().build())
            .out_reply(Publish::default())
            .transform(RoundRobin::partitions(8));
    })
}
// --8<-- [end:round_robin]

fn app() -> impl App {
    service(KafkaBroker::new(["localhost:9092"]))
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
#[path = "../support/kafka_publishing_live.rs"]
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
