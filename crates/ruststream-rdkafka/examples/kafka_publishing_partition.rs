//! Pinning one record to a partition: the `partition` step of a publish, ahead of the record key
//! and the partitioner.
//!
//! ```text
//! cargo test --example kafka_publishing_partition --features testing
//! cargo run --example kafka_publishing_partition -- run
//! ```

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "dispatch.orders")]
struct Order {
    id: u64,
    /// The courier lane the order belongs to.
    lane: i32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "dispatch.lanes")]
struct Pickup {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Pickup)]
struct Lanes;

// --8<-- [start:partition]
/// Each partition of `dispatch.lanes` is one courier lane, read by that lane's courier alone, so
/// a pickup goes to the partition its order names.
#[subscriber(KafkaTopic::new("dispatch.orders").group("dispatch"))]
async fn dispatch(
    order: &Order,
    Out(lanes): Out<impl Publisher<Options = KafkaOptions>, Lanes>,
) -> HandlerOutcome {
    let pickup = Pickup { order_id: order.id };
    if let Err(err) = lanes.message(&pickup).partition(order.lane).publish().await {
        tracing::warn!(order.id, order.lane, %err, "pickup not dispatched");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:partition]

/// The service on the cluster `broker` connects to.
fn service(broker: KafkaBroker) -> impl App {
    RustStream::new(AppInfo::new("dispatch", "0.1.0")).with_broker(broker, |b| {
        b.include(dispatch).out(Lanes, Publish::default()).build();
    })
}

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

    use super::{Lanes, Order, Pickup, app, live, service};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pickup_is_pinned_to_its_lane() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 7, lane: 0 })
            .publish()
            .await?;

        tb.out::<Lanes>()
            .assert_called_once()
            .with_options(&KafkaOptions::default().partition(0));
        tb.broker::<KafkaBroker>()
            .published::<Pickup>("dispatch.lanes")
            .assert_called_once()
            .with(&Pickup { order_id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    /// In process every topic has one partition, so a pin to another one is refused, as a
    /// cluster refuses a partition its topic does not have.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lane_the_topic_does_not_have_is_refused() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 8, lane: 2 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("dispatch.orders")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        tb.broker::<KafkaBroker>()
            .published::<Pickup>("dispatch.lanes")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    /// On a topic with three partitions the pickup lands on the one its order names.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_a_pickup_lands_on_the_partition_of_its_lane() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url() else {
            return Ok(());
        };
        live::recreate_topic(&url, "dispatch.orders", 1).await;
        live::recreate_topic(&url, "dispatch.lanes", 3).await;
        // A group forms on the stand in seconds, which the default deadline leaves too little
        // room for.
        let broker = KafkaBroker::new([url.as_str()]);
        let tb = TestApp::start_live_within(service(broker), Duration::from_secs(30)).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 9, lane: 2 })
            .publish()
            .await?;
        tb.settle().await?;

        tb.out::<Lanes>()
            .assert_called_once()
            .with_options(&KafkaOptions::default().partition(2));
        tb.shutdown().await?;
        assert_eq!(
            live::records_per_partition(&url, "dispatch.lanes", 3),
            [0, 0, 1]
        );
        Ok(())
    }
}
