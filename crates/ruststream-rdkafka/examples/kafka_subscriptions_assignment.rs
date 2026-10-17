//! The group's rebalance strategy: a consumer of a cooperative sticky group reads every record of
//! a topic once. The scenario runs in process, and against a cluster on a topic with two
//! partitions, where the group hands both to the one member.
//!
//! ```text
//! KAFKA_TEST_URL=127.0.0.1:9092 cargo test --example kafka_subscriptions_assignment --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "warehouse.shipments")]
struct Shipment {
    id: u64,
}

// --8<-- [start:assignment]
/// Ships each shipment; a rebalance moves only the partitions that change hands.
#[subscriber(
    KafkaTopic::new("warehouse.shipments")
        .start(StartOffset::Earliest)
        .assignment(Assignment::CooperativeSticky)
)]
async fn ship(shipment: &Shipment) -> HandlerOutcome {
    tracing::info!(shipment.id, "shipped");
    HandlerOutcome::ack()
}
// --8<-- [end:assignment]

fn app(servers: &str, group: &str) -> impl App<State = ()> {
    let broker = KafkaBroker::new([servers]).default_group(group);
    RustStream::new(AppInfo::new("warehouse", "0.1.0")).with_broker(broker, |b| {
        b.include(ship);
    })
}

#[cfg(test)]
#[path = "support/kafka_subscriptions_live.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::HeaderMap;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::PARTITION_KEY_HEADER;

    use super::*;

    /// Publishes four shipments under four keys, which spread them over the partitions.
    async fn ship_four(tb: &TestApp<()>) -> Result<(), Box<dyn Error>> {
        for id in 1..=4 {
            let mut headers = HeaderMap::new();
            headers.insert(PARTITION_KEY_HEADER, format!("customer-{id}"));
            tb.broker::<KafkaBroker>()
                .message(&Shipment { id })
                .with_headers(headers)
                .publish()
                .await?;
        }
        Ok(())
    }

    /// The ids the group handled, each once.
    fn shipped(tb: &TestApp<()>) -> Vec<u64> {
        let mut ids: Vec<u64> = tb
            .broker::<KafkaBroker>()
            .subscriber("warehouse.shipments")
            .assert_called(4)
            .received::<Shipment>()
            .into_iter()
            .map(|shipment| shipment.id)
            .collect();
        ids.sort_unstable();
        ids
    }

    #[tokio::test]
    async fn the_group_ships_each_shipment_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app("localhost:9092", "warehouse")).await?;

        ship_four(&tb).await?;

        assert_eq!(shipped(&tb), [1, 2, 3, 4]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_member_reads_both_partitions_on_a_cluster() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url() else {
            return Ok(());
        };
        live::recreate_topic(&url, "warehouse.shipments", 2).await;
        let tb =
            TestApp::start_live_within(app(&url, &live::group("warehouse")), live::START).await?;

        ship_four(&tb).await?;

        assert_eq!(shipped(&tb), [1, 2, 3, 4]);
        tb.shutdown().await?;
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
