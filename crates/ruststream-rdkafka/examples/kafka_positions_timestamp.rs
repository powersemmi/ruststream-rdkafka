//! A place in the log by time: `KafkaPosition::timestamp` opens a subscription at the first record
//! stamped at or after a moment. The scenario is a test against a cluster, because the records
//! before that moment have to be in the log before the subscription opens.
//!
//! ```text
//! KAFKA_TEST_URL=127.0.0.1:9092 cargo test --example kafka_positions_timestamp --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "metering.readings")]
struct Reading {
    id: u64,
}

// --8<-- [start:replay]
/// Totals meter readings.
#[subscriber("metering.readings")]
async fn total(reading: &Reading) -> HandlerOutcome {
    tracing::info!(reading.id, "totalled");
    HandlerOutcome::ack()
}

/// The service, reading from the first record stamped at or after `since` (epoch milliseconds).
fn app(servers: &str, group: &str, since: i64) -> impl App<State = ()> {
    let broker = KafkaBroker::new([servers]).default_group(group);
    RustStream::new(AppInfo::new("metering", "0.1.0")).with_broker(broker, |b| {
        b.include(total.start_at(KafkaPosition::timestamp(since)));
    })
}
// --8<-- [end:replay]

// The scenario is the test above; the command in the header runs it.
fn main() {}

#[cfg(test)]
#[path = "support/kafka_subscriptions_live.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_subscription_opens_at_the_first_record_since() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url() else {
            return Ok(());
        };
        live::recreate_topic(&url, "metering.readings", 1).await;

        // Reading 1 lands before the moment and reading 2 after it, published by a service that
        // reads nothing.
        let producer = RustStream::new(AppInfo::new("meters", "0.1.0"))
            .with_broker(KafkaBroker::new([url.as_str()]), |_| {});
        let earlier = TestApp::start_live_within(producer, live::START).await?;
        let broker = earlier.broker::<KafkaBroker>();
        broker.message(&Reading { id: 1 }).publish().await?;
        let since = live::now_millis();
        broker.message(&Reading { id: 2 }).publish().await?;
        earlier.shutdown().await?;

        let tb =
            TestApp::start_live_within(app(&url, &live::group("metering"), since), live::START)
                .await?;
        tb.broker::<KafkaBroker>()
            .message(&Reading { id: 3 })
            .publish()
            .await?;

        // A live publish returns once the subscription has handled as many records as the test
        // published, so the first of them is what decides: the first reading since the moment.
        let totalled = tb
            .broker::<KafkaBroker>()
            .subscriber("metering.readings")
            .received::<Reading>();
        assert_eq!(totalled.first(), Some(&Reading { id: 2 }));
        tb.shutdown().await?;
        Ok(())
    }
}
