//! Shutdown: the runtime flushes what the handlers published, bounded by `flush_timeout`. The
//! in-process test runs everywhere; the live one runs against a real cluster and skips without
//! one.
//!
//! ```text
//! cargo test --example kafka_broker_shutdown --features testing
//! just brokers-up
//! KAFKA_TEST_URL=127.0.0.1:9092 cargo test --example kafka_broker_shutdown --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_rdkafka::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[subscriber("broker-shutdown-orders", reply)]
async fn ship(order: &Order) -> Shipment {
    Shipment { order_id: order.id }
}

fn service(broker: KafkaBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(ship);
    })
}

// --8<-- [start:broker]
/// The cluster at `servers`; shutdown waits up to ten seconds for in-flight records.
fn broker(servers: &str) -> KafkaBroker {
    KafkaBroker::new([servers])
        .default_group("orders-svc")
        .flush_timeout(Duration::from_secs(10))
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(broker("localhost:9092"))
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "broker-shutdown-orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "broker-shutdown-shipments")]
struct Shipment {
    order_id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;
    use std::time::Duration;

    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::error::RDKafkaErrorCode;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Order, Shipment, app, broker, live, service};

    /// The body both modes run: the shipment is on its topic, and shutdown flushes in time.
    async fn ships_then_shuts_down(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .published::<Shipment>("broker-shutdown-shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        ships_then_shuts_down(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(servers) = live::url("KAFKA_TEST_URL") else {
            return Ok(());
        };
        ensure_topic(&servers, "broker-shutdown-orders").await?;
        let group = format!("broker-shutdown-{}", process::id());
        let app = service(broker(&servers).default_group(group));
        ships_then_shuts_down(TestApp::start_live_within(app, Duration::from_secs(30)).await?).await
    }

    /// Creates `topic` unless the cluster has it: a group assigned a topic created after it
    /// subscribed reads it from `auto.offset.reset`, the end of the log by default.
    async fn ensure_topic(servers: &str, topic: &str) -> Result<(), Box<dyn Error>> {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", servers)
            .create()?;
        let topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
        for created in admin.create_topics([&topic], &AdminOptions::new()).await? {
            match created {
                Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((name, code)) => return Err(format!("creating {name} failed: {code}").into()),
            }
        }
        Ok(())
    }
}
