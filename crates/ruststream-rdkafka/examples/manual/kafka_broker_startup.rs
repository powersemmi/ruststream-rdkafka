//! The service of `examples/kafka_broker_startup.rs` without the macros: the handler is a named type
//! and `main` is written by hand.
//!
//! ```text
//! just brokers-up
//! KAFKA_TEST_URL=127.0.0.1:9092 cargo test --example manual_kafka_broker_startup --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_rdkafka::prelude::*;

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

struct Place;

impl Handle<Order> for Place {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        tracing::info!(order.id, "order placed");
        Ok(())
    }
}

fn service(broker: KafkaBroker) -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("broker-startup-orders", Place).build());
    })
}

// --8<-- [start:broker]
/// The cluster at `servers`, with both startup waits bounded.
fn broker(servers: &str) -> KafkaBroker {
    KafkaBroker::new([servers])
        .default_group("orders-svc")
        // The reachability probe: a metadata fetch.
        .connect_timeout(Duration::from_secs(5))
        // Each consumer group assigning this member its partitions.
        .assignment_timeout(Duration::from_secs(20))
}

fn app() -> impl App {
    service(broker("localhost:9092"))
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "broker-startup-orders")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "broker-startup-orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;
    use std::time::{Duration, Instant};

    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::error::RDKafkaErrorCode;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Order, broker, live, service};

    /// A consumer group of this run's own, so nothing an earlier run committed is read.
    fn group(name: &str) -> String {
        format!("broker-startup-{name}-{}", process::id())
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreachable_cluster_fails_startup_within_the_connect_timeout() {
        if live::url("KAFKA_TEST_URL").is_none() {
            return;
        }
        // Nothing listens on port 1, so the probe never gets an answer.
        let started = Instant::now();
        let failed = TestApp::start_live(service(broker("127.0.0.1:1"))).await;

        assert!(failed.is_err(), "startup reached a cluster on port 1");
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "startup gave up after {:?}",
            started.elapsed()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_group_slower_than_the_assignment_timeout_fails_startup() {
        let Some(servers) = live::url("KAFKA_TEST_URL") else {
            return;
        };
        let impatient = broker(&servers)
            .default_group(group("impatient"))
            .assignment_timeout(Duration::from_millis(1));

        let failed = TestApp::start_live(service(impatient))
            .await
            .err()
            .map(|err| err.to_string());
        assert!(
            failed
                .as_deref()
                .is_some_and(|err| err.contains("was not assigned its partitions")),
            "{failed:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_started_group_reads_what_is_published_next() -> Result<(), Box<dyn Error>> {
        let Some(servers) = live::url("KAFKA_TEST_URL") else {
            return Ok(());
        };
        ensure_topic(&servers, "broker-startup-orders").await?;
        let app = service(broker(&servers).default_group(group("orders")));
        let tb = TestApp::start_live_within(app, Duration::from_secs(30)).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("broker-startup-orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
