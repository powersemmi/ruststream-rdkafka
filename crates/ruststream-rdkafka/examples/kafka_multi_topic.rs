//! One subscription over several topics, and one over every topic a pattern matches: one
//! consumer and one group each, and every delivery still names the topic it came from. Each
//! scenario is a test.
//!
//! ```text
//! cargo test --example kafka_multi_topic --features testing
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::Outgoing;
use serde::{Deserialize, Serialize};

/// An event about an order, published to whichever topic tells what happened to it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct OrderEvent {
    id: u64,
}

mod topics {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::OrderEvent;

    /// One line of an order's history: the event, and the topic it was read from.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "order-history")]
    struct HistoryEntry {
        id: u64,
        topic: String,
    }

    // --8<-- [start:multi]
    /// Reads two topics through one consumer of the group `order-history`.
    #[subscriber(
        KafkaTopics::new(["orders", "cancellations"]).group("order-history"),
        reply
    )]
    async fn record(event: &OrderEvent, Ctx(topic): Ctx<Topic>) -> HistoryEntry {
        HistoryEntry {
            id: event.id,
            topic: topic.to_string(),
        }
    }

    fn app() -> impl App {
        let broker = KafkaBroker::new(["localhost:9092"]);
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            // No one topic reaches a topic set again, so the mount site says where a retried
            // event goes: back to the topic it came from.
            b.include(record)
                .out_retry(Publish::default())
                .transform(ToSourceTopic);
        })
    }
    // --8<-- [end:multi]

    #[tokio::test]
    async fn each_entry_names_the_topic_of_its_event() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<KafkaBroker>();

        broker
            .message(&OrderEvent { id: 1 })
            .to("orders")
            .publish()
            .await?;
        broker
            .message(&OrderEvent { id: 2 })
            .to("cancellations")
            .publish()
            .await?;

        // The subscription's name is its topic list.
        broker.subscriber("orders,cancellations").assert_called(2);
        let history = broker
            .published::<HistoryEntry>("order-history")
            .assert_called(2)
            .decoded();
        assert_eq!(
            history,
            [
                HistoryEntry {
                    id: 1,
                    topic: "orders".to_owned(),
                },
                HistoryEntry {
                    id: 2,
                    topic: "cancellations".to_owned(),
                },
            ],
        );
        Ok(())
    }
}

mod pattern {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::OrderEvent;

    // --8<-- [start:pattern]
    /// Reads every topic whose name starts with `audit.`, those created later included.
    #[subscriber(KafkaTopics::pattern("^audit\\..*").group("audit-archive"))]
    async fn archive(event: &OrderEvent, Ctx(topic): Ctx<Topic>) -> HandlerOutcome {
        tracing::info!(event.id, %topic, "archived");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        let broker = KafkaBroker::new(["localhost:9092"]);
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
            // A retried event goes back to the topic it came from.
            b.include(archive)
                .out_retry(Publish::default())
                .transform(ToSourceTopic);
        })
    }
    // --8<-- [end:pattern]

    #[tokio::test]
    async fn the_pattern_reads_the_topics_it_matches() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<KafkaBroker>();

        for (id, topic) in [(1, "audit.orders"), (2, "audit.payments"), (3, "auditing")] {
            broker
                .message(&OrderEvent { id })
                .to(topic)
                .publish()
                .await?;
        }

        // The subscription's name is its pattern.
        let archived = broker.subscriber("^audit\\..*").received::<OrderEvent>();
        assert_eq!(archived, [OrderEvent { id: 1 }, OrderEvent { id: 2 }]);
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
