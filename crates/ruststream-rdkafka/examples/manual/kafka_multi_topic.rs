//! The services of `examples/kafka_multi_topic.rs` written without the macros: a `KafkaTopics`
//! descriptor is the source `subscriber(..)` takes, and the body reads the topic of each
//! delivery off its Kafka context.
//!
//! ```text
//! cargo test --example manual_kafka_multi_topic --features testing
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::schemars::{self, JsonSchema};
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

/// An event about an order, published to whichever topic tells what happened to it.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct OrderEvent {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the topic.
impl OutgoingDestination for OrderEvent {
    type Form = CallerName;
}

impl MessageHeaders for OrderEvent {
    type Contract = NoHeaders;
}

mod topics {
    use std::error::Error;

    use ruststream::schemars::{self, JsonSchema};
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::context::KafkaContext;
    use ruststream_rdkafka::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::OrderEvent;

    /// One line of an order's history: the event, and the topic it was read from.
    #[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
    struct HistoryEntry {
        id: u64,
        topic: String,
    }

    // `#[outgoing(name = "order-history")]` by hand: the reply type declares where it goes.
    impl OutgoingDestination for HistoryEntry {
        type Form = FixedName;

        const DESTINATION: &'static str = "order-history";
    }

    impl MessageHeaders for HistoryEntry {
        type Contract = NoHeaders;
    }

    // --8<-- [start:multi]
    /// Answers each event with a history entry naming the topic it came from.
    struct Record;

    impl Handle<OrderEvent, HistoryEntry, (), KafkaContext> for Record {
        async fn handle(
            &self,
            event: &OrderEvent,
            _outs: &(),
            ctx: &mut Context<'_, KafkaContext>,
        ) -> Result<HistoryEntry, HandlerOutcome> {
            Ok(HistoryEntry {
                id: event.id,
                topic: ctx.context(Topic).to_string(),
            })
        }
    }

    fn app() -> impl App {
        let broker = KafkaBroker::new(["localhost:9092"]);
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            // Two topics through one consumer of the group `order-history`.
            let events = KafkaTopics::new(["orders", "cancellations"]).group("order-history");
            // No one topic reaches a topic set again, so the mount site says where a retried
            // event goes: back to the topic it came from.
            b.include(
                subscriber("orders,cancellations", Record)
                    .reply()
                    .build()
                    .map_source(|_| events),
            )
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
    use ruststream_rdkafka::context::KafkaContext;
    use ruststream_rdkafka::prelude::*;

    use super::OrderEvent;

    // --8<-- [start:pattern]
    /// Archives an event, logging the topic it came from.
    struct Archive;

    impl Handle<OrderEvent, (), (), KafkaContext> for Archive {
        async fn handle(
            &self,
            event: &OrderEvent,
            _outs: &(),
            ctx: &mut Context<'_, KafkaContext>,
        ) -> Result<(), HandlerOutcome> {
            tracing::info!(event.id, topic = %ctx.context(Topic), "archived");
            Ok(())
        }
    }

    fn app() -> impl App {
        let broker = KafkaBroker::new(["localhost:9092"]);
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
            // Every topic whose name starts with `audit.`, those created later included.
            let audit = KafkaTopics::pattern("^audit\\..*").group("audit-archive");
            // A retried event goes back to the topic it came from.
            b.include(
                subscriber("^audit\\..*", Archive)
                    .build()
                    .map_source(|_| audit),
            )
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
