//! A minimal Kafka service: one `#[subscriber]` handler on one topic.
//!
//! `KafkaBroker::new` only records configuration, so the whole service fits the
//! `#[ruststream::app]` macro. The runtime climbs the lifecycle ladder for it: `Broker::connect`
//! once at startup, then every subscription against the connected broker, then `shutdown`. The
//! generated binary understands `run` and `asyncapi gen`.
//!
//! The bare-string subscriber form consumes the topic named `orders` through the broker's
//! default consumer group (Kafka cannot subscribe without a group). Start a broker first:
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_quickstart -- run
//! ```
//!
//! Publish an order from another terminal:
//!
//! ```text
//! docker exec -i ruststream-kafka /opt/kafka/bin/kafka-console-producer.sh \
//!     --bootstrap-server localhost:9092 --topic orders <<< '{"id":1}'
//! ```

// --8<-- [start:handler]
use ruststream_rdkafka::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
        |b| {
            b.include(handle);
        },
    )
}
// --8<-- [end:app]
