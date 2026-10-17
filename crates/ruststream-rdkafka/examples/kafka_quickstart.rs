//! A minimal Kafka service: one `#[subscriber]` handler on the topic `orders`, read through the
//! broker's default consumer group.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_quickstart --features testing -- run
//! cargo test --example kafka_quickstart --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_rdkafka::prelude::*;

#[subscriber("orders")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
        |b| {
            b.include(place);
        },
    )
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn an_order_on_the_topic_is_placed() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
