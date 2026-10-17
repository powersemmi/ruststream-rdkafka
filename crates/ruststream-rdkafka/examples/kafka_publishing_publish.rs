//! The plain publish policy on Kafka: a reply waits for the cluster's delivery report, and
//! `queue_timeout` bounds how long it waits for room in the producer's local queue.
//!
//! ```text
//! cargo test --example kafka_publishing_publish --features testing
//! cargo run --example kafka_publishing_publish -- run
//! ```

use std::time::Duration;

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    order_id: u64,
}

#[subscriber(KafkaTopic::new("orders").group("orders-svc"), reply)]
async fn confirm(order: &Order) -> Confirmation {
    tracing::info!(order.id, "order confirmed");
    Confirmation { order_id: order.id }
}

// --8<-- [start:publish]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]),
        |b| {
            // A full local queue fails the reply after five seconds instead of holding the
            // handler until librdkafka has room again.
            b.include(confirm)
                .out_reply(Publish::default().queue_timeout(Duration::from_secs(5)));
        },
    )
}
// --8<-- [end:publish]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Confirmation, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_reply_lands_on_the_topic_its_type_names() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { order_id: 7 })
            .assert_options_default();
        tb.shutdown().await?;
        Ok(())
    }
}
