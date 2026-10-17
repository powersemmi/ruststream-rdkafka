//! The prelude: one glob brings the core's prelude, the broker, its descriptors and their
//! settings, and the publish policies under the names every broker crate uses.
//!
//! ```text
//! cargo test --example kafka_broker_prelude --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_rdkafka::prelude::*;

#[subscriber(
    KafkaTopic::new("orders")
        .group("orders-svc")
        .commit(Commit::Tracked)
        .start(StartOffset::Earliest),
    reply
)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]),
        |b| {
            b.include(confirm).out_reply(Publish::default());
        },
    )
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Confirmation, Order, app};

    #[tokio::test]
    async fn an_order_is_confirmed_on_the_reply_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<KafkaBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 7 });
        Ok(())
    }
}
