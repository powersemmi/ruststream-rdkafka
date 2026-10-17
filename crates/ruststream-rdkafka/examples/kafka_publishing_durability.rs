//! Durability of what a service publishes: the producer's `acks` and idempotence are broker
//! settings, and together with `Commit::Tracked` they give at-least-once from input to output.
//!
//! ```text
//! cargo test --example kafka_publishing_durability --features testing
//! cargo run --example kafka_publishing_durability -- run
//! ```

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    payment_id: u64,
}

// --8<-- [start:durability]
/// The input's offset is committed once the receipt is acknowledged by every in-sync replica,
/// so a crash in between brings the payment back instead of losing the receipt.
#[subscriber(KafkaTopic::new("payments").group("receipts-svc").commit(Commit::Tracked), reply)]
async fn issue_receipt(payment: &Payment) -> Receipt {
    Receipt {
        payment_id: payment.id,
    }
}

#[ruststream::app]
fn app() -> impl App {
    // Every in-sync replica confirms a record, and a retried send is written once.
    let broker = KafkaBroker::new(["localhost:9092"])
        .producer_config("acks", "all")
        .producer_config("enable.idempotence", "true");
    RustStream::new(AppInfo::new("receipts", "0.1.0")).with_broker(broker, |b| {
        b.include(issue_receipt).out_reply(Publish::default());
    })
}
// --8<-- [end:durability]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Payment, Receipt, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_durable_producer_starts_and_delivers_the_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Payment { id: 3 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { payment_id: 3 });
        tb.broker::<KafkaBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
