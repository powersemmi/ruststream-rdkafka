//! `EosReplies` on an exactly-once reply: each reply joins the pipeline's open window paired with
//! the offset of the delivery it answers. The scenarios are tests.
//!
//! ```text
//! cargo test --example kafka_eos_replies --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    cents: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "ledger-entries")]
struct LedgerEntry {
    payment_id: u64,
    cents: u64,
}

// --8<-- [start:handler]
/// Books a payment; an empty one is dropped, and its offset still commits with the window.
#[subscriber(
    KafkaTopic::new("payments").commit(Commit::Transactional("ledger-1".into())),
    reply,
    workers(4, by_key)
)]
async fn book(payment: &Payment) -> Result<LedgerEntry, HandlerOutcome> {
    if payment.cents == 0 {
        return Err(HandlerOutcome::drop());
    }
    Ok(LedgerEntry {
        payment_id: payment.id,
        cents: payment.cents,
    })
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("ledger-svc"),
        |b| {
            // The transform carries each delivery's coordinates onto its reply, where the pipeline
            // pairs them.
            b.include(book)
                .out_reply(EosPublish::new("ledger-1"))
                .transform(EosReplies);
        },
    )
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rdkafka::prelude::*;

    use super::{LedgerEntry, Payment, app};

    #[tokio::test(start_paused = true)]
    async fn each_reply_commits_with_the_offset_it_answers() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<KafkaBroker>();
        broker
            .message(&Payment { id: 1, cents: 500 })
            .publish()
            .await?;
        broker
            .message(&Payment { id: 2, cents: 0 })
            .publish()
            .await?;

        broker
            .published::<LedgerEntry>("ledger-entries")
            .assert_called_once()
            .with(&LedgerEntry {
                payment_id: 1,
                cents: 500,
            });
        assert_eq!(
            broker.subscriber("payments").outcomes(),
            [Outcome::Ack, Outcome::Drop],
        );
        tb.shutdown().await?;
        Ok(())
    }
}
