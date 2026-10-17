//! The service of `examples/kafka_eos_replies.rs` with its handler written as a `Handle` impl
//! instead of `#[subscriber]`.
//!
//! ```text
//! cargo test --example manual_kafka_eos_replies --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::schemars::{self, JsonSchema};
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    cents: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[outgoing(name = "ledger-entries")]
struct LedgerEntry {
    payment_id: u64,
    cents: u64,
}

// --8<-- [start:handler]
/// Books a payment; an empty one is dropped, and its offset still commits with the window.
struct Book;

impl Handle<Payment, LedgerEntry> for Book {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<LedgerEntry, HandlerOutcome>> {
        if payment.cents == 0 {
            return ready(Err(HandlerOutcome::drop()));
        }
        ready(Ok(LedgerEntry {
            payment_id: payment.id,
            cents: payment.cents,
        }))
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("ledger-svc"),
        |b| {
            let payments = subscriber("payments", Book)
                .map_source(|_| {
                    KafkaTopic::new("payments").commit(Commit::Transactional("ledger-1".into()))
                })
                .reply()
                .workers_by_key(nonzero!(4));
            // The transform carries each delivery's coordinates onto its reply, where the pipeline
            // pairs them.
            b.include(payments.build())
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
