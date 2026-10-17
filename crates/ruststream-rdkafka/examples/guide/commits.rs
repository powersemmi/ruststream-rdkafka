//! The payouts service of the Commits and delivery guarantees guide: payouts commit precisely,
//! page views keep librdkafka's own commits, and statement lines settle one by one in batches.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_commits -- run
//! ```

use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

/// A call that reached a remote service while it was down.
#[derive(Debug)]
struct Unavailable;

/// Stands in for a remote service's client.
#[derive(Debug, Default)]
struct Remote {
    /// How many more calls find the service down.
    outages: Mutex<u32>,
}

impl Remote {
    async fn call(&self) -> Result<(), Unavailable> {
        tokio::task::yield_now().await;
        if take_one(&self.outages) {
            return Err(Unavailable);
        }
        Ok(())
    }

    #[cfg(test)]
    fn down_for(calls: u32) -> Self {
        Self {
            outages: Mutex::new(calls),
        }
    }
}

/// Takes one from `counter` if any is left, reporting whether it did.
fn take_one(counter: &Mutex<u32>) -> bool {
    let mut left = counter.lock().unwrap_or_else(PoisonError::into_inner);
    let took = *left > 0;
    if took {
        *left -= 1;
    }
    took
}

/// The bank's payout API.
#[derive(Debug, Default)]
struct Bank(Remote);

/// The analytics store that counts page views.
#[derive(Debug, Default)]
struct Counters(Remote);

/// The clients the handlers share, opened once at startup.
#[derive(Debug, Default, Clone, FromRef)]
struct Clients {
    bank: Arc<Bank>,
    counters: Arc<Counters>,
}

// --8<-- [start:tracked]
#[derive(Debug, PartialEq, Deserialize)]
struct Payout {
    id: u64,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "payouts.done")]
struct Paid {
    payout_id: u64,
}

#[subscriber(KafkaTopic::new("payouts").commit(Commit::Tracked), reply)]
async fn pay_out(payout: &Payout, State(bank): State<Arc<Bank>>) -> Result<Paid, HandlerOutcome> {
    if payout.amount_cents == 0 {
        return Err(HandlerOutcome::drop());
    }
    if bank.0.call().await.is_err() {
        tracing::warn!(payout.id, "bank unavailable, payout comes back");
        return Err(HandlerOutcome::retry());
    }
    tracing::info!(payout.id, payout.amount_cents, "payout sent");
    Ok(Paid {
        payout_id: payout.id,
    })
}
// --8<-- [end:tracked]

// --8<-- [start:auto]
#[derive(Debug, PartialEq, Deserialize)]
struct PageView {
    page: String,
}

#[subscriber(KafkaTopic::new("page-views").group("analytics"))]
async fn count_view(view: &PageView, State(counters): State<Arc<Counters>>) -> HandlerOutcome {
    if counters.0.call().await.is_err() {
        // Under `Commit::Auto` nothing brings the view back; analytics accepts the loss.
        return HandlerOutcome::retry();
    }
    tracing::debug!(page = %view.page, "page view counted");
    HandlerOutcome::ack()
}
// --8<-- [end:auto]

// --8<-- [start:batch]
#[derive(Debug, PartialEq, Deserialize)]
struct StatementLine {
    payout_id: u64,
    amount_cents: u64,
}

#[subscriber(KafkaTopic::new("statements").group("reconciliation").commit(Commit::Tracked))]
async fn reconcile(lines: &[StatementLine]) -> Vec<HandlerOutcome> {
    lines
        .iter()
        .map(|line| {
            if line.amount_cents == 0 {
                tracing::warn!(line.payout_id, "empty statement line dropped");
                return HandlerOutcome::drop();
            }
            HandlerOutcome::ack()
        })
        .collect()
}
// --8<-- [end:batch]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = Clients> {
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("payouts")
        .producer_config("enable.idempotence", "true")
        .flush_timeout(Duration::from_secs(10));
    RustStream::new(AppInfo::new("payouts", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Clients::default()))
        .with_broker(broker, |b| {
            b.include(pay_out)
                .out_reply(Publish::default().queue_timeout(Duration::from_secs(5)));
            b.include(count_view);
            b.include(reconcile.batch(nonzero!(500usize)));
        })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rdkafka::prelude::*;
    use serde::Serialize;

    use super::{Bank, Clients, Counters, PageView, Paid, Payout, Remote, app};

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "payouts")]
    struct Requested {
        id: u64,
        amount_cents: u64,
    }

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "page-views")]
    struct Viewed {
        page: String,
    }

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "statements")]
    struct Line {
        payout_id: u64,
        amount_cents: u64,
    }

    #[tokio::test]
    async fn a_payout_the_bank_refused_comes_back() -> Result<(), Box<dyn Error>> {
        let clients = Clients {
            bank: Arc::new(Bank(Remote::down_for(1))),
            ..Clients::default()
        };
        let tb = TestApp::with_state(app(), |_| clients).await?;

        tb.broker::<KafkaBroker>()
            .message(&Requested {
                id: 1,
                amount_cents: 5000,
            })
            .publish()
            .await?;

        let broker = tb.broker::<KafkaBroker>();
        let payouts = broker.subscriber("payouts");
        assert_eq!(payouts.outcomes(), [Outcome::Nack, Outcome::Ack]);
        payouts.assert_called(2).with(&Payout {
            id: 1,
            amount_cents: 5000,
        });
        tb.broker::<KafkaBroker>()
            .published::<Paid>("payouts.done")
            .assert_called_once()
            .with(&Paid { payout_id: 1 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_page_view_the_store_refused_does_not_come_back() -> Result<(), Box<dyn Error>> {
        let clients = Clients {
            counters: Arc::new(Counters(Remote::down_for(1))),
            ..Clients::default()
        };
        let tb = TestApp::with_state(app(), |_| clients).await?;

        tb.broker::<KafkaBroker>()
            .message(&Viewed {
                page: "/pricing".to_owned(),
            })
            .publish()
            .await?;

        let broker = tb.broker::<KafkaBroker>();
        let views = broker.subscriber("page-views");
        assert_eq!(views.outcomes(), [Outcome::Nack]);
        views.assert_called_once().with(&PageView {
            page: "/pricing".to_owned(),
        });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn statement_lines_settle_one_by_one() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (payout_id, amount_cents) in [(1, 5000), (2, 0)] {
            tb.broker::<KafkaBroker>()
                .message(&Line {
                    payout_id,
                    amount_cents,
                })
                .publish()
                .await?;
        }

        let broker = tb.broker::<KafkaBroker>();
        let statements = broker.subscriber("statements");
        assert_eq!(statements.outcomes(), [Outcome::Ack, Outcome::Drop]);
        tb.shutdown().await?;
        Ok(())
    }
}
