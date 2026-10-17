//! The payouts service of the Commits and delivery guarantees guide, written without the macros:
//! each handler is a type with an `impl Handle`, mounted with the `subscriber` constructor, and
//! `main` is written by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_commits -- run
//! ```

use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
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

/// The clients the handlers share, opened once at startup.
#[derive(Debug, Default, Clone)]
struct Clients {
    /// The bank's payout API.
    bank: Arc<Remote>,
    /// The analytics store that counts page views.
    counters: Arc<Remote>,
}

// --8<-- [start:tracked]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
struct Payout {
    id: u64,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Paid {
    payout_id: u64,
}

impl OutgoingDestination for Paid {
    type Form = FixedName;

    const DESTINATION: &'static str = "payouts.done";
}

impl MessageHeaders for Paid {
    type Contract = NoHeaders;
}

impl MessageInfo for Paid {
    const NAME: &'static str = "Paid";
}

struct PayOut;

impl Handle<Payout, Paid, (), (), Clients> for PayOut {
    async fn handle(
        &self,
        payout: &Payout,
        _outs: &(),
        ctx: &mut Context<'_, (), Clients>,
    ) -> Result<Paid, HandlerOutcome> {
        if payout.amount_cents == 0 {
            return Err(HandlerOutcome::drop());
        }
        if ctx.state().bank.call().await.is_err() {
            tracing::warn!(payout.id, "bank unavailable, payout comes back");
            return Err(HandlerOutcome::retry());
        }
        tracing::info!(payout.id, payout.amount_cents, "payout sent");
        Ok(Paid {
            payout_id: payout.id,
        })
    }
}
// --8<-- [end:tracked]

// --8<-- [start:auto]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
struct PageView {
    page: String,
}

struct CountView;

impl Handle<PageView, (), (), (), Clients> for CountView {
    async fn handle(
        &self,
        view: &PageView,
        _outs: &(),
        ctx: &mut Context<'_, (), Clients>,
    ) -> Result<(), HandlerOutcome> {
        if ctx.state().counters.call().await.is_err() {
            // Under `Commit::Auto` nothing brings the view back; analytics accepts the loss.
            return Err(HandlerOutcome::retry());
        }
        tracing::debug!(page = %view.page, "page view counted");
        Ok(())
    }
}
// --8<-- [end:auto]

// --8<-- [start:batch]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
struct StatementLine {
    payout_id: u64,
    amount_cents: u64,
}

struct Reconcile;

impl Handle<[StatementLine], (), (), (), Clients> for Reconcile {
    fn handle(
        &self,
        lines: &[StatementLine],
        _outs: &(),
        _ctx: &mut Context<'_, (), Clients>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        if lines.iter().all(|line| line.amount_cents > 0) {
            return ready(Ok(()));
        }
        let outcomes = lines
            .iter()
            .map(|line| {
                if line.amount_cents == 0 {
                    tracing::warn!(line.payout_id, "empty statement line dropped");
                    return HandlerOutcome::drop();
                }
                HandlerOutcome::ack()
            })
            .collect();
        ready(Err(outcomes))
    }
}
// --8<-- [end:batch]

// --8<-- [start:app]
fn app() -> impl App<State = Clients> {
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("payouts")
        .producer_config("enable.idempotence", "true")
        .flush_timeout(Duration::from_secs(10));
    RustStream::new(AppInfo::new("payouts", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Clients::default()))
        .with_broker(broker, |b| {
            b.include(
                subscriber("payouts", PayOut)
                    .map_source(|_| KafkaTopic::new("payouts").commit(Commit::Tracked))
                    .reply()
                    .build(),
            )
            .out_reply(Publish::default().queue_timeout(Duration::from_secs(5)));
            b.include(
                subscriber("page-views", CountView)
                    .map_source(|_| KafkaTopic::new("page-views").group("analytics"))
                    .build(),
            );
            b.include(
                subscriber("statements", Reconcile)
                    .batch(nonzero!(500usize))
                    .map_source(|_| {
                        KafkaTopic::new("statements")
                            .group("reconciliation")
                            .commit(Commit::Tracked)
                    })
                    .build(),
            );
        })
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rdkafka::prelude::*;
    use serde::Serialize;

    use super::{Clients, PageView, Paid, Payout, Remote, app};

    #[derive(Serialize)]
    struct Requested {
        id: u64,
        amount_cents: u64,
    }

    impl OutgoingDestination for Requested {
        type Form = FixedName;

        const DESTINATION: &'static str = "payouts";
    }

    impl MessageHeaders for Requested {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Requested {
        const NAME: &'static str = "Requested";
    }

    #[derive(Serialize)]
    struct Viewed {
        page: String,
    }

    impl OutgoingDestination for Viewed {
        type Form = FixedName;

        const DESTINATION: &'static str = "page-views";
    }

    impl MessageHeaders for Viewed {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Viewed {
        const NAME: &'static str = "Viewed";
    }

    #[derive(Serialize)]
    struct Line {
        payout_id: u64,
        amount_cents: u64,
    }

    impl OutgoingDestination for Line {
        type Form = FixedName;

        const DESTINATION: &'static str = "statements";
    }

    impl MessageHeaders for Line {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Line {
        const NAME: &'static str = "Line";
    }

    #[tokio::test]
    async fn a_payout_the_bank_refused_comes_back() -> Result<(), Box<dyn Error>> {
        let clients = Clients {
            bank: Arc::new(Remote::down_for(1)),
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
            counters: Arc::new(Remote::down_for(1)),
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
