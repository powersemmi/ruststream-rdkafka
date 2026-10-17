//! The services of `examples/kafka_settlement_commit.rs` written without `#[subscriber]`: each
//! handler implements `Handle`, and the descriptor that chooses the commit mode is the source
//! `subscriber(..)` binds it to.
//!
//! ```text
//! cargo test --example manual_kafka_settlement_commit --features testing
//! cargo run --example manual_kafka_settlement_commit -- run
//! ```

use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
    amount_cents: i64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "page-views")]
struct PageView {
    path: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "charges")]
struct Charge {
    id: u64,
}

/// Stands in for the dashboard store page views are counted in; it can be briefly unavailable.
#[derive(Debug, Default)]
struct Dashboard {
    offline: AtomicBool,
}

impl Dashboard {
    fn count(&self, view: &PageView) -> Result<(), String> {
        if self.offline.load(Ordering::Relaxed) {
            return Err(format!("dashboard offline, {} not counted", view.path));
        }
        Ok(())
    }
}

/// Stands in for a payment gateway that turns away the next few charges while it is busy.
#[derive(Debug, Default)]
struct Gateway {
    busy_for: AtomicU32,
}

impl Gateway {
    fn charge(&self, charge: &Charge) -> Result<(), String> {
        let left = self.busy_for.load(Ordering::Relaxed);
        if left > 0 {
            self.busy_for.store(left - 1, Ordering::Relaxed);
            return Err(format!("gateway busy, charge {} not taken", charge.id));
        }
        Ok(())
    }
}

/// What the handlers share, built once at startup and read through `Context::state`.
#[derive(Clone, Default)]
struct AppState {
    dashboard: Arc<Dashboard>,
    gateway: Arc<Gateway>,
}

// --8<-- [start:commit]
/// The group's position moves past an invoice once the handler has settled it.
struct Book;

impl Handle<Invoice, (), (), (), AppState> for Book {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_, (), AppState>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if invoice.amount_cents <= 0 {
            // Nothing to book: a drop settles the offset like an ack, and nothing comes back.
            tracing::warn!(invoice.id, "invoice without an amount");
            return ready(Err(HandlerOutcome::drop()));
        }
        tracing::info!(invoice.id, "invoice booked");
        ready(Ok(()))
    }
}

fn billing() -> KafkaTopic {
    KafkaTopic::new("invoices")
        .group("billing")
        .commit(Commit::Tracked)
}
// --8<-- [end:commit]

// --8<-- [start:auto]
/// A dashboard can lose a few page views in a crash, so this subscription keeps librdkafka's
/// timer commit. The position is stored as the view is handed over, so a retry asks for
/// something the subscription cannot do: the view is not delivered again.
struct CountView;

impl Handle<PageView, (), (), (), AppState> for CountView {
    fn handle(
        &self,
        view: &PageView,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if let Err(err) = ctx.state().dashboard.count(view) {
            tracing::warn!(%err, "page view lost");
            return ready(Err(HandlerOutcome::retry()));
        }
        ready(Ok(()))
    }
}

fn analytics() -> KafkaTopic {
    KafkaTopic::new("page-views").group("analytics")
}
// --8<-- [end:auto]

// --8<-- [start:tracked]
/// A charge the gateway turned away comes back on this subscription before anything behind it,
/// and the group commits nothing past it until it is settled.
struct TakeCharge;

impl Handle<Charge, (), (), (), AppState> for TakeCharge {
    fn handle(
        &self,
        charge: &Charge,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if let Err(err) = ctx.state().gateway.charge(charge) {
            tracing::warn!(%err, "charge retried");
            return ready(Err(HandlerOutcome::retry()));
        }
        tracing::info!(charge.id, "charge taken");
        ready(Ok(()))
    }
}

fn payments() -> KafkaTopic {
    KafkaTopic::new("charges")
        .group("payments")
        .commit(Commit::Tracked)
}
// --8<-- [end:tracked]

fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("billing", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(AppState::default()))
        .with_broker(KafkaBroker::new(["localhost:9092"]), |b| {
            b.include(
                subscriber("invoices", Book)
                    .map_source(|_| billing())
                    .build(),
            );
            b.include(
                subscriber("page-views", CountView)
                    .map_source(|_| analytics())
                    .build(),
            );
            b.include(
                subscriber("charges", TakeCharge)
                    .map_source(|_| payments())
                    .build(),
            );
        })
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::atomic::Ordering;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rdkafka::prelude::*;

    use super::{AppState, Charge, Invoice, PageView, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_invoice_without_an_amount_is_dropped_and_not_redelivered()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for (id, amount_cents) in [(1, 0), (2, 1_500)] {
            tb.broker::<KafkaBroker>()
                .message(&Invoice { id, amount_cents })
                .publish()
                .await?;
        }

        assert_eq!(
            tb.broker::<KafkaBroker>().subscriber("invoices").outcomes(),
            [Outcome::Drop, Outcome::Ack],
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_page_view_retried_under_auto_commit_does_not_come_back() -> Result<(), Box<dyn Error>>
    {
        let state = AppState::default();
        state.dashboard.offline.store(true, Ordering::Relaxed);
        let tb = TestApp::with_state(app(), |_| state.clone()).await?;
        tb.broker::<KafkaBroker>()
            .message(&PageView {
                path: "/pricing".to_owned(),
            })
            .publish()
            .await?;
        tb.settle().await?;

        tb.broker::<KafkaBroker>()
            .subscriber("page-views")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_charge_turned_away_comes_back_and_is_taken() -> Result<(), Box<dyn Error>> {
        let state = AppState::default();
        state.gateway.busy_for.store(1, Ordering::Relaxed);
        let tb = TestApp::with_state(app(), |_| state.clone()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Charge { id: 7 })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("charges")
            .assert_called(2)
            .with(&Charge { id: 7 })
            .settled(HandlerOutcome::ack());
        assert_eq!(
            tb.broker::<KafkaBroker>().subscriber("charges").outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        tb.shutdown().await?;
        Ok(())
    }
}
