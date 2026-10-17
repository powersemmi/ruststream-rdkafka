//! Commit modes on Kafka: a consumer group commits a position in each partition, so what an ack,
//! a drop and a retry mean is chosen per subscription with `Commit`.
//!
//! ```text
//! cargo test --example kafka_settlement_commit --features testing
//! cargo run --example kafka_settlement_commit -- run
//! ```

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
    amount_cents: i64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "page-views")]
struct PageView {
    path: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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

/// What the handlers share, built once at startup; each field is injectable with `State<T>`.
#[derive(Clone, Default, FromRef)]
struct AppState {
    dashboard: Arc<Dashboard>,
    gateway: Arc<Gateway>,
}

// --8<-- [start:commit]
/// The group's position moves past an invoice once the handler has settled it.
#[subscriber(KafkaTopic::new("invoices").group("billing").commit(Commit::Tracked))]
async fn book(invoice: &Invoice) -> HandlerOutcome {
    if invoice.amount_cents <= 0 {
        // Nothing to book: a drop settles the offset like an ack, and nothing comes back.
        tracing::warn!(invoice.id, "invoice without an amount");
        return HandlerOutcome::drop();
    }
    tracing::info!(invoice.id, "invoice booked");
    HandlerOutcome::ack()
}
// --8<-- [end:commit]

// --8<-- [start:auto]
/// A dashboard can lose a few page views in a crash, so this subscription keeps librdkafka's
/// timer commit. The position is stored as the view is handed over, so a retry asks for
/// something the subscription cannot do: the view is not delivered again.
#[subscriber(KafkaTopic::new("page-views").group("analytics"))]
async fn count_view(view: &PageView, State(dashboard): State<Arc<Dashboard>>) -> HandlerOutcome {
    if let Err(err) = dashboard.count(view) {
        tracing::warn!(%err, "page view lost");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:auto]

// --8<-- [start:tracked]
/// A charge the gateway turned away comes back on this subscription before anything behind it,
/// and the group commits nothing past it until it is settled.
#[subscriber(KafkaTopic::new("charges").group("payments").commit(Commit::Tracked))]
async fn take_charge(charge: &Charge, State(gateway): State<Arc<Gateway>>) -> HandlerOutcome {
    if let Err(err) = gateway.charge(charge) {
        tracing::warn!(%err, "charge retried");
        return HandlerOutcome::retry();
    }
    tracing::info!(charge.id, "charge taken");
    HandlerOutcome::ack()
}
// --8<-- [end:tracked]

#[ruststream::app]
fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("billing", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(AppState::default()))
        .with_broker(KafkaBroker::new(["localhost:9092"]), |b| {
            b.include(book);
            b.include(count_view);
            b.include(take_charge);
        })
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
