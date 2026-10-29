//! The services of `examples/kafka_settlement_source_topic.rs` written without `#[subscriber]`:
//! each handler implements `Handle`, and the retry position takes the same steps.
//!
//! ```text
//! cargo test --example manual_kafka_settlement_source_topic --features testing
//! cargo run --example manual_kafka_settlement_source_topic -- run
//! ```

use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::context::KafkaContext;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Entry {
    order_id: u64,
    amount_cents: i64,
}

/// Turns away the next few calls while it is busy.
#[derive(Debug, Default)]
struct Busy {
    calls: AtomicU32,
}

impl Busy {
    fn check(&self) -> Result<(), &'static str> {
        let left = self.calls.load(Ordering::Relaxed);
        if left > 0 {
            self.calls.store(left - 1, Ordering::Relaxed);
            return Err("busy");
        }
        Ok(())
    }
}

/// Stands in for the warehouse stock is reserved in.
#[derive(Debug, Default)]
struct Warehouse {
    busy: Busy,
}

impl Warehouse {
    fn reserve(&self, order: &Order) -> Result<(), &'static str> {
        tracing::debug!(order.id, "reserving stock");
        self.busy.check()
    }
}

/// Stands in for the ledger entries are posted to.
#[derive(Debug, Default)]
struct Ledger {
    busy: Busy,
}

impl Ledger {
    fn post(&self, entry: &Entry) -> Result<(), &'static str> {
        tracing::debug!(entry.order_id, entry.amount_cents, "posting entry");
        self.busy.check()
    }
}

/// What the handlers share, built once at startup and read through `Context::state`.
#[derive(Clone, Default)]
struct AppState {
    warehouse: Arc<Warehouse>,
    ledger: Arc<Ledger>,
}

// --8<-- [start:handler]
/// One subscription reads both regions. A body typed over `KafkaContext` hands that context to
/// the retry position, where `ToSourceTopic` reads it.
struct Reserve;

impl Handle<Order, (), (), KafkaContext, AppState> for Reserve {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, KafkaContext, AppState>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let topic = ctx.context(Topic);
        if let Err(err) = ctx.state().warehouse.reserve(order) {
            tracing::warn!(order.id, %topic, err, "warehouse unavailable, retrying");
            return ready(Err(HandlerOutcome::retry()));
        }
        tracing::info!(order.id, %topic, "stock reserved");
        ready(Ok(()))
    }
}

fn regions() -> KafkaTopics {
    KafkaTopics::new(["orders-eu", "orders-us"])
        .group("orders-svc")
        .commit(Commit::Tracked)
}
// --8<-- [end:handler]

// --8<-- [start:pattern]
/// A pattern reads every ledger topic there is; its copies go to one retry topic the pattern
/// also matches, so they come back to this subscription.
struct PostEntry;

impl Handle<Entry, (), (), (), AppState> for PostEntry {
    fn handle(
        &self,
        entry: &Entry,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if let Err(err) = ctx.state().ledger.post(entry) {
            tracing::warn!(entry.order_id, err, "ledger unavailable, retrying");
            return ready(Err(HandlerOutcome::retry()));
        }
        ready(Ok(()))
    }
}

fn ledgers() -> KafkaTopicPattern {
    KafkaTopics::pattern("^ledger\\..*")
        .group("ledger-svc")
        .commit(Commit::Tracked)
}
// --8<-- [end:pattern]

// --8<-- [start:app]
fn app() -> impl App<State = AppState> {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(AppState::default()))
        .with_broker(broker, |b| {
            // A copy goes back to the topic its delivery arrived on.
            b.include(
                subscriber("orders-eu,orders-us", Reserve)
                    .map_source(|_| regions())
                    .build(),
            )
            .max_attempts(nonzero!(3u32))
            .dead_letter("dlq.orders")
            .out_retry(Publish::default())
            .transform(ToSourceTopic);
            // Every copy goes to one topic. `.to(topic)` belongs to the publisher the copies
            // leave through, so it follows `out_retry`.
            b.include(
                subscriber("^ledger\\..*", PostEntry)
                    .map_source(|_| ledgers())
                    .build(),
            )
            .max_attempts(nonzero!(3u32))
            .dead_letter("dlq.ledger")
            .out_retry(Publish::default())
            .to("ledger.retry");
        })
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::atomic::Ordering;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rdkafka::prelude::*;

    use super::{AppState, Entry, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retry_copy_returns_to_the_topic_its_order_came_from() -> Result<(), Box<dyn Error>> {
        let state = AppState::default();
        state.warehouse.busy.calls.store(1, Ordering::Relaxed);
        let tb = TestApp::with_state(app(), |_| state.clone()).await?;
        tb.broker::<KafkaBroker>()
            .message(&Order { id: 4 })
            .to("orders-us")
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .published::<Order>("orders-us")
            .assert_called(2)
            .with(&Order { id: 4 })
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.broker::<KafkaBroker>()
            .published::<Order>("orders-eu")
            .assert_not_called();
        assert_eq!(
            tb.broker::<KafkaBroker>()
                .subscriber("orders-eu,orders-us")
                .outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pattern_retries_through_its_named_topic_then_dead_letters()
    -> Result<(), Box<dyn Error>> {
        let state = AppState::default();
        state.ledger.busy.calls.store(u32::MAX, Ordering::Relaxed);
        let tb = TestApp::with_state(app(), |_| state.clone()).await?;
        let entry = Entry {
            order_id: 9,
            amount_cents: 2_500,
        };
        tb.broker::<KafkaBroker>()
            .message(&entry)
            .to("ledger.eu")
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .published::<Entry>("ledger.retry")
            .assert_called(2)
            .with(&entry)
            .with_header(RETRY_COUNT_HEADER, "2");
        tb.broker::<KafkaBroker>()
            .published::<Entry>("dlq.ledger")
            .assert_called_once()
            .with(&entry);
        tb.shutdown().await?;
        Ok(())
    }
}
