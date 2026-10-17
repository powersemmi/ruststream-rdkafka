//! Retries on a subscription that reads several topics: no single topic reaches it again, so the
//! mount site says where a retry copy goes, back to the topic it came from or to a fixed one.
//!
//! ```text
//! cargo test --example kafka_settlement_source_topic --features testing
//! cargo run --example kafka_settlement_source_topic -- run
//! ```

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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

/// What the handlers share, built once at startup; each field is injectable with `State<T>`.
#[derive(Clone, Default, FromRef)]
struct AppState {
    warehouse: Arc<Warehouse>,
    ledger: Arc<Ledger>,
}

// --8<-- [start:handler]
/// One subscription reads both regions. Reading the topic is also what hands the Kafka context
/// to the retry position, where `ToSourceTopic` reads it.
#[subscriber(KafkaTopics::new(["orders-eu", "orders-us"]).group("orders-svc").commit(Commit::Tracked))]
async fn reserve(
    order: &Order,
    Ctx(topic): Ctx<Topic>,
    State(warehouse): State<Arc<Warehouse>>,
) -> HandlerOutcome {
    if let Err(err) = warehouse.reserve(order) {
        tracing::warn!(order.id, %topic, err, "warehouse unavailable, retrying");
        return HandlerOutcome::retry();
    }
    tracing::info!(order.id, %topic, "stock reserved");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:pattern]
/// A pattern reads every ledger topic there is; its copies go to one retry topic the pattern
/// also matches, so they come back to this subscription.
#[subscriber(KafkaTopics::pattern("^ledger\\..*").group("ledger-svc").commit(Commit::Tracked))]
async fn post_entry(entry: &Entry, State(ledger): State<Arc<Ledger>>) -> HandlerOutcome {
    if let Err(err) = ledger.post(entry) {
        tracing::warn!(entry.order_id, err, "ledger unavailable, retrying");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:pattern]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = AppState> {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(AppState::default()))
        .with_broker(broker, |b| {
            // A copy goes back to the topic its delivery arrived on.
            b.include(reserve)
                .max_attempts(nonzero!(3u32))
                .dead_letter("dlq.orders")
                .out_retry(Publish::default())
                .transform(ToSourceTopic);
            // Every copy goes to one topic. `.to(topic)` belongs to the publisher the copies
            // leave through, so it follows `out_retry`.
            b.include(post_entry)
                .max_attempts(nonzero!(3u32))
                .dead_letter("dlq.ledger")
                .out_retry(Publish::default())
                .to("ledger.retry");
        })
}
// --8<-- [end:app]

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
