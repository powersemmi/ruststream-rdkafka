//! The services of `examples/kafka_batches.rs` written without the macros: a body over `[T]` is
//! a batch handler, and its error side is one outcome per record.
//!
//! ```text
//! cargo test --example manual_kafka_batches --features testing
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::schemars::{self, JsonSchema};
use ruststream::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Payment {
    id: u64,
    settled: bool,
}

// `#[outgoing(name = "payments")]` by hand: the type fixes its destination.
impl OutgoingDestination for Payment {
    type Form = FixedName;

    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

mod whole {
    use std::error::Error;
    use std::future::{Future, ready};

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::Order;

    // --8<-- [start:handler]
    /// Stores a batch of orders in one round-trip; `Ok` settles the whole batch.
    struct Store;

    impl Handle<[Order]> for Store {
        fn handle(
            &self,
            orders: &[Order],
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
            tracing::info!(count = orders.len(), "stored in one round-trip");
            ready(Ok(()))
        }
    }

    fn app() -> impl App<State = ()> {
        let broker = KafkaBroker::new(["localhost:9092"]);
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            let orders = KafkaTopic::new("orders")
                .group("orders-svc")
                .commit(Commit::Tracked);
            // At most 64 orders per call, up to four calls at once.
            b.include(
                subscriber("orders", Store)
                    .batch(nonzero!(64))
                    .workers(nonzero!(4))
                    .build()
                    .map_source(|_| orders),
            );
        })
    }
    // --8<-- [end:handler]

    #[tokio::test]
    async fn three_waiting_orders_arrive_in_one_call() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let orders: Vec<Order> = (1..=3).map(|id| Order { id }).collect();
        // Published at once, the three orders wait in the consumer's queue together.
        let broker = tb.broker::<KafkaBroker>();
        for published in join_all(orders.iter().map(|order| broker.message(order).publish())).await
        {
            published?;
        }

        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

mod per_element {
    use std::error::Error;
    use std::future::{Future, ready};

    use futures::future::join_all;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::Payment;

    /// How long an unsettled payment waits before it is checked again.
    const RECHECK: Duration = Duration::from_secs(30);

    // --8<-- [start:selective]
    /// One outcome per payment, in slice order: only the unsettled ones come back.
    struct Reconcile;

    impl Handle<[Payment]> for Reconcile {
        fn handle(
            &self,
            payments: &[Payment],
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
            ready(Err(payments
                .iter()
                .map(|payment| {
                    if payment.settled {
                        HandlerOutcome::ack()
                    } else {
                        HandlerOutcome::retry_after(RECHECK)
                    }
                })
                .collect()))
        }
    }

    fn app() -> impl App<State = ()> {
        let broker = KafkaBroker::new(["localhost:9092"]);
        RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
            let payments = KafkaTopic::new("payments")
                .group("payments-svc")
                .commit(Commit::Tracked)
                .config("queued.max.messages.kbytes", "1024");
            // A delayed retry is a copy published back to the topic after the delay.
            b.include(
                subscriber("payments", Reconcile)
                    .batch(nonzero!(16))
                    .build()
                    .map_source(|_| payments),
            )
            .out_retry(Publish::default());
        })
    }
    // --8<-- [end:selective]

    #[tokio::test(start_paused = true)]
    async fn only_the_unsettled_payment_comes_back() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let payments: Vec<Payment> = [(1, true), (2, false), (3, true)]
            .into_iter()
            .map(|(id, settled)| Payment { id, settled })
            .collect();
        let broker = tb.broker::<KafkaBroker>();
        for published in join_all(
            payments
                .iter()
                .map(|payment| broker.message(payment).publish()),
        )
        .await
        {
            published?;
        }
        tb.advance(RECHECK).await?;

        let ids: Vec<Vec<u64>> = tb
            .broker::<KafkaBroker>()
            .subscriber("payments")
            .batches::<Payment>()
            .iter()
            .map(|batch| batch.iter().map(|payment| payment.id).collect())
            .collect();
        assert_eq!(ids, [vec![1, 2, 3], vec![2]]);
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
