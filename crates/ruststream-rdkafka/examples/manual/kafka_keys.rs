//! The services of `examples/kafka_keys.rs` written without the macros: the lanes are a setting
//! on the mount chain, `.workers_by_key(n)`, and the descriptor says what keys them.
//!
//! ```text
//! cargo test --example manual_kafka_keys --features testing
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::error::Error;
use std::time::Duration;

use futures::future::join_all;
use ruststream::runtime::{Context, Handle, HandlerOutcome};
use ruststream::schemars::{self, JsonSchema};
use ruststream::testing::TestApp;
use ruststream::{CallerName, HeaderMap, MessageHeaders, NoHeaders, OutgoingDestination};
use ruststream_rdkafka::{KafkaBroker, PARTITION_KEY_HEADER};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
    tenant: String,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the topic.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

/// The body both services mount: what keys the lanes is the descriptor's word.
struct Process;

impl Handle<Order> for Process {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        process(order).await;
        Ok(())
    }
}

/// Stands in for a call to a downstream service: a second of waiting on I/O.
async fn process(order: &Order) {
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!(order.id, order.tenant, "processed");
}

/// Publishes two orders of each of two tenants at once to `topic`, the tenant as the record key,
/// and returns how many seconds they took to process.
async fn two_orders_per_tenant(tb: &TestApp<()>, topic: &str) -> Result<u64, Box<dyn Error>> {
    let orders: Vec<(Order, HeaderMap)> = [(1, "acme"), (2, "globex"), (3, "acme"), (4, "globex")]
        .into_iter()
        .map(|(id, tenant)| {
            let mut headers = HeaderMap::new();
            headers.insert(PARTITION_KEY_HEADER, tenant);
            let order = Order {
                id,
                tenant: tenant.to_owned(),
            };
            (order, headers)
        })
        .collect();

    let started = Instant::now();
    for published in join_all(orders.iter().map(|(order, headers)| {
        tb.broker::<KafkaBroker>()
            .message(order)
            .with_headers(headers.clone())
            .to(topic)
            .publish()
    }))
    .await
    {
        published?;
    }
    Ok(started.elapsed().as_secs())
}

mod by_record_key {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Process, two_orders_per_tenant};

    // --8<-- [start:consumer]
    /// Eight lanes by record key: one tenant's orders run in order, different tenants at once,
    /// even inside one partition.
    fn app() -> impl App<State = ()> {
        let broker = KafkaBroker::new(["localhost:9092"]);
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            let orders = KafkaTopic::new("orders")
                .group("orders-workers")
                .lane_key(LaneKey::RecordKey);
            b.include(
                subscriber("orders", Process)
                    .workers_by_key(nonzero!(8))
                    .build()
                    .map_source(|_| orders),
            );
        })
    }
    // --8<-- [end:consumer]

    #[tokio::test(start_paused = true)]
    async fn tenants_run_side_by_side() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // Each tenant's two orders one after the other, beside the other tenant's.
        assert_eq!(two_orders_per_tenant(&tb, "orders").await?, 2);
        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called(4)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

mod by_partition {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Order, Process, two_orders_per_tenant};

    // --8<-- [start:partition_lanes]
    /// Eight lanes by source partition, the default: everything one partition delivers runs in
    /// order on one lane, keyless records included.
    fn app() -> impl App<State = ()> {
        let broker = KafkaBroker::new(["localhost:9092"]);
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
            let audit = KafkaTopic::new("audit").group("audit-workers");
            b.include(
                subscriber("audit", Process)
                    .workers_by_key(nonzero!(8))
                    .build()
                    .map_source(|_| audit),
            );
        })
    }
    // --8<-- [end:partition_lanes]

    #[tokio::test(start_paused = true)]
    async fn one_partition_runs_in_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // The topic has one partition, so all four orders share one lane.
        assert_eq!(two_orders_per_tenant(&tb, "audit").await?, 4);
        let ids: Vec<u64> = tb
            .broker::<KafkaBroker>()
            .subscriber("audit")
            .received::<Order>()
            .iter()
            .map(|order| order.id)
            .collect();
        assert_eq!(ids, [1, 2, 3, 4]);
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
