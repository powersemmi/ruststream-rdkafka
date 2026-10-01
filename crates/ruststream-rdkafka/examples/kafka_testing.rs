//! Testing a service in process: the production app, `KafkaBroker` included, run by `TestApp`
//! with no Kafka cluster.
//!
//! The app is the one `main` runs in production. `TestApp::start` connects its broker in
//! process: the same handlers, descriptors, publish policies and consumer-group semantics, over
//! an in-process cluster instead of the network. A publish through the harness returns once
//! every handler it triggered has settled, so the assertions read finished state.
//!
//! ```text
//! cargo run --example kafka_testing --features testing
//! ```

use ruststream::runtime::{App, AppInfo, HandlerOutcome, Out, RustStream};
use ruststream::testing::TestApp;
use ruststream::{OutSlot, Outgoing, TransactionalPublisher, subscriber};
use ruststream_rdkafka::{KafkaBroker, KafkaPublish, KafkaTopic};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    amount: u64,
}

// --8<-- [start:handler]
#[subscriber(KafkaTopic::new("payments").group("payments-svc"))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:transactions]
#[derive(Debug, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "refunds")]
struct Refund {
    order_id: u64,
    lines: u64,
    /// Whether the refund settles or is called off half-way, so one handler shows both paths.
    settle: bool,
}

#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "refund-lines")]
struct RefundLine {
    order_id: u64,
    line: u64,
}

#[derive(OutSlot)]
#[publishes(RefundLine)]
struct Lines;

#[subscriber("refunds")]
async fn refund(
    order: &Refund,
    Out(lines): Out<impl TransactionalPublisher, Lines>,
) -> HandlerOutcome {
    if lines.begin_transaction().await.is_err() {
        return HandlerOutcome::retry();
    }
    for line in 0..order.lines {
        let entry = RefundLine {
            order_id: order.order_id,
            line,
        };
        if lines.message(&entry).publish().await.is_err() {
            lines.abort().await.ok();
            return HandlerOutcome::retry();
        }
    }
    let settled = if order.settle {
        lines.commit().await
    } else {
        lines.abort().await
    };
    if settled.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:transactions]

// --8<-- [start:app]
/// The app `main` runs in production, and the one a test hands the harness unchanged.
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("payments-svc"),
        |b| {
            b.include(accept);
            b.include(refund)
                .out(
                    Lines,
                    KafkaPublish::default().transactional_id("refunds-svc-1"),
                )
                .build();
        },
    )
}
// --8<-- [end:app]

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:testapp]
    let tb = TestApp::start(app()).await?;

    tb.broker::<KafkaBroker>()
        .message(&Payment { amount: 100 })
        .publish()
        .await?;

    tb.broker::<KafkaBroker>()
        .subscriber("payments")
        .assert_called_once()
        .with(&Payment { amount: 100 })
        .settled(HandlerOutcome::ack());
    // --8<-- [end:testapp]

    // --8<-- [start:transaction_asserts]
    // The abort discards the whole fan-out: the slot recorded what the handler sent through it,
    // and the topic - what a `read_committed` reader sees - stayed empty.
    tb.broker::<KafkaBroker>()
        .message(&Refund {
            order_id: 1,
            lines: 3,
            settle: false,
        })
        .publish()
        .await?;
    tb.broker::<KafkaBroker>()
        .published::<RefundLine>("refund-lines")
        .assert_not_called();

    // The commit makes all three visible at once.
    tb.broker::<KafkaBroker>()
        .message(&Refund {
            order_id: 2,
            lines: 3,
            settle: true,
        })
        .publish()
        .await?;
    tb.broker::<KafkaBroker>()
        .published::<RefundLine>("refund-lines")
        .assert_called(3);
    // --8<-- [end:transaction_asserts]

    tb.shutdown().await?;
    println!("all in-process checks passed");
    Ok(())
}
