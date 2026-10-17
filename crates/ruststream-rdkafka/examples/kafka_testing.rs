//! Testing a service in process: the production app, `KafkaBroker` included, run by `TestApp`
//! with no Kafka cluster. A publish through the harness returns once every handler it woke has
//! settled, so the assertions read finished state.
//!
//! ```text
//! cargo run --example kafka_testing --features testing -- run
//! cargo test --example kafka_testing --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_rdkafka::prelude::*;

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    amount: u64,
}

#[subscriber(KafkaTopic::new("payments").group("payments-svc"))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

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

/// The app `main` runs in production, and the one a test hands the harness unchanged.
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("payments-svc"),
        |b| {
            b.include(accept);
            b.include(refund)
                .out(Lines, Publish::default().transactional_id("refunds-svc-1"))
                .build();
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Payment, Refund, RefundLine, app};

    // --8<-- [start:testapp]
    #[tokio::test]
    async fn a_payment_is_accepted() -> Result<(), Box<dyn Error>> {
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
        Ok(())
    }
    // --8<-- [end:testapp]

    // --8<-- [start:transactions]
    #[tokio::test]
    async fn an_aborted_refund_publishes_no_line() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let refund = Refund {
            order_id: 1,
            lines: 3,
            settle: false,
        };
        tb.broker::<KafkaBroker>()
            .message(&refund)
            .publish()
            .await?;

        // A `read_committed` reader, librdkafka's default, never sees an aborted transaction.
        tb.broker::<KafkaBroker>()
            .published::<RefundLine>("refund-lines")
            .assert_not_called();
        Ok(())
    }

    #[tokio::test]
    async fn a_committed_refund_publishes_every_line() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let refund = Refund {
            order_id: 2,
            lines: 3,
            settle: true,
        };
        tb.broker::<KafkaBroker>()
            .message(&refund)
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .published::<RefundLine>("refund-lines")
            .assert_called(3);
        Ok(())
    }
    // --8<-- [end:transactions]
}
