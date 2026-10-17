//! The service and tests of `examples/kafka_testing.rs` without the macros: the handlers are
//! named types, the descriptor is mounted with `map_source`, and `main` is written by hand. The
//! tests are the same: the harness drives the app whatever form it is written in.
//!
//! ```text
//! cargo run --example manual_kafka_testing --features testing -- run
//! cargo test --example manual_kafka_testing --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_rdkafka::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    amount: u64,
}

// `#[outgoing(name = "payments")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Payment {
    type Form = FixedName;
    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

struct Accept;

impl Handle<Payment> for Accept {
    async fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if payment.amount == 0 {
            return Err(HandlerOutcome::drop());
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Refund {
    order_id: u64,
    lines: u64,
    /// Whether the refund settles or is called off half-way, so one handler shows both paths.
    settle: bool,
}

// `#[outgoing(name = "refunds")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Refund {
    type Form = FixedName;
    const DESTINATION: &'static str = "refunds";
}

impl MessageHeaders for Refund {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct RefundLine {
    order_id: u64,
    line: u64,
}

// `#[outgoing(name = "refund-lines")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for RefundLine {
    type Form = FixedName;
    const DESTINATION: &'static str = "refund-lines";
}

impl MessageHeaders for RefundLine {
    type Contract = NoHeaders;
}

/// The slot the refund lines leave through, inside one transaction.
struct Lines;

impl OutSlot for Lines {
    const NAME: &'static str = "Lines";
    // `RefundLine` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Lines> for RefundLine {}

struct Refunds;

impl<Entry> Handle<Refund, (), Outs<(Entry,)>> for Refunds
where
    Entry: OutEntry<Lines, Wire: TransactionalPublisher>,
{
    async fn handle(
        &self,
        order: &Refund,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let lines = outs.get(Lines);
        if lines.begin_transaction().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        for line in 0..order.lines {
            let entry = RefundLine {
                order_id: order.order_id,
                line,
            };
            if lines.message(&entry).publish().await.is_err() {
                lines.abort().await.ok();
                return Err(HandlerOutcome::retry());
            }
        }
        let settled = if order.settle {
            lines.commit().await
        } else {
            lines.abort().await
        };
        if settled.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("payments-svc"),
        |b| {
            let accept = subscriber("payments", Accept)
                .map_source(|_| KafkaTopic::new("payments").group("payments-svc"));
            b.include(accept.build());
            b.include(subscriber("refunds", Refunds).build())
                .out(Lines, Publish::default().transactional_id("refunds-svc-1"))
                .build();
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
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
