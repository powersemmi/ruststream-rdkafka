//! Retries and dead-lettering on Kafka: the mount site declares the cap and the dead-letter
//! topic, and the runtime publishes the copies, because Kafka holds no record back and counts
//! no deliveries of its own.
//!
//! ```text
//! cargo test --example kafka_retries --features testing
//! cargo run --example kafka_retries -- run
//! ```

use std::convert::Infallible;
use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    amount_cents: i64,
}

// A stand-in for a payment gateway client that sometimes fails transiently: wired once at
// startup, injected into handlers through `State`.
#[derive(Clone)]
struct PaymentGateway {
    endpoint: String,
}

impl PaymentGateway {
    async fn charge(&self, payment: &Payment) -> Result<(), String> {
        tokio::task::yield_now().await;
        if payment.amount_cents % 10 == 7 {
            return Err(format!("{} timed out", self.endpoint));
        }
        Ok(())
    }
}

// `#[derive(FromRef)]` makes each state field injectable with `State<FieldType>`.
#[derive(FromRef)]
struct AppState {
    gateway: PaymentGateway,
}

/// How long a payment the gateway timed out on waits before it is charged again.
const RECHECK: Duration = Duration::from_secs(5);

// --8<-- [start:retry_after]
/// A transient failure asks for a later delivery. Kafka cannot hold a record back, so the
/// runtime waits out the delay and publishes a copy to the topic this subscription reads, with
/// the retry-count header incremented.
#[subscriber(KafkaTopic::new("payments").group("payments-svc").commit(Commit::Tracked))]
async fn charge(payment: &Payment, State(gateway): State<PaymentGateway>) -> HandlerOutcome {
    if payment.amount_cents <= 0 {
        // Malformed input is not worth retrying: dropping it settles the offset.
        return HandlerOutcome::drop();
    }
    match gateway.charge(payment).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(payment.id, %err, "charge failed, retrying");
            HandlerOutcome::retry_after(RECHECK)
        }
    }
}
// --8<-- [end:retry_after]

// --8<-- [start:dead_letter]
/// The dead-letter consumer is an ordinary subscription. The framework's retry-count header says
/// how many times the payment was delivered before it was carried here.
#[subscriber(KafkaTopic::new("payments.dlq").group("payments-dlq").start(StartOffset::Earliest))]
async fn on_dead_letter(payment: &Payment, ctx: &mut Context<'_>) -> HandlerOutcome {
    let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
    tracing::error!(payment.id, deliveries, "payment dead-lettered");
    HandlerOutcome::ack()
}
// --8<-- [end:dead_letter]

// --8<-- [start:declaration]
#[ruststream::app]
fn app() -> impl App<State = AppState> {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("payments", "0.1.0"))
        .on_startup(async move |()| {
            let gateway = PaymentGateway {
                endpoint: "https://gateway.internal".into(),
            };
            Ok::<_, Infallible>(AppState { gateway })
        })
        .with_broker(broker, |b| {
            // Five deliveries of one payment, the first included; the fifth failure goes to
            // "payments.dlq" instead, payload and headers as they arrived.
            b.include(charge)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments.dlq");
            b.include(on_dead_letter);
        })
}
// --8<-- [end:declaration]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rdkafka::prelude::*;

    use super::{Payment, RECHECK, app};

    /// The gateway times out on every amount ending in 7.
    const TIMES_OUT: Payment = Payment {
        id: 7,
        amount_cents: 1_007,
    };

    #[tokio::test(start_paused = true)]
    async fn a_timed_out_payment_comes_back_after_the_delay() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&TIMES_OUT)
            .publish()
            .await?;
        tb.broker::<KafkaBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RECHECK));

        tb.advance(RECHECK).await?;

        tb.broker::<KafkaBroker>()
            .subscriber("payments")
            .assert_called(2)
            .with(&TIMES_OUT);
        tb.broker::<KafkaBroker>()
            .published::<Payment>("payments")
            .assert_called(2)
            .with_header(RETRY_COUNT_HEADER, "1");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn the_fifth_failure_is_dead_lettered_and_read_there() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KafkaBroker>()
            .message(&TIMES_OUT)
            .publish()
            .await?;
        for _ in 0..4 {
            tb.advance(RECHECK).await?;
        }

        tb.broker::<KafkaBroker>()
            .subscriber("payments")
            .assert_called(5);
        tb.broker::<KafkaBroker>()
            .published::<Payment>("payments.dlq")
            .assert_called_once()
            .with(&TIMES_OUT)
            .with_header(RETRY_COUNT_HEADER, "5");
        tb.broker::<KafkaBroker>()
            .subscriber("payments.dlq")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_charged_payment_is_acked_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let paid = Payment {
            id: 1,
            amount_cents: 1_000,
        };
        tb.broker::<KafkaBroker>().message(&paid).publish().await?;
        tb.advance(RECHECK).await?;

        assert_eq!(
            tb.broker::<KafkaBroker>().subscriber("payments").outcomes(),
            [Outcome::Ack],
        );
        Ok(())
    }
}
