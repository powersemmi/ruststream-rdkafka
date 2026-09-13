//! Retries and dead-lettering on Kafka: the mount site declares the cap and the dead-letter
//! topic, and the runtime publishes the copies, because Kafka holds no record back and counts
//! no deliveries of its own.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_retries -- run
//! ```

use std::convert::Infallible;
use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream_rdkafka::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
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

// --8<-- [start:retry_after]
// A transient failure asks for a later delivery. Kafka cannot hold a record back, so the runtime
// waits out the delay and publishes a copy to the topic this subscription reads, with the
// retry-count header incremented.
#[subscriber(KafkaTopic::new("payments").group("payments-svc").commit(Commit::Tracked))]
async fn charge(payment: &Payment, State(gateway): State<PaymentGateway>) -> HandlerOutcome {
    if payment.amount_cents <= 0 {
        // Malformed input is not worth retrying: dropping it settles the offset, and under a
        // declared dead-letter topic the delivery leaves through it.
        return HandlerOutcome::drop();
    }
    match gateway.charge(payment).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            eprintln!("payment {} failed: {err}; retrying", payment.id);
            HandlerOutcome::retry_after(Duration::from_secs(5))
        }
    }
}
// --8<-- [end:retry_after]

// --8<-- [start:pattern]
// A pattern subscription reads many topics, and a copy published to any one of them would come
// back on the wrong topic, so this registration names its retry destination itself.
#[subscriber(KafkaTopics::pattern("^ledger\\..*").group("ledger-svc").commit(Commit::Tracked))]
async fn post_entry(payment: &Payment) -> HandlerOutcome {
    if payment.amount_cents <= 0 {
        return HandlerOutcome::retry();
    }
    println!("posting ledger entry for payment {}", payment.id);
    HandlerOutcome::ack()
}
// --8<-- [end:pattern]

// --8<-- [start:dead_letter]
// The dead-letter consumer is an ordinary subscription. The framework's retry-count header says
// how many copies the message went through before it was carried here.
#[subscriber(KafkaTopic::new("payments.dlq").group("payments-dlq").start(StartOffset::Earliest))]
async fn on_dead_letter(payment: &Payment, ctx: &mut Context<'_>) -> HandlerOutcome {
    let attempts = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("0");
    println!(
        "payment {} dead-lettered after {attempts} retries",
        payment.id
    );
    HandlerOutcome::ack()
}
// --8<-- [end:dead_letter]

#[ruststream::app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]);
    RustStream::new(AppInfo::new("payments", "0.1.0"))
        .on_startup(|()| async {
            let gateway = PaymentGateway {
                endpoint: "https://gateway.internal".into(),
            };
            Ok::<_, Infallible>(AppState { gateway })
        })
        .with_broker(broker, |b| {
            // --8<-- [start:declaration]
            // Five deliveries of one payment, the first included; the sixth goes to
            // "payments.dlq" instead, payload and headers as they arrived.
            b.include(charge)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments.dlq");
            // --8<-- [end:declaration]
            // --8<-- [start:named]
            // `.to(topic)` belongs to the publisher the copies leave through, so it follows
            // `out_retry`. Without it a pattern subscription refuses to start: it has no topic
            // of its own to publish a copy to.
            b.include(post_entry)
                .max_attempts(nonzero!(3u32))
                .dead_letter("ledger.dlq")
                .out_retry(Publish::default())
                .to("ledger.retry");
            // --8<-- [end:named]
            b.include(on_dead_letter);
        })
}
