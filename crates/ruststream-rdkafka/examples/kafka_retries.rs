//! Retry policies and dead-lettering: `Retry::Topic` republishes a failed delivery to a retry
//! topic and settles the original (the partition keeps flowing), `max_deliveries` caps the
//! attempts, and the drop path routes poison messages to a dead-letter topic stamped with the
//! origin of the failed delivery.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_retries -- run
//! ```

use std::convert::Infallible;

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream, State};
use ruststream::{FromRef, subscriber};
use ruststream_rdkafka::{
    Commit, DLQ_SOURCE_TOPIC_HEADER, KafkaBroker, KafkaTopic, Retry, StartOffset,
};
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

// --8<-- [start:retry_topic]
// `and_topic` puts the retry topic on the same subscription, so retried copies come back to
// this very handler: a fresh delivery arrives from "payments", each retry from
// "payments.retry" with the attempt count riding in the `kafka-retry-count` header. Once the
// next retry would exceed `max_deliveries`, the drop path dead-letters the message instead of
// republishing it again.
#[subscriber(
    KafkaTopic::new("payments")
        .and_topic("payments.retry")
        .group("payments-svc")
        .commit(Commit::Tracked)
        .retry(Retry::Topic("payments.retry".into()))
        .max_deliveries(5)
        .dead_letter("payments.dlq")
)]
async fn charge(payment: &Payment, State(gateway): State<PaymentGateway>) -> HandlerResult {
    if payment.amount_cents <= 0 {
        // Malformed input is not worth retrying: `drop()` takes the drop path straight to the
        // dead-letter topic.
        return HandlerResult::drop();
    }
    match gateway.charge(payment).await {
        Ok(()) => HandlerResult::Ack,
        // A transient failure: republish to "payments.retry" and settle the original, so the
        // partition keeps flowing past it.
        Err(err) => {
            eprintln!("payment {} failed: {err}; retrying", payment.id);
            HandlerResult::retry()
        }
    }
}
// --8<-- [end:retry_topic]

// --8<-- [start:seek_back]
// `Retry::SeekBack` re-consumes the failed delivery in place: redelivery is immediate and the
// partition order holds (nothing overtakes the failed message), at the price of replaying
// everything after it on that partition and of the attempt count only surviving within the
// session.
#[subscriber(
    KafkaTopic::new("ledger")
        .group("ledger-svc")
        .commit(Commit::Tracked)
        .retry(Retry::SeekBack)
        .max_deliveries(3)
        .dead_letter("ledger.dlq")
)]
async fn post_entry(payment: &Payment) -> HandlerResult {
    println!("posting ledger entry for payment {}", payment.id);
    HandlerResult::Ack
}
// --8<-- [end:seek_back]

// --8<-- [start:dead_letter]
// The dead-letter consumer: dropped messages arrive stamped with the origin of the failed
// delivery in the `kafka-dlq-source-*` headers, so alerting can trace them back without
// parsing payloads.
#[subscriber(KafkaTopic::new("payments.dlq").group("payments-dlq").start(StartOffset::Earliest))]
async fn on_dead_letter(payment: &Payment, ctx: &mut Context<'_>) -> HandlerResult {
    let source = ctx
        .headers()
        .get_str(DLQ_SOURCE_TOPIC_HEADER)
        .unwrap_or("<unknown>");
    println!("payment {} dead-lettered from {source}", payment.id);
    HandlerResult::Ack
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
            b.include(charge);
            b.include(post_entry);
            b.include(on_dead_letter);
        })
}
