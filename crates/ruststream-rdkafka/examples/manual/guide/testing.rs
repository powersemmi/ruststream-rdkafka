//! The service the Testing guide tests, written without the macros: card payments keyed by
//! account, a receipt for each, and a review topic for the payments the bank keeps holding.
//!
//! ```text
//! cargo test --example manual_guide_testing --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::context::KafkaContext;
use ruststream_rdkafka::context::keys::Key;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../../tests/live/mod.rs"]
mod live;

// --8<-- [start:service]
/// The largest payment the bank settles at once; a larger one is held.
const INSTANT_LIMIT_CENTS: u64 = 100_000;

/// How long a held payment waits before the service asks the bank again.
const HOLD: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    amount_cents: u64,
}

// What `#[derive(Outgoing)]` with `#[outgoing(name = "card-payments")]` writes.
impl OutgoingDestination for Payment {
    type Form = FixedName;

    const DESTINATION: &'static str = "card-payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

impl MessageInfo for Payment {
    const NAME: &'static str = "Payment";
    const DESCRIPTION: Option<&'static str> = None;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    payment: u64,
    account: String,
}

impl OutgoingDestination for Receipt {
    type Form = FixedName;

    const DESTINATION: &'static str = "card-receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

impl MessageInfo for Receipt {
    const NAME: &'static str = "Receipt";
    const DESCRIPTION: Option<&'static str> = None;
}

/// Charges a payment to the account its record key names.
struct Charge;

impl Handle<Payment, Receipt, (), KafkaContext> for Charge {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_, KafkaContext>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        let Some(account) = ctx.context(Key) else {
            tracing::warn!(payment.id, "payment without an account dropped");
            return ready(Err(HandlerOutcome::drop()));
        };
        if payment.amount_cents > INSTANT_LIMIT_CENTS {
            tracing::info!(payment.id, "payment held by the bank");
            return ready(Err(HandlerOutcome::retry_after(HOLD)));
        }
        let account = String::from_utf8_lossy(account).into_owned();
        tracing::info!(payment.id, account, "payment charged");
        ready(Ok(Receipt {
            payment: payment.id,
            account,
        }))
    }
}

fn service(broker: KafkaBroker) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber("card-payments", Charge)
                .map_source(|_| KafkaTopic::new("card-payments").commit(Commit::Tracked))
                .reply()
                .build(),
        )
        .max_attempts(nonzero!(3u32))
        .dead_letter("card-payments.review")
        .out_reply(Publish::default());
    })
}

fn app() -> RustStream {
    service(KafkaBroker::new(["localhost:9092"]).default_group("payments-svc"))
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;
    use std::time::Duration;

    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::error::RDKafkaErrorCode;
    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rdkafka::PARTITION_KEY_HEADER;
    use ruststream_rdkafka::prelude::*;

    use super::{HOLD, Payment, Receipt, live, service};

    // --8<-- [start:in_process]
    #[tokio::test]
    async fn a_keyed_payment_gets_a_receipt() -> Result<(), Box<dyn Error>> {
        let broker = KafkaBroker::new(["localhost:9092"]).default_group("payments-svc");
        let tb = TestApp::start(service(broker)).await?;
        a_receipt_names_the_account_of_the_key(tb).await
    }

    async fn a_receipt_names_the_account_of_the_key(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        let mut key = HeaderMap::new();
        key.insert(PARTITION_KEY_HEADER, "acct-7");
        tb.broker::<KafkaBroker>()
            .message(&Payment {
                id: 1,
                amount_cents: 2_500,
            })
            .with_headers(key)
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<KafkaBroker>()
            .published::<Receipt>("card-receipts")
            .assert_called_once()
            .with(&Receipt {
                payment: 1,
                account: "acct-7".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:in_process]

    // --8<-- [start:keyless]
    #[tokio::test]
    async fn a_payment_without_an_account_is_dropped() -> Result<(), Box<dyn Error>> {
        let broker = KafkaBroker::new(["localhost:9092"]).default_group("payments-svc");
        let tb = TestApp::start(service(broker)).await?;

        tb.broker::<KafkaBroker>()
            .message(&Payment {
                id: 2,
                amount_cents: 2_500,
            })
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<KafkaBroker>()
            .published::<Receipt>("card-receipts")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:keyless]

    // --8<-- [start:held]
    #[tokio::test(start_paused = true)]
    async fn a_payment_the_bank_keeps_holding_goes_to_review() -> Result<(), Box<dyn Error>> {
        let broker = KafkaBroker::new(["localhost:9092"]).default_group("payments-svc");
        let tb = TestApp::start(service(broker)).await?;
        let held = Payment {
            id: 3,
            amount_cents: 250_000,
        };
        let mut key = HeaderMap::new();
        key.insert(PARTITION_KEY_HEADER, "acct-7");

        tb.broker::<KafkaBroker>()
            .message(&held)
            .with_headers(key)
            .publish()
            .await?;
        tb.broker::<KafkaBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(HOLD));

        // The delay is a copy of the record, published back to the topic after `HOLD`.
        tb.advance(HOLD).await?;
        tb.broker::<KafkaBroker>()
            .published::<Payment>("card-payments")
            .assert_called(2)
            .with(&held)
            .with_header(RETRY_COUNT_HEADER, "1");

        // The third delivery is the last one: it goes to the review topic as it arrived.
        tb.advance(HOLD).await?;
        assert_eq!(
            tb.broker::<KafkaBroker>()
                .subscriber("card-payments")
                .outcomes(),
            [Outcome::Nack, Outcome::Nack, Outcome::Nack],
        );
        tb.broker::<KafkaBroker>()
            .published::<Payment>("card-payments.review")
            .assert_called_once()
            .with(&held);
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:held]

    // --8<-- [start:live]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_keyed_payment_gets_a_receipt_on_a_cluster() -> Result<(), Box<dyn Error>> {
        let Some(servers) = live::url("KAFKA_TEST_URL") else {
            return Ok(());
        };
        create_topic(&servers, "card-payments").await?;
        let group = format!("payments-svc-{}", process::id());
        let broker = KafkaBroker::new([servers]).default_group(group);
        let tb = TestApp::start_live_within(service(broker), Duration::from_secs(30)).await?;
        a_receipt_names_the_account_of_the_key(tb).await
    }
    // --8<-- [end:live]

    /// Creates `topic` with one partition, unless the cluster already has it.
    async fn create_topic(servers: &str, topic: &str) -> Result<(), Box<dyn Error>> {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", servers)
            .create()?;
        let created = admin
            .create_topics(
                [&NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await?;
        match created.first() {
            Some(Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists))) | None => Ok(()),
            Some(Err((name, code))) => Err(format!("creating {name} failed: {code}").into()),
        }
    }
}
