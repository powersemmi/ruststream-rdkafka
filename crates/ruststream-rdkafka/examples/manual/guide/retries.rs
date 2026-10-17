//! The refunds service of the Retries and dead-letter topics guide, written without the macros:
//! each handler is a type with an `impl Handle`, mounted with the `subscriber` constructor, and
//! `main` is written by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_retries -- run
//! ```

use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::context::KafkaContext;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

/// Why the card processor refused a refund.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// The processor's rate limit is used up for now.
    Busy,
    /// The processor did not answer.
    Unavailable,
    /// The card account is closed; another try gets the same answer.
    Declined,
}

/// Stands in for the card processor's client.
#[derive(Debug, Default)]
struct Processor {
    /// How many more calls find the processor busy.
    busy_for: Mutex<u32>,
    offline: AtomicBool,
}

impl Processor {
    async fn refund(&self, refund: &Refund) -> Result<(), Refusal> {
        tokio::task::yield_now().await;
        if self.offline.load(Ordering::Relaxed) {
            return Err(Refusal::Unavailable);
        }
        if refund.card.is_empty() {
            return Err(Refusal::Declined);
        }
        let busy = {
            let mut busy_for = self.busy_for.lock().unwrap_or_else(PoisonError::into_inner);
            let busy = *busy_for > 0;
            if busy {
                *busy_for -= 1;
            }
            busy
        };
        if busy { Err(Refusal::Busy) } else { Ok(()) }
    }
}

/// The clients the handlers share, opened once at startup.
#[derive(Debug, Default, Clone)]
struct Clients {
    processor: Arc<Processor>,
}

// --8<-- [start:refund]
/// How long a busy processor asks its callers to wait.
const BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Refund {
    id: u64,
    card: String,
    amount_cents: u64,
}

impl OutgoingDestination for Refund {
    type Form = CallerName;
}

impl MessageHeaders for Refund {
    type Contract = NoHeaders;
}

impl MessageInfo for Refund {
    const NAME: &'static str = "Refund";
}

struct ProcessRefund;

impl Handle<Refund, (), (), (), Clients> for ProcessRefund {
    async fn handle(
        &self,
        refund: &Refund,
        _outs: &(),
        ctx: &mut Context<'_, (), Clients>,
    ) -> Result<(), HandlerOutcome> {
        settle(&ctx.state().processor, refund).await
    }
}

async fn settle(processor: &Processor, refund: &Refund) -> Result<(), HandlerOutcome> {
    match processor.refund(refund).await {
        Ok(()) => Ok(()),
        // Past the rate limit: a delivery right now would be refused again.
        Err(Refusal::Busy) => Err(HandlerOutcome::retry_after(BACKOFF)),
        // No answer: try again at once, up to the attempt cap.
        Err(Refusal::Unavailable) => Err(HandlerOutcome::retry()),
        Err(Refusal::Declined) => {
            tracing::warn!(refund.id, "card declined, refund dropped");
            Err(HandlerOutcome::drop())
        }
    }
}
// --8<-- [end:refund]

// --8<-- [start:dead_letters]
struct Review;

impl Handle<Refund, (), (), (), Clients> for Review {
    fn handle(
        &self,
        refund: &Refund,
        _outs: &(),
        ctx: &mut Context<'_, (), Clients>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
        tracing::error!(refund.id, deliveries, "refund needs a person to look at it");
        ready(Ok(()))
    }
}
// --8<-- [end:dead_letters]

// --8<-- [start:regional]
struct ProcessRegionalRefund;

impl Handle<Refund, (), (), KafkaContext, Clients> for ProcessRegionalRefund {
    async fn handle(
        &self,
        refund: &Refund,
        _outs: &(),
        ctx: &mut Context<'_, KafkaContext, Clients>,
    ) -> Result<(), HandlerOutcome> {
        tracing::debug!(refund.id, topic = %ctx.context(Topic), "regional refund");
        settle(&ctx.state().processor, refund).await
    }
}
// --8<-- [end:regional]

// --8<-- [start:app]
fn app() -> impl App<State = Clients> {
    RustStream::new(AppInfo::new("refunds", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Clients::default()))
        .with_broker(
            KafkaBroker::new(["localhost:9092"]).default_group("refunds"),
            |b| {
                // Five deliveries of one refund, the first included; then the dead-letter topic.
                b.include(
                    subscriber("refunds", ProcessRefund)
                        .map_source(|_| KafkaTopic::new("refunds").commit(Commit::Tracked))
                        .build(),
                )
                .max_attempts(nonzero!(5u32))
                .dead_letter("refunds.dlq");
                b.include(
                    subscriber("refunds.dlq", Review)
                        .map_source(|_| {
                            KafkaTopic::new("refunds.dlq")
                                .group("refunds-review")
                                .start(StartOffset::Earliest)
                        })
                        .build(),
                );
                // A set of topics names where a copy goes: back to the topic it came from.
                b.include(
                    subscriber("refunds.eu,refunds.us", ProcessRegionalRefund)
                        .map_source(|_| {
                            KafkaTopics::new(["refunds.eu", "refunds.us"])
                                .group("regional-refunds")
                                .commit(Commit::Tracked)
                        })
                        .build(),
                )
                .max_attempts(nonzero!(5u32))
                .dead_letter("refunds.dlq")
                .out_retry(Publish::default())
                .transform(ToSourceTopic);
            },
        )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rdkafka::prelude::*;

    use super::{BACKOFF, Clients, Processor, Refund, app};

    fn refund(id: u64) -> Refund {
        Refund {
            id,
            card: "4242".to_owned(),
            amount_cents: 1500,
        }
    }

    fn with_processor(busy_for: u32, offline: bool) -> Clients {
        Clients {
            processor: Arc::new(Processor {
                busy_for: Mutex::new(busy_for),
                offline: AtomicBool::new(offline),
            }),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_processor_is_asked_again_after_the_backoff() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_processor(1, false)).await?;

        tb.broker::<KafkaBroker>()
            .message(&refund(1))
            .to("refunds")
            .publish()
            .await?;
        tb.broker::<KafkaBroker>()
            .subscriber("refunds")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(BACKOFF));

        tb.advance(BACKOFF).await?;

        assert_eq!(
            tb.broker::<KafkaBroker>().subscriber("refunds").outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        tb.broker::<KafkaBroker>()
            .published::<Refund>("refunds")
            .assert_called(2)
            .with(&refund(1))
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_processor_sends_the_refund_to_the_dead_letter_topic()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_processor(0, true)).await?;

        tb.broker::<KafkaBroker>()
            .message(&refund(2))
            .to("refunds")
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("refunds")
            .assert_called(5);
        tb.broker::<KafkaBroker>()
            .published::<Refund>("refunds.dlq")
            .assert_called_once()
            .with(&refund(2))
            .with_header(RETRY_COUNT_HEADER, "5");
        tb.broker::<KafkaBroker>()
            .subscriber("refunds.dlq")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_declined_card_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let declined = Refund {
            card: String::new(),
            ..refund(3)
        };

        tb.broker::<KafkaBroker>()
            .message(&declined)
            .to("refunds")
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("refunds")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<KafkaBroker>()
            .published::<Refund>("refunds.dlq")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_regional_copy_returns_to_its_own_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_processor(1, false)).await?;

        tb.broker::<KafkaBroker>()
            .message(&refund(4))
            .to("refunds.us")
            .publish()
            .await?;
        tb.advance(BACKOFF).await?;

        tb.broker::<KafkaBroker>()
            .published::<Refund>("refunds.us")
            .assert_called(2)
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.broker::<KafkaBroker>()
            .published::<Refund>("refunds.eu")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
