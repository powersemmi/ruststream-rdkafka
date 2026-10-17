//! The service of the Replay and seeking guide, written without the macros: each handler is a
//! type with an `impl Handle` that declares the Kafka context and the app's state, the start
//! positions are steps of the mount chain, and `main` is written by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_replay -- run
//! ```

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::context::{KafkaBatchContext, KafkaContext};
use ruststream_rdkafka::prelude::*;
use serde::Deserialize;

#[cfg(test)]
#[path = "../../../tests/live/mod.rs"]
mod live;

// --8<-- [start:projection]
/// One posting of the ledger. `revision` is the ledger's revision: a correction of past
/// postings is published as a new revision, and a read model meeting one rebuilds itself.
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Entry {
    id: u64,
    account: String,
    amount_cents: i64,
    revision: u32,
}

/// A read model kept in memory: the balance of every account.
#[derive(Debug, Default)]
struct Projection {
    revision: u32,
    balances: HashMap<String, i64>,
}

impl Projection {
    /// Applies `entry`, or clears the model and answers `false` when the entry belongs to a newer
    /// revision, which the model has to be rebuilt for from the start of the log.
    fn apply(&mut self, entry: &Entry) -> bool {
        if entry.revision > self.revision {
            self.revision = entry.revision;
            self.balances.clear();
            return false;
        }
        *self.balances.entry(entry.account.clone()).or_default() += entry.amount_cents;
        true
    }
}

/// The service's state: one read model per subscription that builds one.
#[derive(Clone, Default)]
struct Projections {
    balances: Arc<Mutex<Projection>>,
    spend: Arc<Mutex<Projection>>,
}
// --8<-- [end:projection]

// --8<-- [start:balance]
/// Declaring `KafkaContext` is what puts the seek handle in the context.
struct ProjectBalance;

impl Handle<Entry, (), (), KafkaContext, Projections> for ProjectBalance {
    async fn handle(
        &self,
        entry: &Entry,
        _outs: &(),
        ctx: &mut Context<'_, KafkaContext, Projections>,
    ) -> Result<(), HandlerOutcome> {
        let applied = ctx
            .state()
            .balances
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .apply(entry);
        if applied {
            tracing::info!(entry.id, "balance updated");
            return Ok(());
        }
        tracing::info!(
            entry.id,
            entry.revision,
            "new revision, rebuilding the balances"
        );
        if ctx
            .context(SeekHandle)
            .seek(KafkaPosition::earliest())
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:balance]

// --8<-- [start:batch]
/// A batch body declares `KafkaBatchContext`, which carries the same seek handle.
struct ProjectSpend;

impl Handle<[Entry], (), (), KafkaBatchContext, Projections> for ProjectSpend {
    async fn handle(
        &self,
        entries: &[Entry],
        _outs: &(),
        ctx: &mut Context<'_, KafkaBatchContext, Projections>,
    ) -> Result<(), Vec<HandlerOutcome>> {
        let applied = {
            let mut spend = ctx
                .state()
                .spend
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            entries.iter().all(|entry| spend.apply(entry))
        };
        if applied {
            tracing::info!(entries = entries.len(), "card spend updated");
            return Ok(());
        }
        tracing::info!("new revision, rebuilding the card spend");
        if ctx
            .context(SeekHandle)
            .seek(KafkaPosition::earliest())
            .await
            .is_err()
        {
            return Err(entries.iter().map(|_| HandlerOutcome::retry()).collect());
        }
        Ok(())
    }
}
// --8<-- [end:batch]

// --8<-- [start:payouts]
/// A payout as the payments service publishes it.
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payout {
    id: u64,
    amount_cents: i64,
}

/// The body reads no context, but runs in an app whose state is `Projections`.
struct ReviewPayout;

impl Handle<Payout, (), (), (), Projections> for ReviewPayout {
    fn handle(
        &self,
        payout: &Payout,
        _outs: &(),
        _ctx: &mut Context<'_, (), Projections>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(payout.id, payout.amount_cents, "payout reviewed");
        ready(Ok(()))
    }
}
// --8<-- [end:payouts]

// --8<-- [start:service]
/// Milliseconds since the epoch, the unit Kafka stamps records with.
fn epoch_millis(at: SystemTime) -> i64 {
    at.duration_since(UNIX_EPOCH).map_or(0, |since| {
        i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
    })
}

fn service(broker: KafkaBroker, review_from: SystemTime) -> impl App<State = Projections> {
    RustStream::new(AppInfo::new("ledger", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Projections::default()))
        .with_broker(broker, |b| {
            b.include(
                subscriber("ledger", ProjectBalance)
                    .map_source(|_| {
                        KafkaTopic::new("ledger")
                            .group("balances")
                            .commit(Commit::Tracked)
                    })
                    .start_at(KafkaPosition::earliest())
                    .build(),
            );
            b.include(
                subscriber("card-entries", ProjectSpend)
                    .map_source(|_| {
                        KafkaTopic::new("card-entries")
                            .group("card-spend")
                            .commit(Commit::Tracked)
                    })
                    .batch(nonzero!(500usize))
                    .build(),
            );
            b.include(
                subscriber("payouts", ReviewPayout)
                    .map_source(|_| {
                        KafkaTopic::new("payouts")
                            .group("payout-review")
                            .commit(Commit::Tracked)
                    })
                    .start_at(KafkaPosition::timestamp(epoch_millis(review_from)))
                    .build(),
            );
        })
}

/// The review reads the payouts of the last seven days on every start.
const REVIEW_WINDOW: Duration = Duration::from_hours(7 * 24);

fn app() -> impl App<State = Projections> {
    let review_from = SystemTime::now() - REVIEW_WINDOW;
    service(KafkaBroker::new(["localhost:9092"]), review_from)
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::{Duration, Instant, SystemTime};

    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::error::RDKafkaErrorCode;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Projections, REVIEW_WINDOW, app, live, service};

    /// A posting as the ledger publishes it; the call names the topic.
    #[derive(Serialize)]
    struct Posted {
        id: u64,
        account: &'static str,
        amount_cents: i64,
        revision: u32,
    }

    impl OutgoingDestination for Posted {
        type Form = CallerName;
    }

    impl MessageHeaders for Posted {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Posted {
        const NAME: &'static str = "Posted";
        const DESCRIPTION: Option<&'static str> = None;
    }

    fn posted(id: u64, revision: u32) -> Posted {
        Posted {
            id,
            account: "acc-1",
            amount_cents: 100,
            revision,
        }
    }

    /// A payout as the payments service publishes it.
    #[derive(Serialize)]
    struct PaidOut {
        id: u64,
        amount_cents: i64,
    }

    impl OutgoingDestination for PaidOut {
        type Form = FixedName;

        const DESTINATION: &'static str = "payouts";
    }

    impl MessageHeaders for PaidOut {
        type Contract = NoHeaders;
    }

    impl MessageInfo for PaidOut {
        const NAME: &'static str = "PaidOut";
        const DESCRIPTION: Option<&'static str> = None;
    }

    /// The ids `topic`'s handler received, in order.
    fn ids(tb: &TestApp<Projections>, topic: &str) -> Vec<u64> {
        #[derive(Deserialize)]
        struct Id {
            id: u64,
        }
        tb.broker::<KafkaBroker>()
            .subscriber(topic)
            .received::<Id>()
            .into_iter()
            .map(|entry| entry.id)
            .collect()
    }

    #[tokio::test]
    async fn a_new_revision_rebuilds_the_balances_from_the_start() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (id, revision) in [(1, 0), (2, 0), (3, 1)] {
            tb.broker::<KafkaBroker>()
                .message(&posted(id, revision))
                .to("ledger")
                .publish()
                .await?;
        }

        assert_eq!(ids(&tb, "ledger"), [1, 2, 3, 1, 2, 3]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_batch_with_a_new_revision_rebuilds_the_spend() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (id, revision) in [(1, 0), (2, 1)] {
            tb.broker::<KafkaBroker>()
                .message(&posted(id, revision))
                .to("card-entries")
                .publish()
                .await?;
        }

        assert_eq!(ids(&tb, "card-entries"), [1, 2, 1, 2]);
        tb.broker::<KafkaBroker>()
            .subscriber("card-entries")
            .assert_batch_sizes(&[1, 1, 2]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_review_reads_the_payouts_published_after_its_moment() -> Result<(), Box<dyn Error>>
    {
        let review_from = SystemTime::now();
        let tb = TestApp::start(service(KafkaBroker::new(["kafka:9092"]), review_from)).await?;

        tb.broker::<KafkaBroker>()
            .message(&PaidOut {
                id: 1,
                amount_cents: 500,
            })
            .publish()
            .await?;

        assert_eq!(ids(&tb, "payouts"), [1]);
        tb.shutdown().await?;
        Ok(())
    }

    /// Starts the service on the stand.
    async fn start_live(url: &str, review_from: SystemTime) -> TestApp<Projections> {
        let broker = KafkaBroker::new([url]);
        TestApp::start_live_within(service(broker, review_from), Duration::from_secs(30))
            .await
            .expect("start live")
    }

    /// Two starts of the service on the stand, in the groups the descriptors name. The topic is
    /// recreated first, so no earlier run's commit applies; the second start replays what the
    /// first one handled and committed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_the_balances_replay_the_whole_log_on_every_start() {
        let Some(url) = live::url("KAFKA_TEST_URL") else {
            return;
        };
        recreate_topic(&url, "ledger").await;
        let window = SystemTime::now() - REVIEW_WINDOW;

        let tb = start_live(&url, window).await;
        post(&tb, 1).await;
        tb.shutdown().await.expect("shutdown");

        let tb = start_live(&url, window).await;
        post(&tb, 2).await;
        // The harness waits for one delivery per publish; on one partition, what the start
        // position replays arrives ahead of the record published now.
        assert_eq!(ids(&tb, "ledger")[0], 1);
        tb.shutdown().await.expect("shutdown");
    }

    /// Three starts of the service on the stand. The third starts the review between payout 1
    /// and payout 2, so it replays payout 2, which the group has committed, and skips payout 1.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "the first start loses the payout published after an empty partition is \
                assigned: a timestamp no record matches resolves to the logical end of the log"]
    async fn live_the_review_starts_at_its_moment() {
        let Some(url) = live::url("KAFKA_TEST_URL") else {
            return;
        };
        recreate_topic(&url, "payouts").await;
        let window = SystemTime::now() - REVIEW_WINDOW;

        let tb = start_live(&url, window).await;
        pay_out(&tb, 1).await;
        tb.shutdown().await.expect("shutdown");

        // Payout 1 is stamped before this moment, payout 2 after it.
        let review_from = SystemTime::now();
        let tb = start_live(&url, window).await;
        pay_out(&tb, 2).await;
        tb.shutdown().await.expect("shutdown");

        let tb = start_live(&url, review_from).await;
        pay_out(&tb, 3).await;
        assert_eq!(ids(&tb, "payouts")[0], 2);
        tb.shutdown().await.expect("shutdown");
    }

    async fn post(tb: &TestApp<Projections>, id: u64) {
        tb.broker::<KafkaBroker>()
            .message(&posted(id, 0))
            .to("ledger")
            .publish()
            .await
            .expect("publish the posting");
    }

    async fn pay_out(tb: &TestApp<Projections>, id: u64) {
        tb.broker::<KafkaBroker>()
            .message(&PaidOut {
                id,
                amount_cents: 500,
            })
            .publish()
            .await
            .expect("publish the payout");
    }

    /// Deletes and recreates `topic` with one partition, so the run reads only what it
    /// publishes.
    async fn recreate_topic(url: &str, topic: &str) {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", url)
            .create()
            .expect("admin client");
        let _ = admin
            .delete_topics(&[topic], &AdminOptions::new())
            .await
            .expect("delete_topics call");
        // Deletion completes asynchronously; each metadata fetch waits for the cluster's answer,
        // so the loop is paced by the condition it waits for.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let metadata = admin
                .inner()
                .fetch_metadata(None, Duration::from_millis(500))
                .expect("fetch_metadata call");
            let present = metadata
                .topics()
                .iter()
                .any(|known| known.name() == topic && known.error().is_none());
            if !present {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "topic {topic} outlived its delete"
            );
        }
        let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
        let results = admin
            .create_topics([&new_topic], &AdminOptions::new())
            .await
            .expect("create_topics call");
        match &results[0] {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => panic!("recreating {name} failed: {code}"),
        }
    }
}
