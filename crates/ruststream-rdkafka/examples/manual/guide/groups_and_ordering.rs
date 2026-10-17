//! The ledger service of the Topics, consumer groups and ordering guide, written without the
//! macros: each handler is a type with an `impl Handle`, mounted with the `subscriber`
//! constructor, and `main` is written by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_groups_and_ordering -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rdkafka::context::KafkaContext;
use ruststream_rdkafka::context::keys::Offset;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:post]
/// The account a record belongs to, carried as the Kafka record key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct AccountKey {
    #[serde(rename = "kafka-partition-key")]
    account: String,
}

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Transfer {
    id: u64,
    amount_cents: i64,
}

/// One line of an account's ledger, keyed by the account like the transfer it came from.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Entry {
    transfer_id: u64,
    amount_cents: i64,
}

impl OutgoingDestination for Entry {
    type Form = FixedName;

    const DESTINATION: &'static str = "bank.entries";
}

impl MessageHeaders for Entry {
    type Contract = WithHeaders<AccountKey>;
}

impl MessageInfo for Entry {
    const NAME: &'static str = "Entry";
}

struct Entries;

impl OutSlot for Entries {
    const NAME: &'static str = "Entries";
    type Destination = Reads;
}

impl PublishedThrough<Entries> for Entry {}

struct Post;

impl<Ledger> Handle<Message<AccountKey, Transfer>, (), Outs<(Ledger,)>> for Post
where
    Ledger: OutEntry<Entries, Wire: Publisher>,
{
    async fn handle(
        &self,
        transfer: &Message<AccountKey, Transfer>,
        outs: &Outs<(Ledger,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let key = &transfer.headers;
        let entry = Entry {
            transfer_id: transfer.body.id,
            amount_cents: transfer.body.amount_cents,
        };
        if outs
            .get(Entries)
            .message(&entry)
            .with_headers(key)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        tracing::info!(transfer.body.id, account = %key.account, "transfer posted");
        Ok(())
    }
}
// --8<-- [end:post]

// --8<-- [start:screen]
struct Screen;

impl Handle<Transfer> for Screen {
    fn handle(
        &self,
        transfer: &Transfer,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if transfer.amount_cents > 1_000_000 {
            tracing::warn!(transfer.id, "large transfer flagged");
        }
        ready(Ok(()))
    }
}
// --8<-- [end:screen]

// --8<-- [start:archive]
/// Where a record of one of the bank's topics sits.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Archived {
    topic: String,
    partition: i32,
    offset: i64,
}

impl OutgoingDestination for Archived {
    type Form = FixedName;

    const DESTINATION: &'static str = "archive.index";
}

impl MessageHeaders for Archived {
    type Contract = NoHeaders;
}

impl MessageInfo for Archived {
    const NAME: &'static str = "Archived";
}

struct Archive;

impl Handle<serde_json::Value, Archived, (), KafkaContext> for Archive {
    fn handle(
        &self,
        _record: &serde_json::Value,
        _outs: &(),
        ctx: &mut Context<'_, KafkaContext>,
    ) -> impl Future<Output = Result<Archived, HandlerOutcome>> {
        ready(Ok(Archived {
            topic: ctx.context(Topic).to_string(),
            partition: ctx.context(Partition),
            offset: ctx.context(Offset),
        }))
    }
}
// --8<-- [end:archive]

// --8<-- [start:cases]
/// The partition of `bank.cases` kept for urgent cases.
const URGENT: i32 = 2;

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Dispute {
    transfer_id: u64,
    urgent: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Case {
    transfer_id: u64,
}

impl OutgoingDestination for Case {
    type Form = FixedName;

    const DESTINATION: &'static str = "bank.cases";
}

impl MessageHeaders for Case {
    type Contract = NoHeaders;
}

impl MessageInfo for Case {
    const NAME: &'static str = "Case";
}

struct Cases;

impl OutSlot for Cases {
    const NAME: &'static str = "Cases";
    type Destination = Reads;
}

impl PublishedThrough<Cases> for Case {}

struct OpenCase;

impl<Opened> Handle<Dispute, (), Outs<(Opened,)>> for OpenCase
where
    Opened: OutEntry<Cases, Wire: Publisher<Options = KafkaOptions>>,
{
    async fn handle(
        &self,
        dispute: &Dispute,
        outs: &Outs<(Opened,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let cases = outs.get(Cases);
        let case = Case {
            transfer_id: dispute.transfer_id,
        };
        let opened = if dispute.urgent {
            cases.message(&case).partition(URGENT).publish().await
        } else {
            cases.message(&case).publish().await
        };
        opened.map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:cases]

// --8<-- [start:app]
fn app() -> impl App<State = ()> {
    service(KafkaBroker::new(["localhost:9092"]).default_group("ledger"))
}

fn service(broker: KafkaBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber("transfers", Post)
                .workers_by_key(nonzero!(8usize))
                .map_source(|_| KafkaTopic::new("transfers").lane_key(LaneKey::RecordKey))
                .build(),
        )
        .out(Entries, Publish::default())
        .build();
        b.include(
            subscriber("transfers", Screen)
                .map_source(|_| {
                    KafkaTopic::new("transfers")
                        .group("fraud-check")
                        .start(StartOffset::Earliest)
                        .assignment(Assignment::CooperativeSticky)
                })
                .build(),
        );
        // A set of topics has no single topic to send a retry copy to, so the mount site names it.
        b.include(
            subscriber("^bank\\..*", Archive)
                .map_source(|_| KafkaTopics::pattern("^bank\\..*").group("archive"))
                .reply()
                .build(),
        )
        .out_retry(Publish::default())
        .transform(ToSourceTopic);
        // Partitions 0 and 1 share the regular cases evenly; partition 2 stays for urgent ones.
        b.include(
            subscriber("card-disputes", OpenCase)
                .map_source(|_| KafkaTopic::new("card-disputes").group("case-intake"))
                .build(),
        )
        .out(Cases, Publish::default())
        .transform(RoundRobin::partitions(2))
        .build();
    })
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
#[path = "../../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::error::Error;
    use std::process;
    use std::time::{Duration, Instant};

    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::error::RDKafkaErrorCode;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;
    use serde::Serialize;

    use super::{AccountKey, Archived, Case, Cases, Entry, Transfer, app, live, service};

    /// A transfer as the payments service publishes it, keyed by its account.
    #[derive(Serialize)]
    struct Sent {
        id: u64,
        amount_cents: i64,
    }

    impl OutgoingDestination for Sent {
        type Form = FixedName;

        const DESTINATION: &'static str = "transfers";
    }

    impl MessageHeaders for Sent {
        type Contract = WithHeaders<AccountKey>;
    }

    impl MessageInfo for Sent {
        const NAME: &'static str = "Sent";
    }

    /// A dispute as the support desk publishes it.
    #[derive(Serialize)]
    struct Raised {
        transfer_id: u64,
        urgent: bool,
    }

    impl OutgoingDestination for Raised {
        type Form = FixedName;

        const DESTINATION: &'static str = "card-disputes";
    }

    impl MessageHeaders for Raised {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Raised {
        const NAME: &'static str = "Raised";
    }

    fn key(account: &str) -> AccountKey {
        AccountKey {
            account: account.to_owned(),
        }
    }

    async fn send(tb: &TestApp<()>, id: u64, account: &str) -> Result<(), Box<dyn Error>> {
        tb.broker::<KafkaBroker>()
            .message(&Sent {
                id,
                amount_cents: 100,
            })
            .with_headers(&key(account))
            .publish()
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn each_group_reads_its_own_copy() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        send(&tb, 1, "acc-1").await?;

        // `Post` reads in the broker's default group, `Screen` in `fraud-check`.
        tb.broker::<KafkaBroker>()
            .subscriber("transfers")
            .assert_called(2)
            .with(&Transfer {
                id: 1,
                amount_cents: 100,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_entry_keeps_the_account_as_its_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        send(&tb, 2, "acc-7").await?;

        tb.broker::<KafkaBroker>()
            .published::<Entry>("bank.entries")
            .assert_called_once()
            .with(&Entry {
                transfer_id: 2,
                amount_cents: 100,
            })
            .with_header("kafka-partition-key", "acc-7");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn entries_of_one_account_keep_their_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (id, account) in [(1, "acc-1"), (2, "acc-2"), (3, "acc-1"), (4, "acc-2")] {
            send(&tb, id, account).await?;
        }

        let entries = tb
            .broker::<KafkaBroker>()
            .published::<Entry>("bank.entries");
        let mut by_account: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for (message, entry) in entries.messages().iter().zip(entries.decoded()) {
            let account = message
                .headers()
                .get_str("kafka-partition-key")
                .unwrap_or("");
            by_account
                .entry(account.to_owned())
                .or_default()
                .push(entry.transfer_id);
        }
        assert_eq!(by_account["acc-1"], [1, 3]);
        assert_eq!(by_account["acc-2"], [2, 4]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_archive_indexes_every_bank_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        send(&tb, 5, "acc-1").await?;
        tb.broker::<KafkaBroker>()
            .message(&Raised {
                transfer_id: 5,
                urgent: false,
            })
            .publish()
            .await?;

        let topics: BTreeSet<String> = tb
            .broker::<KafkaBroker>()
            .published::<Archived>("archive.index")
            .assert_called(2)
            .decoded()
            .into_iter()
            .map(|archived| archived.topic)
            .collect();
        assert_eq!(
            topics,
            BTreeSet::from(["bank.cases".into(), "bank.entries".into()])
        );
        tb.out::<Cases>()
            .assert_called_once()
            .decoded_as::<Case>()
            .with(&Case { transfer_id: 5 })
            .with_options(&KafkaOptions::default().partition(0));
        tb.shutdown().await?;
        Ok(())
    }

    /// The cluster places records on several partitions, which the in-process Kafka with its
    /// one partition per topic cannot show.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn records_land_on_the_partitions_the_service_chose() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("KAFKA_TEST_URL") else {
            return Ok(());
        };
        for (topic, partitions) in [
            ("transfers", 1),
            ("card-disputes", 1),
            ("bank.entries", 3),
            ("bank.cases", 3),
            ("archive.index", 1),
        ] {
            recreate_topic(&url, topic, partitions).await;
        }
        let group = format!("manual-groups-guide-{}", process::id());
        let tb = TestApp::start_live_within(
            service(KafkaBroker::new([url.as_str()]).default_group(group)),
            Duration::from_secs(30),
        )
        .await?;

        let accounts = ["acc-1", "acc-2", "acc-3", "acc-4", "acc-5", "acc-6"];
        for round in 0..2 {
            for (index, account) in (0u64..).zip(accounts) {
                send(&tb, round * 10 + index, account).await?;
            }
        }
        // Eight regular cases and one urgent one: a random spread matches the cycle by chance
        // well under one run in a hundred.
        for transfer_id in 1..=9 {
            let urgent = transfer_id == 5;
            tb.broker::<KafkaBroker>()
                .message(&Raised {
                    transfer_id,
                    urgent,
                })
                .publish()
                .await?;
        }
        tb.settle().await?;

        let index = tb
            .broker::<KafkaBroker>()
            .published::<Archived>("archive.index")
            .decoded();
        let mut cases_per_partition = BTreeMap::new();
        let mut entry_partitions = BTreeSet::new();
        for archived in &index {
            if archived.topic == "bank.cases" {
                *cases_per_partition.entry(archived.partition).or_insert(0) += 1;
            } else {
                entry_partitions.insert(archived.partition);
            }
        }
        assert_eq!(
            cases_per_partition,
            BTreeMap::from([(0, 4), (1, 4), (2, 1)])
        );
        assert!(
            entry_partitions.len() > 1,
            "keys spread over {entry_partitions:?}"
        );
        tb.shutdown().await?;
        Ok(())
    }

    /// Deletes `topic` and creates it again with `partitions`, so the run reads only what it
    /// publishes.
    async fn recreate_topic(url: &str, topic: &str, partitions: i32) {
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
                "topic {topic} outlived its delete request"
            );
        }
        let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
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
