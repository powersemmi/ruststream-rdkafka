//! The ledger service of the Topics, consumer groups and ordering guide: two groups read the same
//! transfers, ledger entries keep the account as their record key, and cases are spread over the
//! partitions of their topic with one partition kept for urgent ones.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_groups_and_ordering -- run
//! ```

use ruststream_rdkafka::context::keys::Offset;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:post]
/// The account a record belongs to, carried as the Kafka record key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct AccountKey {
    #[serde(rename = "kafka-partition-key")]
    account: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Transfer {
    id: u64,
    amount_cents: i64,
}

/// One line of an account's ledger, keyed by the account like the transfer it came from.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "bank.entries", headers = AccountKey)]
struct Entry {
    transfer_id: u64,
    amount_cents: i64,
}

#[derive(OutSlot)]
#[publishes(Entry)]
struct Entries;

#[subscriber(
    KafkaTopic::new("transfers").lane_key(LaneKey::RecordKey),
    workers(8, by_key)
)]
async fn post(
    transfer: &Transfer,
    Headers(key): Headers<AccountKey>,
    Out(entries): Out<impl Publisher, Entries>,
) -> HandlerOutcome {
    let entry = Entry {
        transfer_id: transfer.id,
        amount_cents: transfer.amount_cents,
    };
    if entries
        .message(&entry)
        .with_headers(&key)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    tracing::info!(transfer.id, account = %key.account, "transfer posted");
    HandlerOutcome::ack()
}
// --8<-- [end:post]

// --8<-- [start:screen]
#[subscriber(
    KafkaTopic::new("transfers")
        .group("fraud-check")
        .start(StartOffset::Earliest)
        .assignment(Assignment::CooperativeSticky)
)]
async fn screen(transfer: &Transfer) -> HandlerOutcome {
    if transfer.amount_cents > 1_000_000 {
        tracing::warn!(transfer.id, "large transfer flagged");
    }
    HandlerOutcome::ack()
}
// --8<-- [end:screen]

// --8<-- [start:archive]
/// Where a record of one of the bank's topics sits.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "archive.index")]
struct Archived {
    topic: String,
    partition: i32,
    offset: i64,
}

#[subscriber(KafkaTopics::pattern("^bank\\..*").group("archive"), reply)]
async fn archive(
    _record: &serde_json::Value,
    Ctx(topic): Ctx<Topic>,
    Ctx(partition): Ctx<Partition>,
    Ctx(offset): Ctx<Offset>,
) -> Archived {
    Archived {
        topic: topic.to_string(),
        partition,
        offset,
    }
}
// --8<-- [end:archive]

// --8<-- [start:cases]
/// The partition of `bank.cases` kept for urgent cases.
const URGENT: i32 = 2;

#[derive(Debug, PartialEq, Deserialize)]
struct Dispute {
    transfer_id: u64,
    urgent: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "bank.cases")]
struct Case {
    transfer_id: u64,
}

#[derive(OutSlot)]
#[publishes(Case)]
struct Cases;

#[subscriber(KafkaTopic::new("card-disputes").group("case-intake"))]
async fn open_case(
    dispute: &Dispute,
    Out(cases): Out<impl Publisher<Options = KafkaOptions>, Cases>,
) -> HandlerOutcome {
    let case = Case {
        transfer_id: dispute.transfer_id,
    };
    let opened = if dispute.urgent {
        cases.message(&case).partition(URGENT).publish().await
    } else {
        cases.message(&case).publish().await
    };
    if opened.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:cases]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(KafkaBroker::new(["localhost:9092"]).default_group("ledger"))
}

fn service(broker: KafkaBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(broker, |b| {
        b.include(post).out(Entries, Publish::default()).build();
        b.include(screen);
        // A set of topics has no single topic to send a retry copy to, so the mount site names it.
        b.include(archive)
            .out_retry(Publish::default())
            .transform(ToSourceTopic);
        // Partitions 0 and 1 share the regular cases evenly; partition 2 stays for urgent ones.
        b.include(open_case)
            .out(Cases, Publish::default())
            .transform(RoundRobin::partitions(2))
            .build();
    })
}
// --8<-- [end:app]

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
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
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "transfers", headers = AccountKey)]
    struct Sent {
        id: u64,
        amount_cents: i64,
    }

    /// A dispute as the support desk publishes it.
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "card-disputes")]
    struct Raised {
        transfer_id: u64,
        urgent: bool,
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

        // `post` reads in the broker's default group, `screen` in `fraud-check`.
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
        let group = format!("groups-guide-{}", process::id());
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
