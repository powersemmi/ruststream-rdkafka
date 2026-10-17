//! Transactions under a worker pool: each source partition gets a transactional publisher of its
//! own, so the lanes of `workers(4, by_key)` run independent transactions. The scenarios are
//! tests; the live one reads a topic with two partitions on the cluster at `KAFKA_TEST_URL`.
//!
//! ```text
//! cargo test --example kafka_transactions_partitions --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "invoice-batches")]
struct Invoice {
    id: u64,
    amounts: Vec<u64>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "invoice-lines")]
struct InvoiceLine {
    invoice_id: u64,
    amount: u64,
}

// --8<-- [start:handler]
/// Bills every line of an invoice in the transaction of the invoice's source partition.
#[subscriber(
    KafkaTopic::new("invoice-batches").commit(Commit::Tracked),
    workers(4, by_key)
)]
async fn bill(
    invoice: &Invoice,
    Ctx(partition): Ctx<Partition>,
    Out(lanes): Out<impl PartitionLanes>,
) -> HandlerOutcome {
    let Ok(lane) = lanes.for_partition(partition).await else {
        return HandlerOutcome::retry();
    };
    if lane.begin_transaction().await.is_err() {
        return HandlerOutcome::retry();
    }
    for &amount in &invoice.amounts {
        let entry = InvoiceLine {
            invoice_id: invoice.id,
            amount,
        };
        if lane.message(&entry).publish().await.is_err() {
            lane.abort().await.ok();
            return HandlerOutcome::retry();
        }
    }
    if lane.commit().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app(servers: &str) -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        KafkaBroker::new([servers]).default_group("billing-svc"),
        |b| {
            // One transactional id per source partition: "billing-svc-1-p0", "billing-svc-1-p1".
            let lanes = Publish::default()
                .transactional_id("billing-svc-1")
                .per_partition();
            b.include(bill).out(DefaultSlot, lanes).build();
        },
    )
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;
    use std::time::{Duration, Instant};

    use rdkafka::ClientConfig;
    use rdkafka::Message as _;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::consumer::{BaseConsumer, Consumer as _};
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rdkafka::PARTITION_KEY_HEADER;
    use ruststream_rdkafka::prelude::*;

    use super::{Invoice, InvoiceLine, app};

    #[tokio::test]
    async fn a_lane_bills_an_invoice_in_its_partitions_transaction() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app("localhost:9092")).await?;
        let broker = tb.broker::<KafkaBroker>();
        broker
            .message(&Invoice {
                id: 7,
                amounts: vec![120, 80],
            })
            .publish()
            .await?;

        broker
            .published::<InvoiceLine>("invoice-lines")
            .assert_called(2)
            .with(&InvoiceLine {
                invoice_id: 7,
                amount: 80,
            });
        broker
            .subscriber("invoice-batches")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    /// Two partitions, two lanes, two transactional ids: both invoices commit on the first try,
    /// so neither lane found the other's transaction open on its producer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_two_partitions_commit_their_own_transactions() -> Result<(), Box<dyn Error>> {
        let Some(servers) = live_url() else {
            return Ok(());
        };
        recreate_topic(&servers, "invoice-batches", 2).await;
        recreate_topic(&servers, "invoice-lines", 1).await;
        let tb = TestApp::start_live_within(app(&servers), Duration::from_secs(30)).await?;
        let broker = tb.broker::<KafkaBroker>();
        // librdkafka's partitioner puts these record keys on partitions 0 and 1 of two.
        for (id, key) in [(1, "invoice-4"), (2, "invoice-1")] {
            let mut headers = HeaderMap::new();
            headers.insert(PARTITION_KEY_HEADER, key);
            broker
                .message(&Invoice {
                    id,
                    amounts: vec![10, 20],
                })
                .with_headers(headers)
                .publish()
                .await?;
        }

        assert_eq!(
            broker.subscriber("invoice-batches").outcomes(),
            [Outcome::Ack, Outcome::Ack],
        );
        tb.shutdown().await?;

        // A lane's publisher is the crate's own, outside the harness's record, so the committed
        // lines are read off the topic the way a `read_committed` consumer reads them.
        let mut lines = committed_lines(&servers, "invoice-lines", 4);
        lines.sort_by_key(|line| (line.invoice_id, line.amount));
        assert_eq!(
            lines,
            [(1, 10), (1, 20), (2, 10), (2, 20)]
                .map(|(invoice_id, amount)| InvoiceLine { invoice_id, amount }),
        );
        Ok(())
    }

    /// The first `count` records a `read_committed` consumer reads from `topic`, decoded.
    fn committed_lines(servers: &str, topic: &str, count: usize) -> Vec<InvoiceLine> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", servers)
            .set(
                "group.id",
                format!("invoice-lines-reader-{}", process::id()),
            )
            .set("auto.offset.reset", "earliest")
            .set("isolation.level", "read_committed")
            .create()
            .expect("reader");
        consumer.subscribe(&[topic]).expect("subscribe the reader");
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut lines = Vec::new();
        while lines.len() < count {
            assert!(Instant::now() < deadline, "only {lines:?} were committed");
            // Each poll returns as soon as a record arrives, so the loop is paced by the topic.
            if let Some(record) = consumer.poll(Duration::from_millis(500)) {
                let record = record.expect("a record");
                let payload = record.payload().expect("a payload");
                lines.push(serde_json::from_slice(payload).expect("an invoice line"));
            }
        }
        lines
    }

    /// The cluster at `KAFKA_TEST_URL`, or `None` to skip; under `RUSTSTREAM_REQUIRE_LIVE` a
    /// missing address fails the test instead.
    fn live_url() -> Option<String> {
        match std::env::var("KAFKA_TEST_URL") {
            Ok(url) if !url.is_empty() => Some(url),
            _ => {
                assert!(
                    std::env::var("RUSTSTREAM_REQUIRE_LIVE").is_err(),
                    "RUSTSTREAM_REQUIRE_LIVE is set, but KAFKA_TEST_URL is not"
                );
                None
            }
        }
    }

    /// Deletes `topic` and creates it again with `partitions` partitions, so the run reads only
    /// what it publishes.
    async fn recreate_topic(servers: &str, topic: &str, partitions: i32) {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", servers)
            .create()
            .expect("admin client");
        let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
        let _ = admin.delete_topics(&[topic], &options).await;
        // Deletion completes asynchronously, and a create racing it fails with "already
        // exists"; each attempt waits for the cluster's answer, so the loop is paced by it.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let created = admin
                .create_topics(
                    &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
                    &options,
                )
                .await
                .expect("create_topics call");
            if created.iter().all(Result::is_ok) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{topic} was not recreated: {created:?}"
            );
        }
    }
}
