//! The cluster's own rules, on topics with more partitions than the one a topic comes into being
//! with.

use std::sync::Arc;
use std::time::Duration;

use rdkafka::ClientConfig;
use ruststream::HeaderMap;
use tokio::runtime::Handle;

use super::{Cluster, Member, MemberSpec, ProducerSettings};
use crate::message::PARTITION_KEY_HEADER;
use crate::subscription::StartOffset;
use crate::tracker::CommitTracker;

fn cluster() -> Arc<Cluster> {
    let settings = ProducerSettings::read(&ClientConfig::new()).expect("librdkafka's defaults");
    Cluster::new(settings)
}

impl Cluster {
    /// Creates `topic` with `partitions` partitions, as an administrator would before the
    /// service starts.
    fn create(&self, topic: &str, partitions: usize) {
        self.lock()
            .topics
            .insert(topic.to_owned(), super::Topic::new(partitions));
    }
}

fn member(
    cluster: &Arc<Cluster>,
    group: &str,
    topics: &[&str],
    extra: &[(&str, &str)],
) -> Arc<Member> {
    let mut config = ClientConfig::new();
    config.set("group.id", group);
    config.set("auto.offset.reset", "earliest");
    for (key, value) in extra {
        config.set(*key, *value);
    }
    let names: Vec<String> = topics.iter().map(|topic| (*topic).to_owned()).collect();
    let spec = MemberSpec::read(
        topics.join(","),
        Some(group.to_owned()),
        Some(&names),
        None,
        StartOffset::Committed,
        &config,
        Arc::new(CommitTracker::default()),
    )
    .expect("a valid consumer configuration");
    cluster.join(spec).expect("join")
}

fn produce(cluster: &Cluster, topic: &str, payload: &str, key: Option<&str>) {
    let mut headers = HeaderMap::new();
    if let Some(key) = key {
        headers.insert(PARTITION_KEY_HEADER, key.to_owned());
    }
    cluster
        .produce(topic, None, payload.as_bytes(), &headers, None)
        .expect("produce");
}

/// Everything the member has ready, as `(partition, payload)`.
fn drain(member: &Arc<Member>) -> Vec<(i32, String)> {
    let mut seen = Vec::new();
    while let Some(fetched) = member.fetch() {
        let record = fetched.expect("a record");
        seen.push((
            record.partition(),
            String::from_utf8(record.payload().to_vec()).expect("utf-8"),
        ));
    }
    seen
}

#[test]
fn a_group_splits_the_partitions_and_every_group_reads_every_record() {
    let cluster = cluster();
    cluster.create("orders", 4);
    let first = member(&cluster, "billing", &["orders"], &[]);
    let second = member(&cluster, "billing", &["orders"], &[]);
    let audit = member(&cluster, "audit", &["orders"], &[]);
    for index in 0..8 {
        produce(&cluster, "orders", &index.to_string(), None);
    }

    let one = drain(&first);
    let two = drain(&second);
    assert_eq!(
        one.len(),
        4,
        "range hands each member two of four partitions: {one:?}"
    );
    assert_eq!(two.len(), 4, "{two:?}");
    assert!(one.iter().all(|(partition, _)| *partition < 2), "{one:?}");
    assert!(two.iter().all(|(partition, _)| *partition >= 2), "{two:?}");
    assert_eq!(
        drain(&audit).len(),
        8,
        "another group reads its own copy of everything"
    );
}

#[test]
fn a_key_keeps_its_records_on_one_partition() {
    let cluster = cluster();
    cluster.create("orders", 4);
    let reader = member(&cluster, "billing", &["orders"], &[]);
    for index in 0..6 {
        produce(&cluster, "orders", &index.to_string(), Some("tenant-1"));
    }
    let seen = drain(&reader);
    assert_eq!(seen.len(), 6);
    assert!(
        seen.windows(2).all(|pair| pair[0].0 == pair[1].0),
        "{seen:?}"
    );
    let order: Vec<&str> = seen.iter().map(|(_, payload)| payload.as_str()).collect();
    assert_eq!(
        order,
        ["0", "1", "2", "3", "4", "5"],
        "one partition keeps the key's order"
    );
}

#[test]
fn a_departing_member_hands_its_partitions_over_at_the_committed_offset() {
    let cluster = cluster();
    cluster.create("orders", 2);
    let first = member(&cluster, "billing", &["orders"], &[]);
    let second = member(&cluster, "billing", &["orders"], &[]);
    for index in 0..4 {
        produce(&cluster, "orders", &index.to_string(), None);
    }
    // Auto-commit stores and commits what was handed over.
    let taken = drain(&first);
    assert_eq!(taken.len(), 2);
    drop(first);

    // The survivor now owns both partitions; the departed member's is resumed where its group
    // committed, so nothing it read comes back and nothing is skipped.
    let rest = drain(&second);
    assert_eq!(rest.len(), 2, "{rest:?}");
    produce(&cluster, "orders", "4", None);
    produce(&cluster, "orders", "5", None);
    assert_eq!(drain(&second).len(), 2);
}

#[test]
fn round_robin_deals_every_partition_in_turn() {
    let cluster = cluster();
    cluster.create("a", 1);
    cluster.create("b", 1);
    let strategy = [("partition.assignment.strategy", "roundrobin")];
    let first = member(&cluster, "g", &["a", "b"], &strategy);
    let second = member(&cluster, "g", &["a", "b"], &strategy);
    produce(&cluster, "a", "a0", None);
    produce(&cluster, "b", "b0", None);
    assert_eq!(drain(&first), [(0, "a0".to_owned())]);
    assert_eq!(drain(&second), [(0, "b0".to_owned())]);
}

#[test]
fn an_eager_and_a_cooperative_member_cannot_share_a_group() {
    let cluster = cluster();
    let _eager = member(&cluster, "g", &["orders"], &[]);
    let cooperative = member(
        &cluster,
        "g",
        &["orders"],
        &[("partition.assignment.strategy", "cooperative-sticky")],
    );
    let failed = cooperative
        .fetch()
        .expect("the stream reports the refused join")
        .expect_err("the join is refused");
    assert!(failed.to_string().contains("Inconsistent"), "{failed}");
}

#[tokio::test]
async fn a_committed_reader_waits_for_a_transaction_and_never_sees_an_aborted_one() {
    let cluster = cluster();
    let reader = member(&cluster, "g", &["out"], &[]);
    let producer = cluster.init_transactions("tx-1");

    cluster.begin(&producer, &Handle::current()).expect("begin");
    cluster
        .produce("out", None, b"aborted", &HeaderMap::new(), Some(&producer))
        .expect("produce");
    assert!(
        drain(&reader).is_empty(),
        "an open transaction is invisible"
    );
    cluster.abort(&producer).expect("abort");

    cluster.begin(&producer, &Handle::current()).expect("begin");
    cluster
        .produce(
            "out",
            None,
            b"committed",
            &HeaderMap::new(),
            Some(&producer),
        )
        .expect("produce");
    produce(&cluster, "out", "plain", None);
    assert!(
        drain(&reader).is_empty(),
        "a plain record behind an open transaction waits for it",
    );
    cluster.commit(&producer).expect("commit");

    let fetched = reader.fetch().expect("a record").expect("ok");
    // Offset 0 was aborted and 1 is its marker: the committed record sits at 2.
    assert_eq!(
        (fetched.offset(), fetched.payload()),
        (2, b"committed".as_slice())
    );
    drop(fetched);
    assert_eq!(drain(&reader), [(0, "plain".to_owned())]);
}

#[tokio::test]
async fn a_second_pairing_fences_the_first() {
    let cluster = cluster();
    let older = cluster.init_transactions("tx-1");
    cluster.begin(&older, &Handle::current()).expect("begin");
    let newer = cluster.init_transactions("tx-1");
    let fenced = cluster
        .commit(&older)
        .expect_err("the older producer is fenced");
    assert!(fenced.to_string().contains("fenced"), "{fenced}");
    cluster
        .begin(&newer, &Handle::current())
        .expect("the newer one works");
}

#[tokio::test(start_paused = true)]
async fn a_transaction_open_past_its_timeout_is_aborted() {
    let cluster = cluster();
    let reader = member(&cluster, "g", &["out"], &[]);
    let producer = cluster.init_transactions("tx-1");
    cluster.begin(&producer, &Handle::current()).expect("begin");
    cluster
        .produce("out", None, b"late", &HeaderMap::new(), Some(&producer))
        .expect("produce");
    produce(&cluster, "out", "behind", None);

    // librdkafka's default `transaction.timeout.ms` is a minute.
    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(
        drain(&reader),
        [(0, "behind".to_owned())],
        "the aborted record is skipped and the one behind it is released",
    );
    cluster
        .commit(&producer)
        .expect_err("the transaction the cluster aborted cannot commit");
}

#[test]
fn a_record_over_the_size_limit_and_a_missing_partition_are_refused() {
    let cluster = cluster();
    let big = vec![0u8; 1_000_001];
    cluster
        .produce("orders", None, &big, &HeaderMap::new(), None)
        .expect_err("over message.max.bytes");
    cluster
        .produce("orders", Some(1), b"x", &HeaderMap::new(), None)
        .expect_err("the topic has one partition");
    cluster
        .produce("orders eu", None, b"x", &HeaderMap::new(), None)
        .expect_err("an illegal topic name");
}
