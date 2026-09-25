//! The in-process transport itself: the cluster `TestApp::start` connects the production broker
//! to, driven here through the broker's own connected form.
//!
//! These cases are the transport's contract - what it delivers, to whom, from which offset, and
//! what it refuses - so they talk to the connected broker directly. Anything whose subject is a
//! service runs on `TestApp` in `tests/harness.rs`. The same rules hold against a cluster, and
//! `tests/integration_rdkafka.rs` checks them there.

#![cfg(feature = "testing")]

use std::thread;
use std::time::Duration;

use futures::{FutureExt as _, Stream, StreamExt};
use ruststream::testing::{InProcess as _, TestableBroker as _};
use ruststream::{
    AckError, ConnectedBroker as _, HeaderMap, IncomingMessage, OutgoingMessage, Partitioned,
    PublishPolicy as _, Publisher, RawMessage, Seekable as _, Seeker as _, Subscribe as _,
    Subscriber, SubscriptionSource as _, TransactionalPublisher,
};
use ruststream_rdkafka::{
    Commit, ConnectedKafkaBroker, KafkaBroker, KafkaEosPublish, KafkaError, KafkaMessage,
    KafkaOptions, KafkaPartitions, KafkaPosition, KafkaPublish, KafkaTopic, KafkaTopics, LaneKey,
    PARTITION_KEY_HEADER, SourceOffset, StartOffset,
};
use tokio::runtime;
use tokio::sync::oneshot;

/// The production broker as a service configures it, connected in process: its address is never
/// dialled.
async fn connected() -> ConnectedKafkaBroker {
    KafkaBroker::new(["kafka:9092"])
        .default_group("tests")
        .connect_in_process()
        .await
        .expect("connect in process")
}

async fn publish(broker: &ConnectedKafkaBroker, topic: &str, payload: &[u8]) {
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(topic, payload), None)
        .await
        .expect("publish");
}

/// The next delivery the transport has ready. The transport hands a record over synchronously,
/// so a delivery that is not ready now is not coming.
fn ready<S>(stream: &mut S) -> Option<KafkaMessage>
where
    S: Stream<Item = Result<KafkaMessage, KafkaError>> + Unpin,
{
    stream
        .next()
        .now_or_never()
        .flatten()
        .map(|delivery| delivery.expect("a delivery, not an error"))
}

/// Every delivery ready now, acked, as payloads.
async fn drain<S>(stream: &mut S) -> Vec<Vec<u8>>
where
    S: Stream<Item = Result<KafkaMessage, KafkaError>> + Unpin,
{
    let mut seen = Vec::new();
    while let Some(msg) = ready(stream) {
        seen.push(msg.payload().to_vec());
        msg.ack().await.expect("ack");
    }
    seen
}

fn payloads(items: &[&str]) -> Vec<Vec<u8>> {
    items.iter().map(|item| item.as_bytes().to_vec()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_published_record_reaches_the_subscription_of_its_topic_only() {
    let broker = connected().await;
    let mut orders = broker.subscribe("orders").await.expect("subscribe");
    let mut payments = broker.subscribe("payments").await.expect("subscribe");
    publish(&broker, "orders", b"o1").await;

    let mut orders = Box::pin(orders.stream());
    assert_eq!(drain(&mut orders).await, payloads(&["o1"]));
    let mut payments = Box::pin(payments.stream());
    assert!(ready(&mut payments).is_none(), "another topic stays silent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_the_cluster_refuses_is_refused() {
    let broker = connected().await;
    let publisher = broker.publisher(KafkaPublish::default());

    let empty = publisher
        .publish(OutgoingMessage::new("", b"x"), None)
        .await
        .expect_err("an empty topic name");
    assert!(matches!(empty, KafkaError::Publish(_)), "{empty}");
    let illegal = publisher
        .publish(OutgoingMessage::new("orders eu", b"x"), None)
        .await
        .expect_err("a topic name Kafka does not accept");
    assert!(matches!(illegal, KafkaError::Publish(_)), "{illegal}");

    // A topic comes into being with the broker's one default partition.
    let missing = publisher
        .publish(
            OutgoingMessage::new("orders", b"x"),
            Some(&KafkaOptions::default().partition(1)),
        )
        .await
        .expect_err("a partition the topic does not have");
    assert!(
        missing.to_string().contains("Unknown partition"),
        "{missing}"
    );

    let big = vec![0u8; 1_000_001];
    let oversized = publisher
        .publish(OutgoingMessage::new("orders", &big), None)
        .await
        .expect_err("a record over message.max.bytes");
    assert!(oversized.to_string().contains("too large"), "{oversized}");

    let subscribe = broker
        .subscribe("")
        .await
        .expect_err("an empty subscription name");
    assert!(
        matches!(subscribe, KafkaError::InvalidOptions(_)),
        "{subscribe}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_producer_settings_come_from_the_production_broker() {
    // The broker the service configures is what the cluster reads its limits from.
    let broker = KafkaBroker::new(["kafka:9092"])
        .default_group("tests")
        .producer_config("message.max.bytes", "1000")
        .connect_in_process()
        .await
        .expect("connect in process");
    let refused = broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new("orders", &[0u8; 1_001]), None)
        .await
        .expect_err("over the configured limit");
    assert!(refused.to_string().contains("too large"), "{refused}");

    let invalid = KafkaBroker::new(["kafka:9092"])
        .producer_config("acks", "sometimes")
        .connect_in_process()
        .await
        .expect_err("librdkafka refuses the value, so the connection does too");
    assert!(matches!(invalid, KafkaError::Connect(_)), "{invalid}");
    let no_servers = KafkaBroker::new(Vec::<String>::new())
        .connect_in_process()
        .await
        .expect_err("no bootstrap server");
    assert!(
        matches!(no_servers, KafkaError::InvalidOptions(_)),
        "{no_servers}"
    );
}

// ------------------------------------------------------------- settlement as a read position

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_auto_commit_requeue_is_unsupported_and_brings_nothing_back() {
    let broker = connected().await;
    let mut subscriber = broker.subscribe("advisory").await.expect("subscribe");
    publish(&broker, "advisory", b"once").await;

    let mut stream = Box::pin(subscriber.stream());
    let first = ready(&mut stream).expect("the record");
    let requeued = first.nack(true).await;
    assert!(
        matches!(requeued, Err(AckError::Unsupported)),
        "auto-commit stored the position as the record was handed over, got {requeued:?}",
    );
    assert!(ready(&mut stream).is_none());
    drop(stream);
    drop(subscriber);

    // Its group committed past it, so the next member does not see it either.
    let mut again = broker.subscribe("advisory").await.expect("subscribe");
    let mut stream = Box::pin(again.stream());
    assert!(
        ready(&mut stream).is_none(),
        "the group committed past the record"
    );
}

/// A handler on a dedicated thread may be the one whose publish opens an exactly-once window,
/// from a runtime that stops afterwards. The window's commit is the pipeline's own task and runs
/// on the runtime the broker connected on, so the window still commits and its record becomes
/// visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_opened_from_a_stopped_runtime_still_commits() {
    let broker = connected().await;
    publish(&broker, "eos-foreign-in", b"a").await;
    let pipeline = KafkaEosPublish::new("eos-foreign")
        .commit_interval(Duration::from_millis(50))
        .pair(&broker)
        .await
        .expect("pair the pipeline");
    let mut subscriber = KafkaTopic::new("eos-foreign-in")
        .group("eos-foreign-workers")
        .start(StartOffset::Earliest)
        .commit(Commit::Transactional("eos-foreign".into()))
        .subscribe(&broker)
        .await
        .expect("subscribe the input");
    let mut output = tracked("eos-foreign-out", "eos-foreign-readers")
        .subscribe(&broker)
        .await
        .expect("subscribe the output");

    let mut input = Box::pin(subscriber.stream());
    let msg = ready(&mut input).expect("the input record");
    let source = SourceOffset::new(msg.topic(), msg.partition(), msg.offset());
    let forwarding = pipeline.clone();
    on_foreign_runtime(async move || {
        forwarding
            .publish(
                &source,
                OutgoingMessage::new("eos-foreign-out", b"a".as_slice()),
                None,
            )
            .await
            .expect("the publish opens the window");
    })
    .await;
    msg.ack().await.expect("ack");

    let mut output = Box::pin(output.stream());
    let committed = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("a window opened from a stopped runtime must still commit")
        .expect("the output stream is open")
        .expect("a delivery, not an error");
    assert_eq!(committed.payload(), b"a");
}

/// Runs `work` on a single-threaded runtime of its own thread, stopped as soon as `work` returns,
/// the way a handler on a dedicated thread publishes.
async fn on_foreign_runtime<Output: Send + 'static>(
    work: impl AsyncFnOnce() -> Output + Send + 'static,
) -> Output {
    let (done, finished) = oneshot::channel();
    thread::spawn(move || {
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime builds");
        let output = runtime.block_on(work());
        drop(runtime);
        let _ = done.send(output);
    });
    finished
        .await
        .expect("the foreign runtime's work completes")
}

/// A tracked subscription reading `topic` in `group`, from the start of the log.
fn tracked(topic: &str, group: &str) -> KafkaTopic {
    KafkaTopic::new(topic)
        .group(group)
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
}

// A retry leaves the offset unsettled, which holds the group's committed position below it. The
// consumer reads on; the record and everything after it come back when the partition is next
// fetched from the committed offset, which is a restart or a rebalance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracked_retry_holds_the_committed_position_until_the_next_member() {
    let broker = connected().await;
    for payload in [b"a".as_slice(), b"b", b"c"] {
        publish(&broker, "rewind", payload).await;
    }
    let mut subscriber = tracked("rewind", "workers")
        .subscribe(&broker)
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());
    let a = ready(&mut stream).expect("a");
    a.ack().await.expect("ack");
    let b = ready(&mut stream).expect("b");
    b.nack(true).await.expect("a tracked requeue");
    let c = ready(&mut stream).expect("the consumer reads on past the retried record");
    assert_eq!(c.payload(), b"c");
    c.ack().await.expect("ack");
    assert!(
        ready(&mut stream).is_none(),
        "nothing comes back in this session"
    );
    drop(stream);
    drop(subscriber);

    let mut restarted = tracked("rewind", "workers")
        .subscribe(&broker)
        .await
        .expect("subscribe");
    let mut stream = Box::pin(restarted.stream());
    assert_eq!(
        drain(&mut stream).await,
        payloads(&["b", "c"]),
        "the group resumes at the retried record, so the tail behind it replays too",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_record_settles_its_offset() {
    let broker = connected().await;
    for payload in [b"a".as_slice(), b"b"] {
        publish(&broker, "dropped", payload).await;
    }
    let mut subscriber = tracked("dropped", "workers")
        .subscribe(&broker)
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());
    ready(&mut stream)
        .expect("a")
        .nack(false)
        .await
        .expect("drop");
    ready(&mut stream).expect("b").ack().await.expect("ack");
    drop(stream);
    drop(subscriber);

    let mut restarted = tracked("dropped", "workers")
        .subscribe(&broker)
        .await
        .expect("subscribe");
    let mut stream = Box::pin(restarted.stream());
    assert!(
        drain(&mut stream).await.is_empty(),
        "a dropped and an acked record both moved the committed position past themselves",
    );
}

// ------------------------------------------------------------------------- consumer groups

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_reads_each_record_once_and_every_group_reads_its_own_copy() {
    let broker = connected().await;
    let worker = || KafkaTopic::new("shared").group("workers");
    let mut first = worker().subscribe(&broker).await.expect("subscribe first");
    let mut second = worker().subscribe(&broker).await.expect("subscribe second");
    let mut auditor = KafkaTopic::new("shared")
        .group("audit")
        .subscribe(&broker)
        .await
        .expect("subscribe auditor");
    for payload in [b"1".as_slice(), b"2", b"3"] {
        publish(&broker, "shared", payload).await;
    }

    let one = drain(&mut Box::pin(first.stream())).await;
    let two = drain(&mut Box::pin(second.stream())).await;
    let audit = drain(&mut Box::pin(auditor.stream())).await;
    // The topic has one partition and a group hands a partition to one member.
    assert_eq!(
        (one, two),
        (payloads(&["1", "2", "3"]), Vec::new()),
        "the group's one partition is owned by one member",
    );
    assert_eq!(
        audit,
        payloads(&["1", "2", "3"]),
        "a second group reads its own copy"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_brokers_default_group_makes_bare_subscriptions_compete() {
    let broker = connected().await;
    let mut first = broker.subscribe("bare").await.expect("subscribe first");
    let mut second = broker.subscribe("bare").await.expect("subscribe second");
    publish(&broker, "bare", b"one").await;

    let one = drain(&mut Box::pin(first.stream())).await;
    let two = drain(&mut Box::pin(second.stream())).await;
    assert_eq!(one.len() + two.len(), 1, "{one:?} and {two:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_leaving_hands_its_partition_over_at_the_committed_offset() {
    let broker = connected().await;
    let worker = || tracked("handover", "workers");
    let mut first = worker().subscribe(&broker).await.expect("subscribe first");
    let mut second = worker().subscribe(&broker).await.expect("subscribe second");
    publish(&broker, "handover", b"handled").await;
    publish(&broker, "handover", b"unhandled").await;

    let mut stream = Box::pin(first.stream());
    ready(&mut stream)
        .expect("handled")
        .ack()
        .await
        .expect("ack");
    drop(stream);
    assert!(drain(&mut Box::pin(second.stream())).await.is_empty());
    drop(first);

    // The rebalance hands the partition to the survivor, which resumes where the group committed.
    assert_eq!(
        drain(&mut Box::pin(second.stream())).await,
        payloads(&["unhandled"]),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_starts_where_the_descriptor_says_when_it_has_committed_nothing() {
    let broker = connected().await;
    publish(&broker, "history", b"old").await;

    let mut latest = KafkaTopic::new("history")
        .group("latest")
        .subscribe(&broker)
        .await
        .expect("subscribe");
    let mut earliest = KafkaTopic::new("history")
        .group("earliest")
        .start(StartOffset::Earliest)
        .subscribe(&broker)
        .await
        .expect("subscribe");
    publish(&broker, "history", b"new").await;

    assert_eq!(
        drain(&mut Box::pin(latest.stream())).await,
        payloads(&["new"]),
        "librdkafka's default resets a new group to the end of the log",
    );
    assert_eq!(
        drain(&mut Box::pin(earliest.stream())).await,
        payloads(&["old", "new"]),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_lists_and_patterns_read_the_topics_they_name_and_match() {
    let broker = connected().await;
    let mut listed = KafkaTopics::new(["orders", "cancellations"])
        .group("listed")
        .subscribe(&broker)
        .await
        .expect("subscribe");
    let mut matched = KafkaTopics::pattern("^orders\\..*")
        .group("matched")
        .subscribe(&broker)
        .await
        .expect("subscribe");
    publish(&broker, "orders", b"o1").await;
    publish(&broker, "cancellations", b"c1").await;
    // A topic that comes into being after the subscription is matched as it appears.
    publish(&broker, "orders.eu", b"eu1").await;

    let mut seen = drain(&mut Box::pin(listed.stream())).await;
    seen.sort();
    assert_eq!(seen, payloads(&["c1", "o1"]));
    assert_eq!(
        drain(&mut Box::pin(matched.stream())).await,
        payloads(&["eu1"])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partition_list_reads_its_partitions_apart_from_any_group() {
    let broker = connected().await;
    publish(&broker, "assigned", b"before").await;
    let mut reader = KafkaPartitions::new("assigned", [0])
        .start(StartOffset::Earliest)
        .subscribe(&broker)
        .await
        .expect("subscribe");
    // A group member on the same topic does not take the partition away from it.
    let mut member = KafkaTopic::new("assigned")
        .group("workers")
        .start(StartOffset::Earliest)
        .subscribe(&broker)
        .await
        .expect("subscribe");
    publish(&broker, "assigned", b"after").await;

    assert_eq!(
        drain(&mut Box::pin(reader.stream())).await,
        payloads(&["before", "after"])
    );
    assert_eq!(
        drain(&mut Box::pin(member.stream())).await,
        payloads(&["before", "after"])
    );
}

/// Publishes one keyed and one keyless record to `topic`.
async fn publish_keyed_pair(broker: &ConnectedKafkaBroker, topic: &str) {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert(PARTITION_KEY_HEADER, "k-1");
    broker
        .publisher(KafkaPublish::default())
        .publish(
            OutgoingMessage::new(topic, b"{}").with_headers(headers),
            None,
        )
        .await
        .expect("publish");
    publish(broker, topic, b"plain").await;
}

// `partition_key` is the keyed-lane key, not the record key: the subscriber resolves it from the
// descriptor's `LaneKey`, so a `workers(n, by_key)` handler lanes the same in process as on a
// cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_and_the_key_travel_and_lanes_follow_the_descriptor() {
    let broker = connected().await;
    let mut by_partition = broker.subscribe("keyed").await.expect("subscribe");
    let mut by_key = KafkaTopic::new("keyed")
        .group("by-key")
        .lane_key(LaneKey::RecordKey)
        .subscribe(&broker)
        .await
        .expect("subscribe");
    publish_keyed_pair(&broker, "keyed").await;

    let mut stream = Box::pin(by_partition.stream());
    let keyed = ready(&mut stream).expect("keyed");
    assert_eq!(
        keyed.headers().get_str("content-type"),
        Some("application/json")
    );
    assert_eq!(keyed.key(), Some(b"k-1".as_slice()));
    assert_eq!(keyed.headers().get_str(PARTITION_KEY_HEADER), Some("k-1"));
    assert_eq!(Partitioned::partition_key(&keyed), Some(b"0".as_slice()));
    let keyless = ready(&mut stream).expect("keyless");
    assert_eq!(
        IncomingMessage::partition_key(&keyless),
        Some(b"0".as_slice())
    );
    assert!(
        keyless.timestamp_millis().is_some(),
        "a record carries its create time"
    );

    let mut stream = Box::pin(by_key.stream());
    let keyed = ready(&mut stream).expect("keyed");
    assert_eq!(Partitioned::partition_key(&keyed), Some(b"k-1".as_slice()));
    let keyless = ready(&mut stream).expect("keyless");
    assert!(Partitioned::partition_key(&keyless).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_publish_log_reads_back_what_the_topic_holds() {
    let broker = connected().await;
    publish(&broker, "audit", b"first").await;
    publish(&broker, "audit", b"second").await;

    let observed = broker.published("audit");
    let observed: Vec<&[u8]> = observed.iter().map(RawMessage::payload).collect();
    assert_eq!(observed, [b"first".as_slice(), b"second"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_can_be_reentered_without_losing_deliveries() {
    let broker = connected().await;
    let mut subscriber = broker.subscribe("reenter").await.expect("subscribe");
    publish(&broker, "reenter", b"one").await;
    assert_eq!(
        drain(&mut Box::pin(subscriber.stream())).await,
        payloads(&["one"])
    );
    publish(&broker, "reenter", b"two").await;
    assert_eq!(
        drain(&mut Box::pin(subscriber.stream())).await,
        payloads(&["two"])
    );
}

// ------------------------------------------------------------------------------ positions

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_position_opens_the_subscription_on_what_the_log_holds() {
    let broker = connected().await;
    for payload in [b"a".as_slice(), b"b"] {
        publish(&broker, "replay", payload).await;
    }
    let mut subscriber = broker.subscribe("replay").await.expect("subscribe");
    // What a mount site's `start_at(..)` does before the first poll.
    subscriber
        .seeker()
        .seek(KafkaPosition::earliest())
        .await
        .expect("seek");
    assert_eq!(
        drain(&mut Box::pin(subscriber.stream())).await,
        payloads(&["a", "b"])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_position_resolves_against_the_partitions_the_member_reads() {
    let broker = connected().await;
    let mut subscriber = broker.subscribe("positions").await.expect("subscribe");
    publish(&broker, "positions", b"a").await;
    let mut stream = Box::pin(subscriber.stream());
    let a = ready(&mut stream).expect("a");
    let stamped = a.timestamp_millis().expect("create time");
    a.ack().await.expect("ack");
    drop(stream);
    let seeker = subscriber.seeker();

    // A timestamp resolves to the first record stamped at or after it.
    seeker
        .seek(KafkaPosition::timestamp(stamped))
        .await
        .expect("a timestamp resolves");
    assert_eq!(
        drain(&mut Box::pin(subscriber.stream())).await,
        payloads(&["a"])
    );

    // A partition or a topic the member does not read is refused, as the live seeker refuses it.
    let other_partition = seeker
        .seek(KafkaPosition::offset(3, 0))
        .await
        .expect_err("the topic has one partition");
    assert!(
        matches!(other_partition, KafkaError::InvalidOptions(_)),
        "{other_partition}"
    );
    let other_topic = seeker
        .seek(KafkaPosition::topic_offset("elsewhere", 0, 0))
        .await
        .expect_err("a topic the subscription does not read");
    assert!(
        matches!(other_topic, KafkaError::InvalidOptions(_)),
        "{other_topic}"
    );
}

// ----------------------------------------------------------------------------- transactions

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_committed_reader_sees_a_transaction_at_its_commit_and_never_after_an_abort() {
    let broker = connected().await;
    let mut subscriber = broker.subscribe("txn-out").await.expect("subscribe");
    let publisher = broker
        .transactional_publisher(KafkaPublish::default().transactional_id("txn-1"))
        .await
        .expect("transactional publisher");
    let mut stream = Box::pin(subscriber.stream());

    // With nothing open the record goes out through the plain producer.
    publisher
        .publish(OutgoingMessage::new("txn-out", b"plain"), None)
        .await
        .expect("publish");
    assert_eq!(drain(&mut stream).await, payloads(&["plain"]));

    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("txn-out", b"aborted"), None)
        .await
        .expect("publish");
    publisher.abort().await.expect("abort");
    assert!(
        drain(&mut stream).await.is_empty(),
        "an aborted record is never read"
    );

    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("txn-out", b"one"), None)
        .await
        .expect("publish");
    publisher
        .publish(OutgoingMessage::new("txn-out", b"two"), None)
        .await
        .expect("publish");
    assert!(
        drain(&mut stream).await.is_empty(),
        "an open transaction is invisible"
    );
    publisher.commit().await.expect("commit");
    assert_eq!(drain(&mut stream).await, payloads(&["one", "two"]));
    let log: Vec<Vec<u8>> = broker
        .published("txn-out")
        .iter()
        .map(|msg| msg.payload().to_vec())
        .collect();
    assert_eq!(log, payloads(&["plain", "one", "two"]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_uncommitted_reader_reads_a_transaction_as_it_is_written() {
    let broker = connected().await;
    let mut reader = KafkaTopic::new("dirty")
        .group("dirty")
        .config("isolation.level", "read_uncommitted")
        .subscribe(&broker)
        .await
        .expect("subscribe");
    let publisher = broker
        .transactional_publisher(KafkaPublish::default().transactional_id("txn-dirty"))
        .await
        .expect("transactional publisher");
    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("dirty", b"pending"), None)
        .await
        .expect("publish");
    assert_eq!(
        drain(&mut Box::pin(reader.stream())).await,
        payloads(&["pending"])
    );
    publisher.abort().await.expect("abort");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_pairing_of_one_transactional_id_fences_the_first() {
    let broker = connected().await;
    let older = broker
        .transactional_publisher(KafkaPublish::default().transactional_id("txn-zombie"))
        .await
        .expect("transactional publisher");
    older.begin_transaction().await.expect("begin");
    older
        .publish(OutgoingMessage::new("zombie-out", b"zombie"), None)
        .await
        .expect("publish");

    let newer = broker
        .transactional_publisher(KafkaPublish::default().transactional_id("txn-zombie"))
        .await
        .expect("transactional publisher");
    let fenced = older
        .commit()
        .await
        .expect_err("the older producer is fenced");
    assert!(fenced.to_string().contains("fenced"), "{fenced}");
    assert!(
        broker.published("zombie-out").is_empty(),
        "the fenced producer's open transaction was aborted",
    );

    newer.begin_transaction().await.expect("begin");
    newer
        .publish(OutgoingMessage::new("zombie-out", b"live"), None)
        .await
        .expect("publish");
    newer.commit().await.expect("commit");
    assert_eq!(broker.published("zombie-out").len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_misuse_errors_instead_of_silently_succeeding() {
    let broker = connected().await;
    let publisher = broker
        .transactional_publisher(KafkaPublish::default().transactional_id("txn-misuse"))
        .await
        .expect("transactional publisher");

    let no_commit = publisher.commit().await.expect_err("nothing is open");
    assert!(
        matches!(&no_commit, KafkaError::NoTransaction { id } if id == "txn-misuse"),
        "{no_commit}",
    );
    let no_abort = publisher.abort().await.expect_err("nothing is open");
    assert!(
        matches!(&no_abort, KafkaError::NoTransaction { .. }),
        "{no_abort}"
    );

    publisher.begin_transaction().await.expect("begin");
    // Clones share the handle's one transaction, as clones of the live publisher share one
    // producer.
    let busy = publisher
        .clone()
        .begin_transaction()
        .await
        .expect_err("a second begin");
    assert!(
        matches!(&busy, KafkaError::TransactionBusy { id } if id == "txn-misuse"),
        "{busy}",
    );
    publisher
        .publish(OutgoingMessage::new("txn-misuse-out", b"kept"), None)
        .await
        .expect("publish");
    publisher.commit().await.expect("commit");
    assert_eq!(broker.published("txn-misuse-out").len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_lanes_hand_out_independent_transactions() {
    let broker = connected().await;
    let lanes = KafkaPublish::default()
        .transactional_id("lanes-svc")
        .per_partition()
        .pair(&broker)
        .await
        .expect("pair");

    let p0 = lanes.for_partition(0).await.expect("lane 0");
    let p1 = lanes.for_partition(1).await.expect("lane 1");
    assert_eq!(p0.id(), "lanes-svc-p0");
    assert_eq!(p1.id(), "lanes-svc-p1");
    p0.begin_transaction().await.expect("begin p0");
    let again = lanes.for_partition(0).await.expect("lane 0 again");
    let busy = again
        .begin_transaction()
        .await
        .expect_err("a cached lane is the same handle");
    assert!(
        matches!(&busy, KafkaError::TransactionBusy { .. }),
        "{busy}"
    );

    p1.begin_transaction().await.expect("begin p1");
    p0.publish(OutgoingMessage::new("lane-out", b"p0"), None)
        .await
        .expect("publish p0");
    p1.publish(OutgoingMessage::new("lane-out", b"p1"), None)
        .await
        .expect("publish p1");
    p0.commit().await.expect("commit p0");
    p1.abort().await.expect("abort p1");
    let landed: Vec<Vec<u8>> = broker
        .published("lane-out")
        .iter()
        .map(|msg| msg.payload().to_vec())
        .collect();
    assert_eq!(
        landed,
        payloads(&["p0"]),
        "the aborted lane leaves nothing behind"
    );
}

// ------------------------------------------------------------------------------- shutdown

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handles_aliasing_a_shut_down_connection_error() {
    let broker = connected().await;
    let publisher = broker.publisher(KafkaPublish::default());
    let transactional = broker
        .transactional_publisher(KafkaPublish::default().transactional_id("txn-closed"))
        .await
        .expect("transactional publisher");
    transactional.begin_transaction().await.expect("begin");

    let closed = broker.shutdown().await.expect("shutdown");
    assert_eq!(closed.unflushed_records(), 0);

    let err = publisher
        .publish(OutgoingMessage::new("orders", b"after"), None)
        .await
        .expect_err("publishing after shutdown");
    assert!(
        matches!(&err, KafkaError::Closed { topic } if topic == "orders"),
        "{err}"
    );
    for settled in [transactional.commit().await, transactional.abort().await] {
        let err = settled.expect_err("a transaction control call after shutdown");
        assert!(
            matches!(&err, KafkaError::Closed { topic } if topic == "txn-closed"),
            "{err}"
        );
    }
}
