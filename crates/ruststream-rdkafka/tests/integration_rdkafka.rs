//! Integration tests against a real Kafka.
//!
//! Every test is a no-op unless `KAFKA_TEST_URL` points at a cluster:
//!
//! ```text
//! just brokers-up
//! KAFKA_TEST_URL=127.0.0.1:9092 cargo test --workspace --all-features -- --test-threads=1
//! ```
//!
//! These cover exactly what the in-process test broker does not simulate: consumer groups,
//! committed positions across subscriber restarts, the two commit modes, start offsets, and
//! native-key partitioning.

use std::collections::HashMap;
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::process;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer as _};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use ruststream::runtime::{
    App, AppInfo, ContextKind, Ctx, DefaultSlot, HandlerOutcome, Out, Outgoing as OutgoingRecord,
    PublishTransform, RETRY_COUNT_HEADER as RUNTIME_RETRY_COUNT_HEADER, Reads, Reply, RustStream,
    State, SubscriberSettings as _,
};
use ruststream::subscriber;
use ruststream::{
    Broker, ConnectedBroker, FromRef, HeaderMap, IncomingMessage, Outgoing, OutgoingMessage,
    Positioned, PublishPolicy, Publisher, Seekable, Seeker, Subscriber, TransactionalPublisher,
    nonzero,
};
use ruststream_rdkafka::context::{KafkaContext, keys};
use ruststream_rdkafka::{
    Assignment, Commit, ConnectedKafkaBroker, EosPipeline, EosReplies, KafkaBroker,
    KafkaEosPublish, KafkaError, KafkaMessage, KafkaOptions, KafkaPartitions, KafkaPosition,
    KafkaPublish, KafkaTopic, KafkaTopics, LaneKey, PARTITION_KEY_HEADER, PartitionLanes,
    RoundRobin, SourceOffset, StartOffset, ToSourceTopic,
};
use serde::Deserialize;
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(15);

mod live;

fn kafka_url() -> Option<String> {
    live::url("KAFKA_TEST_URL")
}

/// Per-run unique names so reruns never see another run's topics or committed positions.
fn unique(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "{base}-{}-{}",
        process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Recreates a fixed-name topic from scratch: deleting it drops prior runs' segments and
/// transaction markers, so an aborted or still-open transaction from a dead test process
/// cannot hold the new run's last-stable-offset (`read_committed` readers would see nothing
/// until the old transaction times out).
async fn recreate_topic(url: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("admin client");
    let _ = admin
        .delete_topics(&[topic], &AdminOptions::new())
        .await
        .expect("delete_topics call");
    // Deletion completes asynchronously. Waiting on the cluster's own answer is what makes this
    // deterministic: a metadata fetch blocks until the broker replies, so the loop is paced by
    // the condition it is waiting for rather than by a guessed delay.
    await_topic_absent(&admin, topic);
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await
        .expect("create_topics call");
    match &results[0] {
        Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
        Err((name, code)) => panic!("recreate {name}: {code}"),
    }
}

/// Polls cluster metadata until `topic` is gone, bounded by [`WAIT`].
///
/// Each fetch is a round trip to the broker with its own timeout, so the loop advances only when
/// the cluster has answered. The fetch asks for the whole cluster rather than for this one topic:
/// the stand has topic auto-creation on, and a metadata request naming a single topic is itself
/// what creates it - with the broker default of one partition, which then swallows the partition
/// count the caller asked for.
fn await_topic_absent(admin: &AdminClient<DefaultClientContext>, topic: &str) {
    let probe = Duration::from_millis(500);
    let deadline = std::time::Instant::now() + WAIT;
    while std::time::Instant::now() < deadline {
        let metadata = admin
            .inner()
            .fetch_metadata(None, probe)
            .expect("fetch_metadata call");
        let present = metadata
            .topics()
            .iter()
            .any(|known| known.name() == topic && known.error().is_none());
        if !present {
            return;
        }
    }
    panic!("topic {topic} was still present {WAIT:?} after the delete request");
}

/// Asserts the cluster really holds `topic` with `partitions` partitions.
///
/// A test that asks for a partitioned topic and silently gets a one-partition one proves nothing
/// about placement, so the count is checked rather than assumed.
fn assert_partition_count(url: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("admin client");
    let metadata = admin
        .inner()
        .fetch_metadata(None, Duration::from_secs(5))
        .expect("fetch_metadata call");
    let found = metadata
        .topics()
        .iter()
        .find(|known| known.name() == topic)
        .unwrap_or_else(|| panic!("topic {topic} must exist"));
    assert_eq!(
        i32::try_from(found.partitions().len()).expect("small partition count"),
        partitions,
        "topic {topic} must hold the partitions the test asked for",
    );
}

/// Creates `topic` up front so the first subscribe does not race topic auto-creation.
async fn create_topic(url: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("admin client");
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await
        .expect("create_topics call");
    for result in results {
        match result {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => panic!("creating topic {name} failed: {code}"),
        }
    }
}

async fn connected_broker(url: &str) -> ConnectedKafkaBroker {
    KafkaBroker::new([url.to_owned()])
        .connect()
        .await
        .expect("connect")
}

fn tracked(topic: &str, group: &str) -> KafkaTopic {
    KafkaTopic::new(topic)
        .group(group)
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
}

async fn next_message<S>(stream: &mut S) -> KafkaMessage
where
    S: Stream<Item = Result<KafkaMessage, KafkaError>> + Unpin,
{
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok")
}

async fn publish(broker: &ConnectedKafkaBroker, topic: &str, payload: &[u8]) {
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(topic, payload), None)
        .await
        .expect("publish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trip_with_headers_and_key() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("roundtrip");
    create_topic(&url, &topic, 1).await;
    let broker = connected_broker(&url).await;

    let mut subscriber = broker
        .subscribe_with(tracked(&topic, &unique("group")))
        .await
        .expect("subscribe");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert(PARTITION_KEY_HEADER, "order-1");
    broker
        .publisher(KafkaPublish::default())
        .publish(
            OutgoingMessage::new(&topic, b"{}").with_headers(headers),
            None,
        )
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    assert_eq!(msg.payload(), b"{}");
    assert_eq!(
        msg.headers().get_str("content-type"),
        Some("application/json")
    );
    // The native record key comes back as the partition-key header; the lane key defaults to
    // the source partition (a single-partition topic, so partition 0).
    assert_eq!(msg.key(), Some(b"order-1".as_slice()));
    assert_eq!(IncomingMessage::partition_key(&msg), Some(b"0".as_slice()));
    assert_eq!(msg.topic(), topic);
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracked_commit_survives_subscriber_restart() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("tracked");
    let group = unique("group");
    create_topic(&url, &topic, 1).await;
    let broker = connected_broker(&url).await;

    publish(&broker, &topic, b"m1").await;
    publish(&broker, &topic, b"m2").await;

    {
        let mut subscriber = broker
            .subscribe_with(tracked(&topic, &group))
            .await
            .expect("subscribe");
        let mut stream = Box::pin(subscriber.stream());
        let first = next_message(&mut stream).await;
        assert_eq!(first.payload(), b"m1");
        first.ack().await.expect("ack m1");
        let second = next_message(&mut stream).await;
        assert_eq!(second.payload(), b"m2");
        second.ack().await.expect("ack m2");
        // Dropping the subscriber closes the consumer; auto-commit flushes the stored
        // watermark on close.
    }

    publish(&broker, &topic, b"m3").await;

    let mut subscriber = broker
        .subscribe_with(tracked(&topic, &group))
        .await
        .expect("re-subscribe");
    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    assert_eq!(
        msg.payload(),
        b"m3",
        "acked offsets must not be redelivered to the same group",
    );
    msg.ack().await.expect("ack m3");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_requeue_leaves_offset_for_redelivery() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("requeue");
    let group = unique("group");
    create_topic(&url, &topic, 1).await;
    let broker = connected_broker(&url).await;

    publish(&broker, &topic, b"poison").await;

    {
        let mut subscriber = broker
            .subscribe_with(tracked(&topic, &group))
            .await
            .expect("subscribe");
        let mut stream = Box::pin(subscriber.stream());
        let msg = next_message(&mut stream).await;
        assert_eq!(msg.payload(), b"poison");
        msg.nack(true).await.expect("nack requeue");
    }

    let mut subscriber = broker
        .subscribe_with(tracked(&topic, &group))
        .await
        .expect("re-subscribe");
    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    assert_eq!(
        msg.payload(),
        b"poison",
        "nack(true) must leave the offset uncommitted for redelivery",
    );
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_drop_settles_the_offset() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("drop");
    let group = unique("group");
    create_topic(&url, &topic, 1).await;
    let broker = connected_broker(&url).await;

    publish(&broker, &topic, b"skip-me").await;

    {
        let mut subscriber = broker
            .subscribe_with(tracked(&topic, &group))
            .await
            .expect("subscribe");
        let mut stream = Box::pin(subscriber.stream());
        let msg = next_message(&mut stream).await;
        assert_eq!(msg.payload(), b"skip-me");
        msg.nack(false).await.expect("nack drop");
    }

    publish(&broker, &topic, b"next").await;

    let mut subscriber = broker
        .subscribe_with(tracked(&topic, &group))
        .await
        .expect("re-subscribe");
    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    assert_eq!(
        msg.payload(),
        b"next",
        "nack(false) must settle the offset so it is not redelivered",
    );
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn out_of_order_acks_commit_only_the_contiguous_prefix() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("watermark");
    let group = unique("group");
    create_topic(&url, &topic, 1).await;
    let broker = connected_broker(&url).await;

    for payload in [b"a".as_slice(), b"b", b"c"] {
        publish(&broker, &topic, payload).await;
    }

    {
        let mut subscriber = broker
            .subscribe_with(tracked(&topic, &group))
            .await
            .expect("subscribe");
        let mut stream = Box::pin(subscriber.stream());
        let first = next_message(&mut stream).await;
        let second = next_message(&mut stream).await;
        let third = next_message(&mut stream).await;
        assert_eq!(first.payload(), b"a");
        // Ack out of order and leave "b" unsettled: only "a" may end up committed.
        third.ack().await.expect("ack c");
        first.ack().await.expect("ack a");
        drop(second);
    }

    let mut subscriber = broker
        .subscribe_with(tracked(&topic, &group))
        .await
        .expect("re-subscribe");
    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    assert_eq!(
        msg.payload(),
        b"b",
        "the committed position must stop at the first unsettled offset",
    );
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_commit_mode_receives_and_acks_advisorily() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("auto");
    create_topic(&url, &topic, 1).await;
    let broker = connected_broker(&url).await;

    let def = KafkaTopic::new(&topic)
        .group(unique("group"))
        .start(StartOffset::Earliest);
    let mut subscriber = broker.subscribe_with(def).await.expect("subscribe");

    publish(&broker, &topic, b"auto-1").await;

    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    assert_eq!(msg.payload(), b"auto-1");
    msg.ack().await.expect("advisory ack always succeeds");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_key_lands_on_one_partition() {
    const COUNT: usize = 8;
    let Some(url) = kafka_url() else { return };
    let topic = unique("keyed");
    create_topic(&url, &topic, 4).await;
    let broker = connected_broker(&url).await;

    let mut subscriber = broker
        .subscribe_with(tracked(&topic, &unique("group")))
        .await
        .expect("subscribe");

    for i in 0..COUNT {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "same-key");
        broker
            .publisher(KafkaPublish::default())
            .publish(
                OutgoingMessage::new(&topic, format!("k{i}").as_bytes()).with_headers(headers),
                None,
            )
            .await
            .expect("publish");
    }

    let mut stream = Box::pin(subscriber.stream());
    let mut partitions = Vec::new();
    for _ in 0..COUNT {
        let msg = next_message(&mut stream).await;
        assert_eq!(msg.key(), Some(b"same-key".as_slice()));
        partitions.push(msg.partition());
        msg.ack().await.expect("ack");
    }
    let first = partitions[0];
    assert!(
        partitions.iter().all(|partition| *partition == first),
        "records sharing a key must land on one partition, got {partitions:?}",
    );

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_name_subscribe_uses_the_default_group() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("bare");
    create_topic(&url, &topic, 1).await;

    let broker = KafkaBroker::new([url.clone()])
        .default_group(unique("default-group"))
        .connect()
        .await
        .expect("connect");

    let mut subscriber = ruststream::Subscribe::subscribe(&broker, &topic)
        .await
        .expect("subscribe by bare name");

    // The bare-name form runs on librdkafka defaults (reset = latest), so a record published
    // before the group finishes joining is legitimately skipped. Publish until one lands past
    // the assignment point.
    let mut stream = Box::pin(subscriber.stream());
    let mut received = None;
    for _ in 0..20 {
        publish(&broker, &topic, b"bare").await;
        match tokio::time::timeout(Duration::from_secs(1), stream.next()).await {
            Ok(Some(Ok(msg))) => {
                received = Some(msg);
                break;
            }
            Ok(Some(Err(err))) => panic!("delivery failed: {err}"),
            Ok(None) => panic!("stream ended"),
            Err(_) => {}
        }
    }
    let msg = received.expect("a delivery once the group is assigned");
    assert_eq!(msg.payload(), b"bare");
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_preserve_order_and_settle_per_message() {
    use ruststream::{BatchSubscriber as _, SubscriptionSource as _};

    const COUNT: usize = 12;
    /// Smaller than the run, so the run cannot come back as one batch: the cap is what is
    /// under test.
    const BATCH: usize = 5;
    let Some(url) = kafka_url() else { return };
    let topic = unique("batches");
    create_topic(&url, &topic, 1).await;
    let broker = connected_broker(&url).await;

    for i in 0..COUNT {
        publish(&broker, &topic, format!("b{i:02}").as_bytes()).await;
    }

    let source = tracked(&topic, &unique("group"));
    let mut subscriber = source.subscribe(&broker).await.expect("subscribe");

    let mut stream =
        Box::pin(subscriber.batches(NonZeroUsize::new(BATCH).expect("non-zero batch")));
    let mut payloads = Vec::new();
    while payloads.len() < COUNT {
        let batch = tokio::time::timeout(WAIT, stream.next())
            .await
            .expect("batch within timeout")
            .expect("stream has next")
            .expect("batch ok");
        assert!(!batch.is_empty(), "a yielded batch must not be empty");
        assert!(
            batch.len() <= BATCH,
            "a batch must never exceed the size it was opened at, got {}",
            batch.len()
        );
        for msg in batch {
            payloads.push(String::from_utf8(msg.payload().to_vec()).expect("utf8"));
            msg.ack().await.expect("ack");
        }
    }
    let expected: Vec<String> = (0..COUNT).map(|i| format!("b{i:02}")).collect();
    assert_eq!(payloads, expected, "batches must preserve publish order");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_topic_subscription_consumes_all_topics() {
    let Some(url) = kafka_url() else { return };
    let orders = unique("mt-orders");
    let cancels = unique("mt-cancels");
    create_topic(&url, &orders, 1).await;
    create_topic(&url, &cancels, 1).await;
    let broker = connected_broker(&url).await;

    let def = KafkaTopics::new([orders.clone(), cancels.clone()])
        .group(unique("group"))
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked);
    let mut subscriber = broker.subscribe_with(def).await.expect("subscribe");

    publish(&broker, &orders, b"o1").await;
    publish(&broker, &cancels, b"c1").await;

    let mut stream = Box::pin(subscriber.stream());
    let mut seen = HashMap::new();
    for _ in 0..2 {
        let msg = next_message(&mut stream).await;
        seen.insert(msg.topic().to_owned(), msg.payload().to_vec());
        msg.ack().await.expect("ack");
    }
    assert_eq!(
        seen.get(orders.as_str()).map(Vec::as_slice),
        Some(b"o1".as_slice())
    );
    assert_eq!(
        seen.get(cancels.as_str()).map(Vec::as_slice),
        Some(b"c1".as_slice())
    );

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pattern_subscription_consumes_matching_topics() {
    let Some(url) = kafka_url() else { return };
    let prefix = unique("pat");
    let first = format!("{prefix}-a");
    let second = format!("{prefix}-b");
    create_topic(&url, &first, 1).await;
    create_topic(&url, &second, 1).await;
    let broker = connected_broker(&url).await;

    let def = KafkaTopics::pattern(format!("^{prefix}-.*"))
        .group(unique("group"))
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked);
    let mut subscriber = broker.subscribe_with(def).await.expect("subscribe");

    publish(&broker, &first, b"p1").await;
    publish(&broker, &second, b"p2").await;

    let mut stream = Box::pin(subscriber.stream());
    let mut topics = Vec::new();
    for _ in 0..2 {
        let msg = next_message(&mut stream).await;
        topics.push(msg.topic().to_owned());
        msg.ack().await.expect("ack");
    }
    topics.sort();
    assert_eq!(topics, vec![first, second]);

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unanchored_pattern_is_rejected() {
    let Some(url) = kafka_url() else { return };
    let broker = connected_broker(&url).await;

    let err = broker
        .subscribe_with(KafkaTopics::pattern("no-anchor").group(unique("group")))
        .await
        .expect_err("an unanchored pattern must be rejected");
    assert!(
        err.to_string().contains('^'),
        "the error must explain the anchor requirement, got: {err}",
    );

    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cooperative_sticky_assignment_round_trips() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("coop");
    create_topic(&url, &topic, 2).await;
    let broker = connected_broker(&url).await;

    let def = tracked(&topic, &unique("group")).assignment(Assignment::CooperativeSticky);
    let mut subscriber = broker.subscribe_with(def).await.expect("subscribe");

    publish(&broker, &topic, b"coop-1").await;

    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    assert_eq!(msg.payload(), b"coop-1");
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_created_topic_recovers_without_stream_errors() {
    let Some(url) = kafka_url() else { return };
    // Deliberately NOT created up front: the consumer must ride out UnknownTopicOrPartition.
    let topic = unique("late");
    let broker = connected_broker(&url).await;

    // Bound librdkafka's metadata refresh so the late topic is noticed promptly.
    let def =
        tracked(&topic, &unique("group")).config("topic.metadata.refresh.interval.ms", "1000");
    let mut subscriber = broker.subscribe_with(def).await.expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());

    // While the topic does not exist the stream stays silent: the pending-creation errors are
    // classified as transient and logged, not yielded.
    let quiet = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert!(
        quiet.is_err(),
        "the stream must stay silent while the topic is pending",
    );

    // The first publish auto-creates the topic; the consumer notices and delivers.
    publish(&broker, &topic, b"late-1").await;
    let msg = next_message(&mut stream).await;
    assert_eq!(msg.payload(), b"late-1");
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_group_fails_subscription_clearly() {
    let Some(url) = kafka_url() else { return };
    let broker = connected_broker(&url).await;

    let err = broker
        .subscribe_with(KafkaTopic::new(unique("nogroup")))
        .await
        .expect_err("subscribing without a group must fail");
    let message = err.to_string();
    assert!(
        message.contains("consumer group"),
        "the error must name the missing option, got: {message}",
    );

    broker.shutdown().await.expect("shutdown");
}

#[derive(Debug, Deserialize)]
struct Tagged {
    tag: String,
}

/// Collects handled tags and wakes the test once the expected count for this run arrived, and
/// remembers the widest batch it was handed so the size cap can be asserted afterwards.
#[derive(Clone)]
struct BatchPoolState {
    prefix: String,
    expected: usize,
    seen: Arc<Mutex<Vec<String>>>,
    widest_batch: Arc<AtomicUsize>,
    done: Arc<Notify>,
}

impl BatchPoolState {
    fn record(&self, tag: &str) {
        if !tag.starts_with(&self.prefix) {
            return;
        }
        let mut seen = self.seen.lock().expect("seen mutex poisoned");
        seen.push(tag.to_owned());
        if seen.len() >= self.expected {
            self.done.notify_one();
        }
    }

    fn saw_batch(&self, len: usize) {
        self.widest_batch.fetch_max(len, Ordering::SeqCst);
    }
}

// Batches from a fixed topic, up to four in flight at once; the state filters by the run's
// prefix so reruns against a long-lived cluster stay isolated.
#[subscriber(
    // Native batches: a batch is one delivery plus whatever librdkafka already fetched, capped by
    // the size the mount site names below; the slice parameter here is what asks for one. A fixed
    // group keeps reruns idempotent (only this run's fresh publishes arrive), and the state
    // filters by the run prefix.
    KafkaTopic::new("e2e-batch-pool")
        .group("e2e-batch-pool-group")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked),
    workers(4)
)]
async fn pool_batch(items: &[Tagged], ctx: &mut Context<'_, (), BatchPoolState>) -> HandlerOutcome {
    ctx.state().saw_batch(items.len());
    for item in items {
        ctx.state().record(&item.tag);
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batches_with_a_worker_pool_process_everything() {
    const COUNT: usize = 20;
    /// Smaller than the run, so a batch that ignored the mount site's size would show up here.
    const BATCH: usize = 6;
    let Some(url) = kafka_url() else { return };
    create_topic(&url, "e2e-batch-pool", 4).await;

    let run = unique("run");
    let broker = connected_broker(&url).await;
    for i in 0..COUNT {
        publish(
            &broker,
            "e2e-batch-pool",
            format!(r#"{{"tag":"{run}-{i:02}"}}"#).as_bytes(),
        )
        .await;
    }
    broker.shutdown().await.expect("shutdown seeder");

    let state = BatchPoolState {
        prefix: run.clone(),
        expected: COUNT,
        seen: Arc::new(Mutex::new(Vec::new())),
        widest_batch: Arc::new(AtomicUsize::new(0)),
        done: Arc::new(Notify::new()),
    };
    let app_state = state.clone();
    let app = RustStream::new(AppInfo::new("batch-pool", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(app_state))
        .with_broker(KafkaBroker::new([url.clone()]), |b| {
            b.include(pool_batch.batch(nonzero!(6)));
        });

    let done = Arc::clone(&state.done);
    let wait = async move {
        tokio::time::timeout(WAIT, done.notified())
            .await
            .expect("all messages within timeout");
    };
    App::run_until(app, wait).await.expect("run");

    let mut seen = state.seen.lock().expect("seen mutex poisoned").clone();
    seen.sort();
    let expected: Vec<String> = (0..COUNT).map(|i| format!("{run}-{i:02}")).collect();
    assert_eq!(seen, expected, "every message must be handled exactly once");
    let widest = state.widest_batch.load(Ordering::SeqCst);
    assert!(
        widest <= BATCH,
        "a batch must never exceed the size the mount site named, got {widest}",
    );
}

#[derive(Debug, Deserialize)]
struct KeyedEvent {
    key: String,
    seq: u32,
}

/// Records per-key sequences and wakes the test once the expected total for this run arrived.
#[derive(Clone)]
struct KeyedLanesState {
    prefix: String,
    expected: usize,
    seen: Arc<Mutex<HashMap<String, Vec<u32>>>>,
    count: Arc<Mutex<usize>>,
    done: Arc<Notify>,
}

impl KeyedLanesState {
    fn record(&self, key: &str, seq: u32) {
        if !key.starts_with(&self.prefix) {
            return;
        }
        self.seen
            .lock()
            .expect("seen mutex poisoned")
            .entry(key.to_owned())
            .or_default()
            .push(seq);
        let mut count = self.count.lock().expect("count mutex poisoned");
        *count += 1;
        if *count >= self.expected {
            self.done.notify_one();
        }
    }
}

// Eight keyed lanes over a fixed topic: deliveries sharing a record key must stay ordered even
// though up to eight messages process concurrently.
#[subscriber(
    KafkaTopic::new("e2e-keyed-lanes")
        .group("e2e-keyed-lanes-group")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
        .lane_key(LaneKey::RecordKey),
    workers(8, by_key)
)]
async fn keyed_lane(
    event: &KeyedEvent,
    ctx: &mut Context<'_, (), KeyedLanesState>,
) -> HandlerOutcome {
    ctx.state().record(&event.key, event.seq);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_worker_lanes_preserve_per_key_order() {
    const KEYS: usize = 3;
    const PER_KEY: u32 = 6;
    let Some(url) = kafka_url() else { return };
    create_topic(&url, "e2e-keyed-lanes", 4).await;

    let run = unique("run");
    let broker = connected_broker(&url).await;
    for seq in 0..PER_KEY {
        for k in 0..KEYS {
            let key = format!("{run}-k{k}");
            let mut headers = HeaderMap::new();
            headers.insert(PARTITION_KEY_HEADER, key.clone());
            broker
                .publisher(KafkaPublish::default())
                .publish(
                    OutgoingMessage::new(
                        "e2e-keyed-lanes",
                        format!(r#"{{"key":"{key}","seq":{seq}}}"#).as_bytes(),
                    )
                    .with_headers(headers),
                    None,
                )
                .await
                .expect("publish");
        }
    }
    broker.shutdown().await.expect("shutdown seeder");

    let state = KeyedLanesState {
        prefix: run.clone(),
        expected: KEYS * PER_KEY as usize,
        seen: Arc::new(Mutex::new(HashMap::new())),
        count: Arc::new(Mutex::new(0)),
        done: Arc::new(Notify::new()),
    };
    let app_state = state.clone();
    let app = RustStream::new(AppInfo::new("keyed-lanes", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(app_state))
        .with_broker(KafkaBroker::new([url.clone()]), |b| {
            b.include(keyed_lane);
        });

    let done = Arc::clone(&state.done);
    let wait = async move {
        tokio::time::timeout(WAIT, done.notified())
            .await
            .expect("all messages within timeout");
    };
    App::run_until(app, wait).await.expect("run");

    let seen = state.seen.lock().expect("seen mutex poisoned").clone();
    assert_eq!(seen.len(), KEYS, "every key must be seen: {seen:?}");
    for (key, seqs) in seen {
        let expected: Vec<u32> = (0..PER_KEY).collect();
        assert_eq!(seqs, expected, "per-key order must be preserved for {key}");
    }
}

/// Records the global arrival order and wakes the test once the run's total arrived.
#[derive(Clone)]
struct PartitionLaneState {
    prefix: String,
    expected: usize,
    seen: Arc<Mutex<Vec<u32>>>,
    done: Arc<Notify>,
}

impl PartitionLaneState {
    fn record(&self, key: &str, seq: u32) {
        if !key.starts_with(&self.prefix) {
            return;
        }
        let mut seen = self.seen.lock().expect("seen mutex poisoned");
        seen.push(seq);
        if seen.len() >= self.expected {
            self.done.notify_one();
        }
    }
}

// Partition lanes (the default): the topic has one partition, so every delivery (whatever
// its record key) shares one lane and the global partition order must survive eight
// concurrent workers.
#[subscriber(
    KafkaTopic::new("e2e-partition-lanes")
        .group("e2e-partition-lanes-group")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked),
    workers(8, by_key)
)]
async fn partition_lane(
    event: &KeyedEvent,
    ctx: &mut Context<'_, (), PartitionLaneState>,
) -> HandlerOutcome {
    ctx.state().record(&event.key, event.seq);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_lanes_preserve_partition_order_across_keys() {
    const COUNT: u32 = 18;
    let Some(url) = kafka_url() else { return };
    create_topic(&url, "e2e-partition-lanes", 1).await;

    let run = unique("run");
    let broker = connected_broker(&url).await;
    for seq in 0..COUNT {
        // Alternating record keys: under record-key lanes these could interleave, but the
        // partition lane must keep the single partition's global order.
        let key = format!("{run}-{}", if seq % 2 == 0 { "even" } else { "odd" });
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, key.clone());
        broker
            .publisher(KafkaPublish::default())
            .publish(
                OutgoingMessage::new(
                    "e2e-partition-lanes",
                    format!(r#"{{"key":"{key}","seq":{seq}}}"#).as_bytes(),
                )
                .with_headers(headers),
                None,
            )
            .await
            .expect("publish");
    }
    broker.shutdown().await.expect("shutdown seeder");

    let state = PartitionLaneState {
        prefix: run.clone(),
        expected: COUNT as usize,
        seen: Arc::new(Mutex::new(Vec::new())),
        done: Arc::new(Notify::new()),
    };
    let app_state = state.clone();
    let app = RustStream::new(AppInfo::new("partition-lanes", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(app_state))
        .with_broker(KafkaBroker::new([url.clone()]), |b| {
            b.include(partition_lane);
        });

    let done = Arc::clone(&state.done);
    let wait = async move {
        tokio::time::timeout(WAIT, done.notified())
            .await
            .expect("all messages within timeout");
    };
    App::run_until(app, wait).await.expect("run");

    let seen = state.seen.lock().expect("seen mutex poisoned").clone();
    let expected: Vec<u32> = (0..COUNT).collect();
    assert_eq!(
        seen, expected,
        "one partition = one lane: global partition order must be preserved across keys",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_scoped_transactions_run_independently() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("txscope");
    create_topic(&url, &topic, 1).await;
    let broker = connected_broker(&url).await;

    let mut subscriber = broker
        .subscribe_with(tracked(&topic, &unique("group")))
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());

    let publishers = KafkaPublish::default()
        .transactional_id(unique("txp"))
        .per_partition()
        .pair(&broker)
        .await
        .expect("pair the per-partition set");
    let p0 = publishers.for_partition(0).await.expect("publisher p0");
    let p1 = publishers.for_partition(1).await.expect("publisher p1");

    p0.begin_transaction().await.expect("begin p0");
    // One publisher runs one transaction: a second begin is an explicit error, not a silent
    // merge of two flows into one transaction.
    let busy = p0
        .begin_transaction()
        .await
        .expect_err("begin while open must fail");
    assert!(matches!(busy, KafkaError::TransactionBusy { .. }));

    // Misuse is an error, never a silent no-op: p1 has nothing open yet.
    let idle = p1
        .commit()
        .await
        .expect_err("commit without begin must fail");
    assert!(matches!(idle, KafkaError::NoTransaction { .. }));

    // Another partition's publisher owns its own id and transacts independently.
    p1.begin_transaction().await.expect("begin p1");
    p0.publish(OutgoingMessage::new(&topic, b"from-p0".as_slice()), None)
        .await
        .expect("publish p0");
    p1.publish(OutgoingMessage::new(&topic, b"from-p1".as_slice()), None)
        .await
        .expect("publish p1");

    // Commit in the reverse order of the begins: the transactions do not entangle.
    p1.commit().await.expect("commit p1");
    p0.commit().await.expect("commit p0");

    let first = next_message(&mut stream).await;
    let second = next_message(&mut stream).await;
    let mut payloads = [first.payload().to_vec(), second.payload().to_vec()];
    payloads.sort();
    assert_eq!(payloads, [b"from-p0".to_vec(), b"from-p1".to_vec()]);
    first.ack().await.expect("ack first");
    second.ack().await.expect("ack second");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eos_pipeline_commits_offsets_with_records() {
    let Some(url) = kafka_url() else { return };
    let input = unique("eos-in");
    let output = unique("eos-out");
    create_topic(&url, &input, 1).await;
    create_topic(&url, &output, 1).await;
    let broker = connected_broker(&url).await;
    let pipeline_id = unique("eos");
    let group = unique("group");

    for payload in [b"a".as_slice(), b"b", b"c"] {
        publish(&broker, &input, payload).await;
    }

    let pipeline = KafkaEosPublish::new(&pipeline_id)
        .commit_interval(Duration::from_millis(50))
        .pair(&broker)
        .await
        .expect("pair the pipeline");
    let mut subscriber = broker
        .subscribe_with(
            KafkaTopic::new(&input)
                .group(&group)
                .start(StartOffset::Earliest)
                .commit(Commit::Transactional(pipeline_id.clone())),
        )
        .await
        .expect("subscribe input");
    {
        let mut stream = Box::pin(subscriber.stream());
        for _ in 0..3 {
            let msg = next_message(&mut stream).await;
            let source = SourceOffset::new(msg.topic(), msg.partition(), msg.offset());
            let forwarded: Vec<u8> = msg.payload().to_vec();
            pipeline
                .publish(
                    &source,
                    OutgoingMessage::new(&output, forwarded.as_slice()),
                    None,
                )
                .await
                .expect("pipeline publish");
            msg.ack().await.expect("ack");
        }
    }

    // The committed window makes the records visible to a read_committed reader.
    let mut out_subscriber = broker
        .subscribe_with(tracked(&output, &unique("reader")))
        .await
        .expect("subscribe output");
    let mut out_stream = Box::pin(out_subscriber.stream());
    let mut seen = Vec::new();
    for _ in 0..3 {
        let msg = next_message(&mut out_stream).await;
        seen.push(msg.payload().to_vec());
        msg.ack().await.expect("ack output");
    }
    seen.sort();
    assert_eq!(seen, [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

    // The offsets went into the transaction: a fresh consumer of the same group resumes
    // after the processed records instead of redelivering them.
    drop(subscriber);
    publish(&broker, &input, b"d").await;
    let mut resumed = broker
        .subscribe_with(
            KafkaTopic::new(&input)
                .group(&group)
                .commit(Commit::Transactional(pipeline_id.clone())),
        )
        .await
        .expect("resubscribe input");
    let mut resumed_stream = Box::pin(resumed.stream());
    let msg = next_message(&mut resumed_stream).await;
    assert_eq!(
        msg.payload(),
        b"d",
        "transactionally committed offsets must position the group after the window",
    );
    msg.ack().await.expect("ack resumed");

    drop(resumed_stream);
    drop(resumed);
    drop(out_stream);
    drop(out_subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eos_aborted_window_replays_without_output_duplicates() {
    let Some(url) = kafka_url() else { return };
    let input = unique("eos-abort-in");
    let output = unique("eos-abort-out");
    create_topic(&url, &input, 1).await;
    create_topic(&url, &output, 1).await;
    let broker = connected_broker(&url).await;
    let pipeline_id = unique("eos-abort");

    publish(&broker, &input, b"first").await;
    publish(&broker, &input, b"second").await;

    // A short transaction deadline keeps the stall-abort quick; the deadline also bounds
    // init/commit, so the transaction coordinator is warmed up by the earlier tests.
    let pipeline = KafkaEosPublish::new(&pipeline_id)
        .transaction_timeout(Duration::from_secs(5))
        .commit_interval(Duration::from_millis(50))
        .pair(&broker)
        .await
        .expect("pair the pipeline");
    let mut subscriber = broker
        .subscribe_with(
            KafkaTopic::new(&input)
                .group(unique("group"))
                .start(StartOffset::Earliest)
                .commit(Commit::Transactional(pipeline_id.clone())),
        )
        .await
        .expect("subscribe input");
    let mut stream = Box::pin(subscriber.stream());

    // First pass: both deliveries publish into the window, but the second one requeues, so
    // the window can never satisfy its settle condition: it aborts at the deadline and seeks
    // back. The aborted copies stay invisible to read_committed readers.
    for expected in [b"first".as_slice(), b"second"] {
        let msg = next_message(&mut stream).await;
        assert_eq!(msg.payload(), expected);
        let source = SourceOffset::new(msg.topic(), msg.partition(), msg.offset());
        let forwarded: Vec<u8> = msg.payload().to_vec();
        pipeline
            .publish(
                &source,
                OutgoingMessage::new(&output, forwarded.as_slice()),
                None,
            )
            .await
            .expect("pipeline publish (first pass)");
        if expected == b"second" {
            msg.nack(true).await.expect("requeue second");
        } else {
            msg.ack().await.expect("ack first");
        }
    }

    // Second pass: the seek-back redelivers the whole window; processing it cleanly commits.
    for expected in [b"first".as_slice(), b"second"] {
        let msg = next_message(&mut stream).await;
        assert_eq!(
            msg.payload(),
            expected,
            "aborted window must redeliver whole"
        );
        let source = SourceOffset::new(msg.topic(), msg.partition(), msg.offset());
        let forwarded: Vec<u8> = msg.payload().to_vec();
        pipeline
            .publish(
                &source,
                OutgoingMessage::new(&output, forwarded.as_slice()),
                None,
            )
            .await
            .expect("pipeline publish (second pass)");
        msg.ack().await.expect("ack");
    }

    // Exactly-once on the output: the aborted first-pass copies never became visible.
    let mut out_subscriber = broker
        .subscribe_with(tracked(&output, &unique("reader")))
        .await
        .expect("subscribe output");
    let mut out_stream = Box::pin(out_subscriber.stream());
    let mut seen = Vec::new();
    for _ in 0..2 {
        let msg = next_message(&mut out_stream).await;
        seen.push(msg.payload().to_vec());
        msg.ack().await.expect("ack output");
    }
    seen.sort();
    assert_eq!(
        seen,
        [b"first".to_vec(), b"second".to_vec()],
        "output must contain each message exactly once",
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(3), out_stream.next())
            .await
            .is_err(),
        "no duplicate output records may follow",
    );

    drop(out_stream);
    drop(out_subscriber);
    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

/// The typed per-record setting reaches the cluster: the record lands where the call site said,
/// ahead of the partitioner and the record key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_partition_setting_places_the_record() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("placed");
    create_topic(&url, &topic, 2).await;
    let broker = connected_broker(&url).await;

    let mut subscriber = broker
        .subscribe_with(tracked(&topic, &unique("group")))
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());

    broker
        .publisher(KafkaPublish::default())
        .publish(
            OutgoingMessage::new(&topic, b"placed".as_slice()),
            Some(&KafkaOptions::default().partition(1)),
        )
        .await
        .expect("publish placed");

    let msg = next_message(&mut stream).await;
    assert_eq!(msg.payload(), b"placed");
    assert_eq!(msg.partition(), 1, "the setting must place the record");
    msg.ack().await.expect("ack");

    broker
        .publisher(KafkaPublish::default())
        .publish(
            OutgoingMessage::new(&topic, b"other".as_slice()),
            Some(&KafkaOptions::default().partition(0)),
        )
        .await
        .expect("publish other");

    let msg = next_message(&mut stream).await;
    assert_eq!(msg.payload(), b"other");
    assert_eq!(msg.partition(), 0, "each record takes its own setting");
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_assignment_consumes_only_the_assigned_partition() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("assign");
    create_topic(&url, &topic, 2).await;
    let broker = connected_broker(&url).await;

    for (payload, partition) in [(b"p0".as_slice(), 0), (b"p1", 1)] {
        broker
            .publisher(KafkaPublish::default())
            .publish(
                OutgoingMessage::new(&topic, payload),
                Some(&KafkaOptions::default().partition(partition)),
            )
            .await
            .expect("publish pinned");
    }

    // A group-less reader pinned to partition 1: it must see p1 and never p0.
    let mut subscriber = broker
        .subscribe_with(KafkaPartitions::new(&topic, [1]).start(StartOffset::Earliest))
        .await
        .expect("subscribe assigned");
    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    assert_eq!(msg.payload(), b"p1");
    assert_eq!(msg.partition(), 1);
    msg.ack().await.expect("advisory ack");
    assert!(
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .is_err(),
        "the unassigned partition must stay unseen",
    );

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_assignment_commits_into_a_group_without_joining() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("assign-commit");
    create_topic(&url, &topic, 1).await;
    let broker = connected_broker(&url).await;
    let group = unique("group");

    publish(&broker, &topic, b"first").await;
    {
        let mut subscriber = broker
            .subscribe_with(
                KafkaPartitions::new(&topic, [0])
                    .group(&group)
                    .start(StartOffset::Earliest)
                    .commit(Commit::Tracked),
            )
            .await
            .expect("subscribe assigned with group");
        let mut stream = Box::pin(subscriber.stream());
        let msg = next_message(&mut stream).await;
        assert_eq!(msg.payload(), b"first");
        msg.ack().await.expect("tracked ack");
    }

    // The tracked position went into the group: a fresh assignment resuming from committed
    // offsets sees only what came after.
    publish(&broker, &topic, b"second").await;
    let mut resumed = broker
        .subscribe_with(
            KafkaPartitions::new(&topic, [0])
                .group(&group)
                .commit(Commit::Tracked),
        )
        .await
        .expect("resubscribe assigned");
    let mut stream = Box::pin(resumed.stream());
    let msg = next_message(&mut stream).await;
    assert_eq!(
        msg.payload(),
        b"second",
        "committed positions must survive across manual assignments",
    );
    msg.ack().await.expect("ack");

    drop(stream);
    drop(resumed);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_assignment_rejects_unsupported_combinations() {
    let Some(url) = kafka_url() else { return };
    let broker = connected_broker(&url).await;

    // Naming no partition at all: the reader would take nothing.
    let empty = broker
        .subscribe_with(KafkaPartitions::new("orders", []))
        .await
        .expect_err("an assignment over no partition must fail");
    assert!(matches!(empty, KafkaError::InvalidOptions(_)));

    let tracked = broker
        .subscribe_with(
            KafkaPartitions::new("orders", [0])
                .start(StartOffset::Earliest)
                .commit(Commit::Tracked),
        )
        .await
        .expect_err("tracked without a group must fail");
    assert!(matches!(tracked, KafkaError::InvalidOptions(_)));

    let committed = broker
        .subscribe_with(KafkaPartitions::new("orders", [0]))
        .await
        .expect_err("group-less committed start must fail");
    assert!(matches!(committed, KafkaError::InvalidOptions(_)));

    let transactional = broker
        .subscribe_with(
            KafkaPartitions::new("orders", [0])
                .group("g")
                .commit(Commit::Transactional("pipe".into())),
        )
        .await
        .expect_err("transactional manual assignment must fail");
    assert!(matches!(transactional, KafkaError::InvalidOptions(_)));

    broker.shutdown().await.expect("shutdown");
}

#[derive(Clone)]
struct AssignedLaneState {
    expected: usize,
    seen: Arc<Mutex<Vec<(i32, u32)>>>,
    done: Arc<Notify>,
}

#[subscriber(
    KafkaPartitions::new(std::env::var("ASSIGNED_LANES_TOPIC").expect("topic env"), [0, 1])
        .start(StartOffset::Earliest),
    workers(2, by_key)
)]
async fn assigned_lane(
    payload: &OrderPayload,
    ctx: &mut ruststream::runtime::Context<'_, (), AssignedLaneState>,
) -> HandlerOutcome {
    let state = ctx.state().clone();
    {
        let mut seen = state.seen.lock().expect("seen mutex poisoned");
        seen.push((payload.partition, payload.seq));
        if seen.len() < state.expected {
            return HandlerOutcome::ack();
        }
    }
    state.done.notify_waiters();
    HandlerOutcome::ack()
}

// The exactly-once relay republishes the payload it read, so the type declares no topic of its
// own: the reply topic stays the mount site's.
#[derive(Debug, Clone, serde::Serialize, Deserialize, Outgoing)]
struct OrderPayload {
    partition: i32,
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_assignment_composes_with_partition_lanes() {
    const PER_PARTITION: u32 = 20;

    let Some(url) = kafka_url() else { return };
    let topic = unique("assign-lanes");
    create_topic(&url, &topic, 2).await;
    // The macro source expression cannot capture locals; the topic travels via the env.
    unsafe { std::env::set_var("ASSIGNED_LANES_TOPIC", &topic) };

    let broker = connected_broker(&url).await;
    for seq in 0..PER_PARTITION {
        for partition in [0, 1] {
            let payload = format!(r#"{{"partition":{partition},"seq":{seq}}}"#);
            broker
                .publisher(KafkaPublish::default())
                .publish(
                    OutgoingMessage::new(&topic, payload.as_bytes()),
                    Some(&KafkaOptions::default().partition(partition)),
                )
                .await
                .expect("publish");
        }
    }
    broker.shutdown().await.expect("producer shutdown");

    let state = AssignedLaneState {
        expected: (PER_PARTITION * 2) as usize,
        seen: Arc::new(Mutex::new(Vec::new())),
        done: Arc::new(Notify::new()),
    };
    let app_state = state.clone();
    let app = RustStream::new(AppInfo::new("assign-lanes", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(app_state))
        .with_broker(KafkaBroker::new([url.clone()]), |b| {
            // A partition reader addresses no retry copies, so every registration over one names
            // where they would go, even a handler that never retries.
            b.include(assigned_lane)
                .out_retry(KafkaPublish::default())
                .to(topic.clone());
        });

    let done = Arc::clone(&state.done);
    let wait = async move {
        tokio::time::timeout(WAIT, done.notified())
            .await
            .expect("all messages within timeout");
    };
    App::run_until(app, wait).await.expect("run");

    // Each assigned partition must arrive in order on its lane, interleaving aside.
    let seen = state.seen.lock().expect("seen mutex poisoned").clone();
    for partition in [0, 1] {
        let sequence: Vec<u32> = seen
            .iter()
            .filter(|(p, _)| *p == partition)
            .map(|(_, seq)| *seq)
            .collect();
        let expected: Vec<u32> = (0..PER_PARTITION).collect();
        assert_eq!(
            sequence, expected,
            "partition {partition} must stay ordered on its lane",
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeking_inside_an_eos_window_replays_without_committing_past_the_target() {
    let Some(url) = kafka_url() else { return };
    let input = unique("eos-seek-in");
    let output = unique("eos-seek-out");
    create_topic(&url, &input, 1).await;
    create_topic(&url, &output, 1).await;
    let broker = connected_broker(&url).await;
    let pipeline_id = unique("eos-seek");
    let group = unique("group");

    for payload in [b"a".as_slice(), b"b", b"c"] {
        publish(&broker, &input, payload).await;
    }

    // A window long enough to still be open when the reposition lands.
    let pipeline = KafkaEosPublish::new(&pipeline_id)
        .commit_interval(Duration::from_millis(500))
        .pair(&broker)
        .await
        .expect("pair the pipeline");
    let mut subscriber = broker
        .subscribe_with(
            KafkaTopic::new(&input)
                .group(&group)
                .start(StartOffset::Earliest)
                .commit(Commit::Transactional(pipeline_id.clone())),
        )
        .await
        .expect("subscribe input");
    let seeker = subscriber.seeker();
    let mut stream = Box::pin(subscriber.stream());

    // First pass: two deliveries publish into the open window, then the handler repositions the
    // subscription back to the first of them while that window is still open.
    let mut replay_from = None;
    for expected in [b"a".as_slice(), b"b"] {
        let msg = next_message(&mut stream).await;
        assert_eq!(msg.payload(), expected);
        if replay_from.is_none() {
            replay_from = Some(msg.position());
        }
        forward(&pipeline, &msg, &output).await;
        msg.ack().await.expect("ack first pass");
    }
    seeker
        .seek(replay_from.expect("a captured position"))
        .await
        .expect("seek back inside the window");

    // The window that was open when the seek landed is discarded, so the whole input replays
    // from the sought offset and is processed into fresh windows.
    for expected in [b"a".as_slice(), b"b", b"c"] {
        let msg = next_message(&mut stream).await;
        assert_eq!(
            msg.payload(),
            expected,
            "the replay must start at the sought offset and keep the log order",
        );
        forward(&pipeline, &msg, &output).await;
        msg.ack().await.expect("ack replay");
    }

    // Exactly-once on the output: a `read_committed` reader sees each record once - the
    // discarded window's copies of "a" and "b" never became visible.
    let mut out_subscriber = broker
        .subscribe_with(tracked(&output, &unique("reader")))
        .await
        .expect("subscribe output");
    let mut out_stream = Box::pin(out_subscriber.stream());
    let mut seen = Vec::new();
    for _ in 0..3 {
        let msg = next_message(&mut out_stream).await;
        seen.push(msg.payload().to_vec());
        msg.ack().await.expect("ack output");
    }
    seen.sort();
    assert_eq!(
        seen,
        [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        "each input must be published exactly once despite the reposition",
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(3), out_stream.next())
            .await
            .is_err(),
        "the discarded window must not have made its copies visible",
    );

    // The transactional offsets followed the seek: the group resumes right after the replayed
    // range. Committing the pre-seek window would have carried it past the replay instead.
    drop(stream);
    drop(subscriber);
    // The seeker keeps the consumer (and its group membership) alive; the rejoin below needs
    // the partition free.
    drop(seeker);
    publish(&broker, &input, b"d").await;
    let mut resumed = broker
        .subscribe_with(
            KafkaTopic::new(&input)
                .group(&group)
                .commit(Commit::Transactional(pipeline_id.clone())),
        )
        .await
        .expect("resubscribe input");
    let mut resumed_stream = Box::pin(resumed.stream());
    let msg = next_message(&mut resumed_stream).await;
    assert_eq!(
        msg.payload(),
        b"d",
        "the committed offsets must cover the replayed range exactly, no more and no less",
    );
    msg.ack().await.expect("ack resumed");

    drop(resumed_stream);
    drop(resumed);
    drop(out_stream);
    drop(out_subscriber);
    broker.shutdown().await.expect("shutdown");
}

/// Publishes a delivery's payload into the pipeline's window, paired with its source offset.
async fn forward(pipeline: &EosPipeline, msg: &KafkaMessage, output: &str) {
    let source = SourceOffset::new(msg.topic(), msg.partition(), msg.offset());
    let payload = msg.payload().to_vec();
    pipeline
        .publish(
            &source,
            OutgoingMessage::new(output, payload.as_slice()),
            None,
        )
        .await
        .expect("pipeline publish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seek_moves_the_tracked_watermark_with_the_read_position() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("seek-tracked");
    let group = unique("group");
    create_topic(&url, &topic, 1).await;
    // A long auto-commit interval pins what the test is about: the only commit that can happen
    // here is the one the consumer flushes when it closes, after the seek. With the default
    // five seconds a passing run could be luck.
    let broker = KafkaBroker::new([url.clone()])
        .config("auto.commit.interval.ms", "60000")
        .connect()
        .await
        .expect("connect");

    for payload in [b"1".as_slice(), b"2", b"3"] {
        publish(&broker, &topic, payload).await;
    }

    let mut subscriber = broker
        .subscribe_with(tracked(&topic, &group))
        .await
        .expect("subscribe");
    let seeker = subscriber.seeker();
    let mut stream = Box::pin(subscriber.stream());

    let first = next_message(&mut stream).await;
    let replay_from = first.position();
    first.ack().await.expect("ack first");
    let second = next_message(&mut stream).await;
    second.ack().await.expect("ack second");

    // Back to the first record. The watermark those two acks built describes a read position
    // this subscription no longer has, so it must not survive the seek.
    seeker.seek(replay_from).await.expect("seek back");
    for expected in [b"1".as_slice(), b"2"] {
        let msg = next_message(&mut stream).await;
        assert_eq!(msg.payload(), expected, "the whole suffix must replay");
        // Deliberately NOT acked: nothing after the seek was handled, so nothing after the
        // seek may be committed.
        drop(msg);
    }

    // The seeker holds the consumer alive, so it goes before the group is rejoined; otherwise
    // the old member keeps the partition and the new one waits out a rebalance.
    drop(stream);
    drop(subscriber);
    drop(seeker);

    // A fresh member of the same group must start at the seek target: had the pre-seek acks
    // still decided the committed position, the replayed records would be skipped here.
    let mut resumed = broker
        .subscribe_with(tracked(&topic, &group))
        .await
        .expect("resubscribe");
    let mut resumed_stream = Box::pin(resumed.stream());
    let msg = next_message(&mut resumed_stream).await;
    assert_eq!(
        msg.payload(),
        b"1",
        "a commit must never advance past records the seek replayed but nobody handled",
    );
    msg.ack().await.expect("ack resumed");

    drop(resumed_stream);
    drop(resumed);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn positions_reach_every_assigned_partition_and_report_bad_targets() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("seek-vocab");
    create_topic(&url, &topic, 1).await;
    let broker = connected_broker(&url).await;

    publish(&broker, &topic, b"first").await;
    publish(&broker, &topic, b"second").await;

    let mut subscriber = broker
        .subscribe_with(tracked(&topic, &unique("group")))
        .await
        .expect("subscribe");
    let seeker = subscriber.seeker();
    let mut stream = Box::pin(subscriber.stream());

    let msg = next_message(&mut stream).await;
    let published_at = msg
        .timestamp_millis()
        .expect("the broker stamps a timestamp");
    msg.ack().await.expect("ack");

    // Stream-wide: every assigned partition goes back to the start of the log.
    seeker
        .seek(KafkaPosition::earliest())
        .await
        .expect("seek earliest");
    let replayed = next_message(&mut stream).await;
    assert_eq!(replayed.payload(), b"first");
    replayed.ack().await.expect("ack replayed");

    // A timestamp resolves per partition; the first record's own timestamp resolves to it.
    seeker
        .seek(KafkaPosition::timestamp(published_at))
        .await
        .expect("seek timestamp");
    let by_time = next_message(&mut stream).await;
    assert_eq!(by_time.payload(), b"first");
    by_time.ack().await.expect("ack by_time");

    // A partition this consumer does not hold is a clear error, not a silent no-op.
    let err = seeker
        .seek(KafkaPosition::offset(7, 0))
        .await
        .expect_err("an unassigned partition must be rejected");
    assert!(matches!(err, KafkaError::InvalidOptions(_)), "got {err}");

    // Latest parks the subscription at the end of the log: only new records arrive.
    seeker
        .seek(KafkaPosition::latest())
        .await
        .expect("seek latest");
    publish(&broker, &topic, b"third").await;
    let live = next_message(&mut stream).await;
    assert_eq!(
        live.payload(),
        b"third",
        "after seeking to the end only fresh records arrive",
    );
    live.ack().await.expect("ack live");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

/// Records the runtime retry-count header of every delivery, so the test can tell the original
/// from the deferred copy.
#[derive(Clone)]
struct DeferredRetryProbe {
    seen: Arc<Mutex<Vec<Option<String>>>>,
    done: Arc<Notify>,
}

// Kafka has no native delayed redelivery, so `retry_after` runs through the runtime's
// deferred-republish fallback: the original settles, and a copy comes back through the
// scope's retry publisher after the delay with the retry count incremented.
#[subscriber(
    KafkaTopic::new(std::env::var("DEFERRED_RETRY_TOPIC").expect("topic env"))
        .group("deferred-retry-svc")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
)]
async fn deferred_retry(
    _order: &OrderPayload,
    ctx: &mut Context<'_, (), DeferredRetryProbe>,
) -> HandlerOutcome {
    let count = ctx
        .headers()
        .get_str(RUNTIME_RETRY_COUNT_HEADER)
        .map(str::to_owned);
    let probe = ctx.state().clone();
    probe
        .seen
        .lock()
        .expect("seen mutex poisoned")
        .push(count.clone());
    if count.is_none() {
        return HandlerOutcome::retry_after(Duration::from_millis(200));
    }
    probe.done.notify_one();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_after_republishes_through_the_retry_position() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("deferred-retry");
    create_topic(&url, &topic, 1).await;
    unsafe { std::env::set_var("DEFERRED_RETRY_TOPIC", &topic) };

    let broker = connected_broker(&url).await;
    publish(&broker, &topic, br#"{"partition":0,"seq":1}"#).await;
    broker.shutdown().await.expect("shutdown seeder");

    let probe = DeferredRetryProbe {
        seen: Arc::new(Mutex::new(Vec::new())),
        done: Arc::new(Notify::new()),
    };
    let app_probe = probe.clone();
    let app = RustStream::new(AppInfo::new("deferred-retry", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(app_probe))
        .with_broker(KafkaBroker::new([url.clone()]), |b| {
            // Kafka defers the copy itself, so the registration names the publisher it
            // leaves through; the policy pairs at startup like every other one.
            b.include(deferred_retry).out_retry(KafkaPublish::default());
        });

    let done = Arc::clone(&probe.done);
    let wait = async move {
        tokio::time::timeout(WAIT, done.notified())
            .await
            .expect("the deferred copy must arrive within the timeout");
    };
    App::run_until(app, wait).await.expect("run");

    let seen = probe.seen.lock().expect("seen mutex poisoned").clone();
    assert_eq!(
        seen,
        vec![None, Some("1".to_owned())],
        "the original carries no retry count and the deferred copy carries the first one",
    );
}

#[derive(Clone)]
struct CtxDiProbe {
    expected: usize,
    seen: Arc<Mutex<Vec<(i32, i64)>>>,
    done: Arc<Notify>,
}

#[derive(FromRef)]
struct CtxDiApp {
    probe: CtxDiProbe,
}

#[subscriber(
    KafkaTopic::new(std::env::var("CTX_DI_TOPIC").expect("topic env")).group("ctx-di-svc")
)]
async fn ctx_di(
    _order: &OrderPayload,
    Ctx(partition): Ctx<keys::Partition>,
    Ctx(offset): Ctx<keys::Offset>,
    State(probe): State<CtxDiProbe>,
) -> HandlerOutcome {
    {
        let mut seen = probe.seen.lock().expect("seen mutex poisoned");
        seen.push((partition, offset));
        if seen.len() < probe.expected {
            return HandlerOutcome::ack();
        }
    }
    probe.done.notify_waiters();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctx_extractors_inject_delivery_fields() {
    const COUNT: usize = 3;

    let Some(url) = kafka_url() else { return };
    let topic = unique("ctx-di");
    create_topic(&url, &topic, 1).await;
    unsafe { std::env::set_var("CTX_DI_TOPIC", &topic) };

    let broker = connected_broker(&url).await;
    for seq in 0..COUNT {
        let payload = format!(r#"{{"partition":0,"seq":{seq}}}"#);
        broker
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(&topic, payload.as_bytes()), None)
            .await
            .expect("publish");
    }
    broker.shutdown().await.expect("producer shutdown");

    let probe = CtxDiProbe {
        expected: COUNT,
        seen: Arc::new(Mutex::new(Vec::new())),
        done: Arc::new(Notify::new()),
    };
    let app_probe = probe.clone();
    let app = RustStream::new(AppInfo::new("ctx-di", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(CtxDiApp { probe: app_probe }))
        .with_broker(
            KafkaBroker::new([url.clone()]).config("auto.offset.reset", "earliest"),
            |b| {
                b.include(ctx_di);
            },
        );

    let done = Arc::clone(&probe.done);
    let wait = async move {
        tokio::time::timeout(WAIT, done.notified())
            .await
            .expect("all messages within timeout");
    };
    App::run_until(app, wait).await.expect("run");

    let seen = probe.seen.lock().expect("seen mutex poisoned").clone();
    let expected: Vec<(i32, i64)> = (0..3_i64).map(|offset| (0, offset)).collect();
    assert_eq!(
        seen, expected,
        "the extractor-injected partition and offset must match the deliveries",
    );
}

/// Publishes one record inside the lane's own transaction. Generic over the capability rather
/// than over a publisher type: an `Out` slot entry has to satisfy the bound, not merely resolve
/// the method, which is what the arena wiring must keep true.
async fn forward_through_lane<L: PartitionLanes>(
    lanes: &L,
    partition: i32,
    order: &OrderPayload,
) -> Result<(), KafkaError> {
    let publisher = lanes.for_partition(partition).await?;
    publisher.begin_transaction().await?;
    let topic = std::env::var("LANES_OUT_TOPIC").expect("out topic env");
    let payload = format!(r#"{{"partition":{},"seq":{}}}"#, order.partition, order.seq);
    if let Err(err) = publisher
        .publish(OutgoingMessage::new(&topic, payload.as_bytes()), None)
        .await
    {
        publisher.abort().await.ok();
        return Err(err);
    }
    publisher.commit().await
}

// A broker-defined capability through the `Out` arena: the handler names `PartitionLanes` and
// never the concrete `TransactionalPartitions` the `per_partition()` policy pairs into.
#[subscriber(
    KafkaTopic::new(std::env::var("LANES_IN_TOPIC").expect("topic env"))
        .group("lanes-svc")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
)]
async fn lane_forward(
    order: &OrderPayload,
    Ctx(partition): Ctx<keys::Partition>,
    Out(lanes): Out<impl PartitionLanes>,
) -> HandlerOutcome {
    if forward_through_lane(lanes, partition, order).await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lanes_slot_publishes_through_its_partition_transaction() {
    const COUNT: usize = 2;

    let Some(url) = kafka_url() else { return };
    let input = unique("lanes-in");
    let output = unique("lanes-out");
    create_topic(&url, &input, 1).await;
    create_topic(&url, &output, 1).await;
    unsafe {
        std::env::set_var("LANES_IN_TOPIC", &input);
        std::env::set_var("LANES_OUT_TOPIC", &output);
    }

    let broker = connected_broker(&url).await;
    for seq in 0..COUNT {
        let payload = format!(r#"{{"partition":0,"seq":{seq}}}"#);
        broker
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(&input, payload.as_bytes()), None)
            .await
            .expect("publish input");
    }

    let app = RustStream::new(AppInfo::new("lanes", "0.0.0")).with_broker(
        KafkaBroker::new([url.clone()]),
        |b| {
            b.include(lane_forward)
                .out(
                    DefaultSlot,
                    KafkaPublish::default()
                        .transactional_id(unique("lanes-txn"))
                        .per_partition(),
                )
                .build();
        },
    );

    // The lane's transaction commits before a `read_committed` reader sees anything, so the
    // output stream is both the signal and the assertion: the run ends when it has delivered
    // what the lanes committed, and each record is checked as it arrives.
    let mut out_subscriber = broker
        .subscribe_with(tracked(&output, &unique("reader")))
        .await
        .expect("subscribe output");
    let consume = async move {
        let mut stream = Box::pin(out_subscriber.stream());
        for seq in 0..COUNT {
            let msg = next_message(&mut stream).await;
            assert_eq!(
                msg.payload(),
                format!(r#"{{"partition":0,"seq":{seq}}}"#).as_bytes(),
                "the lane must forward its partition's records in order",
            );
            msg.ack().await.expect("ack output");
        }
    };
    App::run_until(app, consume).await.expect("run");

    broker.shutdown().await.expect("shutdown");
}

// The reposition contract a handler sees - the `Position` and `SeekHandle` context keys, and the
// batch-scoped context - is application-level behaviour, so it is exercised over the in-process
// transport with `TestApp` in `tests/testing_core.rs`. What lives here is the transport itself:
// that a real consumer moves, and that the offset bookkeeping follows it (see
// `a_seek_moves_the_tracked_watermark_with_the_read_position` and
// `positions_reach_every_assigned_partition_and_report_bad_targets` above).

// The EOS publishing-handler sugar: a bare handler returns the reply, and the pipeline's
// reply publisher pairs it with the consumed offset - no Ctx, no manual publish.
#[subscriber(
    KafkaTopic::new(std::env::var("EOS_SUGAR_TOPIC").expect("topic env"))
        .group(std::env::var("EOS_SUGAR_GROUP").expect("group env"))
        .start(StartOffset::Earliest)
        .commit(Commit::Transactional(
            std::env::var("EOS_SUGAR_PIPELINE").expect("pipeline env"),
        )),
    publish("eos-sugar-replies-placeholder")
)]
async fn eos_sugar(order: &OrderPayload) -> OrderPayload {
    order.clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eos_publishing_handler_replies_ride_the_window() {
    const COUNT: usize = 3;

    let Some(url) = kafka_url() else { return };
    let input = unique("eos-sugar-in");
    let group = unique("group");
    let pipeline_id = unique("eos-sugar");
    create_topic(&url, &input, 1).await;
    // The reply topic is a macro literal, so it is fixed across runs: recreate it, or a dead
    // previous run's open transaction pins the LSO and hides this run's replies.
    recreate_topic(&url, "eos-sugar-replies-placeholder", 1).await;
    unsafe {
        std::env::set_var("EOS_SUGAR_TOPIC", &input);
        std::env::set_var("EOS_SUGAR_GROUP", &group);
        std::env::set_var("EOS_SUGAR_PIPELINE", &pipeline_id);
    }

    let producer = connected_broker(&url).await;
    for seq in 0..COUNT {
        let payload = format!(r#"{{"partition":0,"seq":{seq}}}"#);
        producer
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(&input, payload.as_bytes()), None)
            .await
            .expect("publish input");
    }
    producer.shutdown().await.expect("producer shutdown");

    // Run the app until the replies are visible to a read_committed reader: the window must
    // have committed records and offsets atomically by then.
    // The pipeline is pure policy here; the runtime pairs it (and its reply publisher) with the
    // connected broker at startup.
    let pipeline = KafkaEosPublish::new(&pipeline_id).commit_interval(Duration::from_millis(50));
    let app = RustStream::new(AppInfo::new("eos-sugar", "0.0.0")).with_broker(
        KafkaBroker::new([url.clone()]),
        |b| {
            b.include(eos_sugar)
                .out(Reply, pipeline)
                .transform(EosReplies);
        },
    );

    let reader = connected_broker(&url).await;
    let mut out_subscriber = reader
        .subscribe_with(tracked("eos-sugar-replies-placeholder", &unique("reader")))
        .await
        .expect("subscribe replies");
    let mut out_stream = Box::pin(out_subscriber.stream());

    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let consume_replies = async move {
        for _ in 0..COUNT {
            let msg = next_message(&mut out_stream).await;
            let payload = String::from_utf8(msg.payload().to_vec()).expect("utf8");
            assert!(
                msg.headers()
                    .get(ruststream_rdkafka::EOS_SOURCE_HEADER)
                    .is_none(),
                "the source header must never reach the wire",
            );
            sink.lock().expect("seen mutex poisoned").push(payload);
            msg.ack().await.expect("ack reply");
        }
    };
    App::run_until(app, consume_replies).await.expect("run");
    let seen = seen.lock().expect("seen mutex poisoned").clone();
    for (seq, payload) in seen.iter().enumerate() {
        assert!(
            payload.contains(&format!(r#""seq":{seq}"#)),
            "reply {seq} must carry the source payload, got {payload}",
        );
    }

    // The offsets went into the transaction: the group resumes past the window.
    publish(&reader, &input, br#"{"partition":0,"seq":99}"#).await;
    let mut resumed = reader
        .subscribe_with(
            KafkaTopic::new(&input)
                .group(&group)
                .commit(Commit::Transactional(pipeline_id.clone())),
        )
        .await
        .expect("resubscribe input");
    let mut resumed_stream = Box::pin(resumed.stream());
    let msg = next_message(&mut resumed_stream).await;
    assert!(
        msg.payload().ends_with(br#""seq":99}"#),
        "transactionally committed offsets must position the group after the window",
    );
    msg.ack().await.expect("ack resumed");

    drop(resumed_stream);
    drop(resumed);
    drop(out_subscriber);
    reader.shutdown().await.expect("shutdown");
}

/// Stamps a reply with a record key, standing in for a handler that chose its own placement. It
/// writes no per-record setting, so it stays generic over them.
struct KeyStamp;

impl<K: ContextKind, Options> PublishTransform<K, Options> for KeyStamp {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut OutgoingRecord<'_>,
        _options: &mut Option<Options>,
        _cx: &K::View<'_>,
    ) {
        out.headers_mut().insert(PARTITION_KEY_HEADER, "tenant-1");
    }
}

// The round-robin cycle is a producer-side placement, and only a topic with real partitions can
// show where a record went: the in-process transport gives every topic one partition and records
// the setting instead of honouring it.
#[subscriber(
    KafkaTopic::new(std::env::var("SPREAD_IN_TOPIC").expect("topic env"))
        .group("spread-svc")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked),
    publish("round-robin-replies-placeholder")
)]
async fn spread(order: &OrderPayload) -> OrderPayload {
    order.clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_robin_walks_the_partitions_of_the_reply_topic() {
    const COUNT: u32 = 4;
    const PARTITIONS: i32 = 2;

    let Some(url) = kafka_url() else { return };
    let input = unique("spread-in");
    create_topic(&url, &input, 1).await;
    // The reply topic is a macro literal, so it is fixed across runs; recreating it gives this
    // run an empty log with the partition count the cycle is asserted against.
    recreate_topic(&url, "round-robin-replies-placeholder", PARTITIONS).await;
    assert_partition_count(&url, "round-robin-replies-placeholder", PARTITIONS);
    unsafe { std::env::set_var("SPREAD_IN_TOPIC", &input) };

    let seeder = connected_broker(&url).await;
    for seq in 0..COUNT {
        publish(
            &seeder,
            &input,
            format!(r#"{{"partition":0,"seq":{seq}}}"#).as_bytes(),
        )
        .await;
    }
    seeder.shutdown().await.expect("seeder shutdown");

    let app = RustStream::new(AppInfo::new("spread", "0.0.0")).with_broker(
        KafkaBroker::new([url.clone()]),
        |b| {
            b.include(spread)
                .out_reply(KafkaPublish::default())
                .transform(RoundRobin::partitions(PARTITIONS));
        },
    );

    let reader = connected_broker(&url).await;
    let mut out_subscriber = reader
        .subscribe_with(tracked(
            "round-robin-replies-placeholder",
            &unique("reader"),
        ))
        .await
        .expect("subscribe replies");
    let placed: Arc<Mutex<HashMap<u32, i32>>> = Arc::new(Mutex::new(HashMap::new()));
    let sink = Arc::clone(&placed);
    let consume = async move {
        let mut stream = Box::pin(out_subscriber.stream());
        for _ in 0..COUNT {
            let msg = next_message(&mut stream).await;
            let reply: OrderPayload = serde_json::from_slice(msg.payload()).expect("reply json");
            sink.lock()
                .expect("placed mutex poisoned")
                .insert(reply.seq, msg.partition());
            msg.ack().await.expect("ack reply");
        }
    };
    App::run_until(app, consume).await.expect("run");

    let placed = placed.lock().expect("placed mutex poisoned").clone();
    for seq in 0..COUNT {
        let partition = placed.get(&seq).copied().expect("every reply arrives");
        let expected = i32::try_from(seq % 2).expect("a cycle of two");
        assert_eq!(
            partition, expected,
            "the cycle must place reply {seq} on partition {expected}, not {partition}",
        );
    }

    reader.shutdown().await.expect("shutdown");
}

// The same cycle over replies that already carry a record key: keys exist for ordering, so the
// cycle must leave the placement Kafka derives from the key alone.
#[subscriber(
    KafkaTopic::new(std::env::var("KEYED_SPREAD_IN_TOPIC").expect("topic env"))
        .group("keyed-spread-svc")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked),
    publish("round-robin-keyed-replies-placeholder")
)]
async fn spread_keyed(order: &OrderPayload) -> OrderPayload {
    order.clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_robin_leaves_a_keyed_reply_where_its_key_sends_it() {
    const COUNT: u32 = 4;
    const PARTITIONS: i32 = 2;

    let Some(url) = kafka_url() else { return };
    let input = unique("keyed-spread-in");
    create_topic(&url, &input, 1).await;
    recreate_topic(&url, "round-robin-keyed-replies-placeholder", PARTITIONS).await;
    assert_partition_count(&url, "round-robin-keyed-replies-placeholder", PARTITIONS);
    unsafe { std::env::set_var("KEYED_SPREAD_IN_TOPIC", &input) };

    let seeder = connected_broker(&url).await;
    for seq in 0..COUNT {
        publish(
            &seeder,
            &input,
            format!(r#"{{"partition":0,"seq":{seq}}}"#).as_bytes(),
        )
        .await;
    }
    seeder.shutdown().await.expect("seeder shutdown");

    let app = RustStream::new(AppInfo::new("keyed-spread", "0.0.0")).with_broker(
        KafkaBroker::new([url.clone()]),
        |b| {
            // KeyStamp runs first, so every reply carries a key by the time the cycle sees it.
            b.include(spread_keyed)
                .out_reply(KafkaPublish::default())
                .transform(KeyStamp)
                .transform(RoundRobin::partitions(PARTITIONS));
        },
    );

    let reader = connected_broker(&url).await;
    let mut out_subscriber = reader
        .subscribe_with(tracked(
            "round-robin-keyed-replies-placeholder",
            &unique("reader"),
        ))
        .await
        .expect("subscribe replies");
    let placed: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&placed);
    let consume = async move {
        let mut stream = Box::pin(out_subscriber.stream());
        for _ in 0..COUNT {
            let msg = next_message(&mut stream).await;
            assert_eq!(
                msg.key(),
                Some(b"tenant-1".as_slice()),
                "the key the transform stamped must reach the record",
            );
            sink.lock()
                .expect("placed mutex poisoned")
                .push(msg.partition());
            msg.ack().await.expect("ack reply");
        }
    };
    App::run_until(app, consume).await.expect("run");

    let placed = placed.lock().expect("placed mutex poisoned").clone();
    let first = placed[0];
    assert!(
        placed.iter().all(|partition| *partition == first),
        "one key means one partition: the cycle must not spread these, got {placed:?}",
    );

    reader.shutdown().await.expect("shutdown");
}

/// Records the framework retry count of every delivery, so the cap can be read off the sequence.
#[derive(Clone)]
struct CapProbe {
    seen: Arc<Mutex<Vec<Option<String>>>>,
}

// Kafka holds no record back and counts no deliveries, so a cap is the framework's: each retry is
// a fresh copy carrying an incremented count, and the delivery that spends the cap is republished
// to the dead-letter topic instead of coming back once more.
#[subscriber(
    KafkaTopic::new(std::env::var("CAP_IN_TOPIC").expect("topic env"))
        .group("cap-svc")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
)]
async fn spend_the_cap(
    _order: &OrderPayload,
    ctx: &mut Context<'_, (), CapProbe>,
) -> HandlerOutcome {
    let count = ctx
        .headers()
        .get_str(RUNTIME_RETRY_COUNT_HEADER)
        .map(str::to_owned);
    ctx.state()
        .seen
        .lock()
        .expect("seen mutex poisoned")
        .push(count);
    HandlerOutcome::retry()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_spent_delivery_lands_on_the_dead_letter_topic() {
    let Some(url) = kafka_url() else { return };
    let input = unique("cap-in");
    let dead_letters = unique("cap-dlq");
    create_topic(&url, &input, 1).await;
    create_topic(&url, &dead_letters, 1).await;
    unsafe { std::env::set_var("CAP_IN_TOPIC", &input) };

    let seeder = connected_broker(&url).await;
    publish(&seeder, &input, br#"{"partition":0,"seq":7}"#).await;
    seeder.shutdown().await.expect("seeder shutdown");

    let probe = CapProbe {
        seen: Arc::new(Mutex::new(Vec::new())),
    };
    let app_probe = probe.clone();
    let app = RustStream::new(AppInfo::new("cap", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(app_probe))
        .with_broker(KafkaBroker::new([url.clone()]), |b| {
            b.include(spend_the_cap)
                .max_attempts(nonzero!(3u32))
                .dead_letter(dead_letters.clone());
        });

    let reader = connected_broker(&url).await;
    let mut dead = reader
        .subscribe_with(tracked(&dead_letters, &unique("reader")))
        .await
        .expect("subscribe dead letters");
    let consume = async move {
        let mut stream = Box::pin(dead.stream());
        let msg = next_message(&mut stream).await;
        assert_eq!(
            msg.payload(),
            br#"{"partition":0,"seq":7}"#,
            "the dead letter must carry the payload as it arrived",
        );
        assert_eq!(
            msg.headers().get_str(RUNTIME_RETRY_COUNT_HEADER),
            Some("3"),
            "the dead letter must carry the count that spent the cap",
        );
        msg.ack().await.expect("ack dead letter");
        assert!(
            tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .is_err(),
            "a spent delivery is dead-lettered once, not on every later attempt",
        );
    };
    App::run_until(app, consume).await.expect("run");

    let seen = probe.seen.lock().expect("seen mutex poisoned").clone();
    assert_eq!(
        seen,
        vec![None, Some("1".to_owned()), Some("2".to_owned())],
        "the cap must allow exactly three deliveries, the first included",
    );
}

/// Records the framework retry count of every delivery and wakes the test once the copy arrives.
#[derive(Clone)]
struct SourceTopicProbe {
    seen: Arc<Mutex<Vec<Option<String>>>>,
    done: Arc<Notify>,
}

// A `KafkaTopics` subscription reads a set no single publish addresses, so the registration says
// where a retry copy goes. `ToSourceTopic` answers it per delivery: back to the topic this one
// arrived on.
#[subscriber(
    KafkaTopics::new([
        std::env::var("SRC_EU_TOPIC").expect("topic env"),
        std::env::var("SRC_US_TOPIC").expect("topic env"),
    ])
    .group("src-topic-svc")
    .start(StartOffset::Earliest)
    .commit(Commit::Tracked)
)]
async fn regional(
    _order: &OrderPayload,
    ctx: &mut Context<'_, KafkaContext, SourceTopicProbe>,
) -> HandlerOutcome {
    let count = ctx
        .headers()
        .get_str(RUNTIME_RETRY_COUNT_HEADER)
        .map(str::to_owned);
    let probe = ctx.state().clone();
    probe
        .seen
        .lock()
        .expect("seen mutex poisoned")
        .push(count.clone());
    if count.is_none() {
        return HandlerOutcome::retry();
    }
    probe.done.notify_one();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_copy_returns_to_the_topic_its_delivery_arrived_on() {
    let Some(url) = kafka_url() else { return };
    let eu = unique("orders-eu");
    let us = unique("orders-us");
    create_topic(&url, &eu, 1).await;
    create_topic(&url, &us, 1).await;
    unsafe {
        std::env::set_var("SRC_EU_TOPIC", &eu);
        std::env::set_var("SRC_US_TOPIC", &us);
    }

    let seeder = connected_broker(&url).await;
    publish(&seeder, &eu, br#"{"partition":0,"seq":4}"#).await;
    seeder.shutdown().await.expect("seeder shutdown");

    let probe = SourceTopicProbe {
        seen: Arc::new(Mutex::new(Vec::new())),
        done: Arc::new(Notify::new()),
    };
    let app_probe = probe.clone();
    let app = RustStream::new(AppInfo::new("regional", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(app_probe))
        .with_broker(KafkaBroker::new([url.clone()]), |b| {
            b.include(regional)
                .max_attempts(nonzero!(3u32))
                .out_retry(KafkaPublish::default())
                .transform(ToSourceTopic);
        });

    let done = Arc::clone(&probe.done);
    let wait = async move {
        tokio::time::timeout(WAIT, done.notified())
            .await
            .expect("the copy must come back within the timeout");
    };
    App::run_until(app, wait).await.expect("run");

    assert_eq!(
        probe.seen.lock().expect("seen mutex poisoned").clone(),
        vec![None, Some("1".to_owned())],
        "the original carries no count and the copy carries the first one",
    );

    // What the cluster holds: the copy is on the topic the delivery came from, and the other
    // topic of the set never saw it.
    let reader = connected_broker(&url).await;
    let mut eu_reader = reader
        .subscribe_with(tracked(&eu, &unique("reader")))
        .await
        .expect("subscribe eu");
    {
        let mut stream = Box::pin(eu_reader.stream());
        let original = next_message(&mut stream).await;
        assert_eq!(
            original.headers().get_str(RUNTIME_RETRY_COUNT_HEADER),
            None,
            "the seeded record carries no count",
        );
        original.ack().await.expect("ack original");
        let copy = next_message(&mut stream).await;
        assert_eq!(copy.topic(), eu, "the copy belongs on its own topic");
        assert_eq!(
            copy.headers().get_str(RUNTIME_RETRY_COUNT_HEADER),
            Some("1"),
            "the copy carries the incremented count",
        );
        assert_eq!(copy.payload(), br#"{"partition":0,"seq":4}"#);
        copy.ack().await.expect("ack copy");
    }
    drop(eu_reader);

    let mut us_reader = reader
        .subscribe_with(tracked(&us, &unique("reader")))
        .await
        .expect("subscribe us");
    {
        let mut stream = Box::pin(us_reader.stream());
        assert!(
            tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .is_err(),
            "a copy must not cross to the other topic of the set",
        );
    }
    drop(us_reader);
    reader.shutdown().await.expect("shutdown");
}

// The same registration without a named retry destination: a set of topics addresses no copy, so
// the app must refuse to start rather than lose the copies at run time.
#[subscriber(
    KafkaTopics::new([
        std::env::var("UNADDRESSED_EU_TOPIC").expect("topic env"),
        std::env::var("UNADDRESSED_US_TOPIC").expect("topic env"),
    ])
    .group("unaddressed-svc")
    .start(StartOffset::Earliest)
    .commit(Commit::Tracked)
)]
async fn unaddressed(_order: &OrderPayload) -> HandlerOutcome {
    HandlerOutcome::retry()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_capped_topic_set_refuses_to_start_without_a_retry_destination() {
    let Some(url) = kafka_url() else { return };
    let eu = unique("unaddressed-eu");
    let us = unique("unaddressed-us");
    create_topic(&url, &eu, 1).await;
    create_topic(&url, &us, 1).await;
    unsafe {
        std::env::set_var("UNADDRESSED_EU_TOPIC", &eu);
        std::env::set_var("UNADDRESSED_US_TOPIC", &us);
    }

    let app = RustStream::new(AppInfo::new("unaddressed", "0.0.0")).with_broker(
        KafkaBroker::new([url.clone()]),
        |b| {
            b.include(unaddressed)
                .max_attempts(nonzero!(3u32))
                .out_retry(KafkaPublish::default());
        },
    );

    // The startup error comes back before the until-future is ever polled, so a ready one keeps
    // the test bounded: an app that wrongly started would return `Ok` at once.
    let err = App::run_until(app, std::future::ready(()))
        .await
        .expect_err("a registration that cannot address its copies must not start");
    let message = err.to_string();
    assert!(
        message.contains("retry"),
        "the startup error must name the retry destination, got: {message}",
    );
}

/// The group's committed offset for one partition, read from the cluster rather than inferred
/// from what a consumer of ours delivered.
fn committed_offset(url: &str, group: &str, topic: &str, partition: i32) -> Option<i64> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", url)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .expect("probe consumer");
    let mut wanted = TopicPartitionList::new();
    wanted
        .add_partition_offset(topic, partition, Offset::Invalid)
        .expect("partition list");
    let committed = consumer
        .committed_offsets(wanted, Duration::from_secs(10))
        .expect("committed offsets");
    match committed.elements()[0].offset() {
        Offset::Offset(offset) => Some(offset),
        _ => None,
    }
}

/// Collects the sequence numbers a run handled and wakes the test once it has them all.
#[derive(Clone)]
struct ReplayProbe {
    expected: usize,
    seen: Arc<Mutex<Vec<u32>>>,
    done: Arc<Notify>,
}

impl ReplayProbe {
    fn new(expected: usize) -> Self {
        Self {
            expected,
            seen: Arc::new(Mutex::new(Vec::new())),
            done: Arc::new(Notify::new()),
        }
    }

    fn record(&self, seq: u32) {
        let mut seen = self.seen.lock().expect("seen mutex poisoned");
        seen.push(seq);
        if seen.len() >= self.expected {
            self.done.notify_waiters();
        }
    }

    fn taken(&self) -> Vec<u32> {
        self.seen.lock().expect("seen mutex poisoned").clone()
    }
}

// The first pass: an ordinary subscription, which leaves its position in the group.
#[subscriber(
    KafkaTopic::new(std::env::var("REPLAY_TOPIC").expect("topic env"))
        .group(std::env::var("REPLAY_GROUP").expect("group env"))
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
)]
async fn consume_once(
    order: &OrderPayload,
    ctx: &mut Context<'_, (), ReplayProbe>,
) -> HandlerOutcome {
    ctx.state().record(order.seq);
    HandlerOutcome::ack()
}

// The second pass over the same topic in the same group, opened at a named position instead.
#[subscriber(
    KafkaTopic::new(std::env::var("REPLAY_TOPIC").expect("topic env"))
        .group(std::env::var("REPLAY_GROUP").expect("group env"))
        .commit(Commit::Tracked),
    start_at(KafkaPosition::earliest())
)]
async fn replay_the_log(
    order: &OrderPayload,
    ctx: &mut Context<'_, (), ReplayProbe>,
) -> HandlerOutcome {
    ctx.state().record(order.seq);
    HandlerOutcome::ack()
}

/// Runs `app` until `probe` has its records, or fails the test.
async fn run_until_seen(app: impl App, probe: &ReplayProbe) {
    let done = Arc::clone(&probe.done);
    let wait = async move {
        tokio::time::timeout(WAIT, done.notified())
            .await
            .expect("every record within the timeout");
    };
    App::run_until(app, wait).await.expect("run");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_at_opens_the_subscription_ahead_of_what_the_group_committed() {
    const COUNT: u32 = 3;

    let Some(url) = kafka_url() else { return };
    let topic = unique("replay");
    let group = unique("replay-group");
    create_topic(&url, &topic, 1).await;
    unsafe {
        std::env::set_var("REPLAY_TOPIC", &topic);
        std::env::set_var("REPLAY_GROUP", &group);
    }

    let seeder = connected_broker(&url).await;
    for seq in 0..COUNT {
        publish(
            &seeder,
            &topic,
            format!(r#"{{"partition":0,"seq":{seq}}}"#).as_bytes(),
        )
        .await;
    }
    seeder.shutdown().await.expect("seeder shutdown");

    // First pass: the group reads the log and leaves its position on the cluster.
    let first = ReplayProbe::new(COUNT as usize);
    let state = first.clone();
    run_until_seen(
        RustStream::new(AppInfo::new("replay", "0.0.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(state))
            .with_broker(KafkaBroker::new([url.clone()]), |b| {
                b.include(consume_once);
            }),
        &first,
    )
    .await;
    assert_eq!(first.taken(), vec![0, 1, 2]);
    assert_eq!(
        committed_offset(&url, &group, &topic, 0),
        Some(i64::from(COUNT)),
        "the group must hold a position past the whole log",
    );

    // Second pass: the named starting position wins over that committed offset.
    let replayed = ReplayProbe::new(COUNT as usize);
    let state = replayed.clone();
    run_until_seen(
        RustStream::new(AppInfo::new("replay", "0.0.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(state))
            .with_broker(KafkaBroker::new([url.clone()]), |b| {
                b.include(replay_the_log);
            }),
        &replayed,
    )
    .await;
    assert_eq!(
        replayed.taken(),
        vec![0, 1, 2],
        "start_at must replay the log whatever the group committed",
    );

    // The control: the same group without the clause resumes after its committed position, so
    // nothing arrives at all.
    let resumed = ReplayProbe::new(1);
    let state = resumed.clone();
    let app = RustStream::new(AppInfo::new("replay", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(KafkaBroker::new([url.clone()]), |b| {
            b.include(consume_once);
        });
    let done = Arc::clone(&resumed.done);
    let wait = async move {
        assert!(
            tokio::time::timeout(Duration::from_secs(5), done.notified())
                .await
                .is_err(),
            "without the clause the group must resume past its committed position",
        );
    };
    App::run_until(app, wait).await.expect("run");
    assert!(resumed.taken().is_empty());
}
