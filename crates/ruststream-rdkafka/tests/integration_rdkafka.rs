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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use ruststream::runtime::{
    App, AppInfo, Ctx, HandlerOutcome, Out, RETRY_COUNT_HEADER as RUNTIME_RETRY_COUNT_HEADER,
    RustStream, State,
};
use ruststream::subscriber;
use ruststream::{
    Broker, ConnectedBroker, FromRef, HeaderMap, IncomingMessage, OutgoingMessage, Positioned,
    PublishPolicy, Publisher, Seekable, Seeker, Subscriber, TransactionalPublisher,
};
use ruststream_rdkafka::context::{KafkaBatchContext, keys};
use ruststream_rdkafka::{
    Assignment, Commit, ConnectedKafkaBroker, EosPipeline, KafkaBroker, KafkaEosPublish,
    KafkaError, KafkaMessage, KafkaPosition, KafkaPublish, KafkaTopic, LaneKey, PARTITION_HEADER,
    PARTITION_KEY_HEADER, PartitionLanes, SourceOffset, StartOffset,
};
use serde::Deserialize;
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(15);

fn kafka_url() -> Option<String> {
    std::env::var("KAFKA_TEST_URL").ok()
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
    // Deletion completes asynchronously; poll creation until the name is free again.
    for _ in 0..50 {
        let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
        let results = admin
            .create_topics([&new_topic], &AdminOptions::new())
            .await
            .expect("create_topics call");
        match &results[0] {
            Ok(_) => return,
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err((name, code)) => panic!("recreate {name}: {code}"),
        }
    }
    panic!("topic {topic} was not recreated in time");
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
        .publish(OutgoingMessage::new(topic, payload))
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
        .publish(OutgoingMessage::new(&topic, b"{}").with_headers(headers))
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
            .publish(OutgoingMessage::new(&topic, format!("k{i}").as_bytes()).with_headers(headers))
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
async fn retry_topic_republishes_with_attempt_count_and_settles() {
    use ruststream_rdkafka::{RETRY_COUNT_HEADER, Retry};

    let Some(url) = kafka_url() else { return };
    let topic = unique("retry-main");
    let retry_topic = unique("retry-hop");
    create_topic(&url, &topic, 1).await;
    create_topic(&url, &retry_topic, 1).await;
    let broker = connected_broker(&url).await;

    let group = unique("group");
    let mut main = broker
        .subscribe_with(tracked(&topic, &group).retry(Retry::Topic(retry_topic.clone())))
        .await
        .expect("subscribe main");
    let mut hop = broker
        .subscribe_with(tracked(&retry_topic, &unique("hop-group")))
        .await
        .expect("subscribe retry topic");

    publish(&broker, &topic, b"boom").await;

    let mut main_stream = Box::pin(main.stream());
    let msg = next_message(&mut main_stream).await;
    assert_eq!(msg.payload(), b"boom");
    msg.nack(true).await.expect("nack requeue");

    // The copy lands on the retry topic with the attempt counter.
    let mut hop_stream = Box::pin(hop.stream());
    let copy = next_message(&mut hop_stream).await;
    assert_eq!(copy.payload(), b"boom");
    assert_eq!(copy.headers().get_str(RETRY_COUNT_HEADER), Some("1"));
    copy.ack().await.expect("ack copy");

    // The original offset settled: the same group sees only new messages after a restart.
    drop(main_stream);
    drop(main);
    publish(&broker, &topic, b"next").await;
    let mut reopened = broker
        .subscribe_with(tracked(&topic, &group))
        .await
        .expect("re-subscribe");
    let mut stream = Box::pin(reopened.stream());
    let msg = next_message(&mut stream).await;
    assert_eq!(
        msg.payload(),
        b"next",
        "a retried message must not redeliver from the main topic",
    );
    msg.ack().await.expect("ack");

    drop(stream);
    drop(reopened);
    drop(hop_stream);
    drop(hop);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhausted_retries_dead_letter_with_source_headers() {
    use ruststream_rdkafka::{
        DLQ_SOURCE_OFFSET_HEADER, DLQ_SOURCE_TOPIC_HEADER, RETRY_COUNT_HEADER, Retry,
    };

    let Some(url) = kafka_url() else { return };
    let topic = unique("dlq-main");
    let retry_topic = unique("dlq-hop");
    let dlq = unique("dlq");
    create_topic(&url, &topic, 1).await;
    create_topic(&url, &retry_topic, 1).await;
    create_topic(&url, &dlq, 1).await;
    let broker = connected_broker(&url).await;

    let def = tracked(&topic, &unique("group"))
        .retry(Retry::Topic(retry_topic.clone()))
        .max_deliveries(2)
        .dead_letter(dlq.clone());
    let mut main = broker.subscribe_with(def).await.expect("subscribe");
    let mut dead = broker
        .subscribe_with(tracked(&dlq, &unique("dlq-reader")))
        .await
        .expect("subscribe dlq");

    // Simulate the second delivery of a message: one retry hop already behind it.
    let mut headers = HeaderMap::new();
    headers.insert(RETRY_COUNT_HEADER, "1");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(&topic, b"poison").with_headers(headers))
        .await
        .expect("publish");

    let mut main_stream = Box::pin(main.stream());
    let msg = next_message(&mut main_stream).await;
    assert_eq!(msg.payload(), b"poison");
    // Delivery number two out of max_deliveries = 2: the next retry would be delivery three,
    // so the drop path runs and the message dead-letters.
    msg.nack(true).await.expect("nack requeue");

    let mut dead_stream = Box::pin(dead.stream());
    let dead_msg = next_message(&mut dead_stream).await;
    assert_eq!(dead_msg.payload(), b"poison");
    assert_eq!(
        dead_msg.headers().get_str(DLQ_SOURCE_TOPIC_HEADER),
        Some(topic.as_str()),
    );
    assert_eq!(
        dead_msg.headers().get_str(DLQ_SOURCE_OFFSET_HEADER),
        Some("0")
    );
    dead_msg.ack().await.expect("ack dlq");

    drop(main_stream);
    drop(main);
    drop(dead_stream);
    drop(dead);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seek_back_redelivers_in_place_and_caps_deliveries() {
    use ruststream_rdkafka::Retry;

    let Some(url) = kafka_url() else { return };
    let topic = unique("seek");
    let dlq = unique("seek-dlq");
    create_topic(&url, &topic, 1).await;
    create_topic(&url, &dlq, 1).await;
    let broker = connected_broker(&url).await;

    let def = tracked(&topic, &unique("group"))
        .retry(Retry::SeekBack)
        .max_deliveries(2)
        .dead_letter(dlq.clone());
    let mut subscriber = broker.subscribe_with(def).await.expect("subscribe");
    let mut dead = broker
        .subscribe_with(tracked(&dlq, &unique("dlq-reader")))
        .await
        .expect("subscribe dlq");

    publish(&broker, &topic, b"flaky").await;

    let mut stream = Box::pin(subscriber.stream());
    // Delivery one: seek back for an immediate in-place redelivery.
    let first = next_message(&mut stream).await;
    assert_eq!(first.payload(), b"flaky");
    assert_eq!(first.offset(), 0);
    first.nack(true).await.expect("nack seeks back");

    // Delivery two (same offset): the cap of two is reached, so the drop path dead-letters.
    let second = next_message(&mut stream).await;
    assert_eq!(second.payload(), b"flaky");
    assert_eq!(
        second.offset(),
        0,
        "seek-back must redeliver the same offset"
    );
    second.nack(true).await.expect("nack over cap");

    let mut dead_stream = Box::pin(dead.stream());
    let dead_msg = next_message(&mut dead_stream).await;
    assert_eq!(dead_msg.payload(), b"flaky");
    dead_msg.ack().await.expect("ack dlq");

    // The offset settled through the drop path: publishing more resumes past it.
    publish(&broker, &topic, b"after").await;
    let msg = next_message(&mut stream).await;
    assert_eq!(msg.payload(), b"after");
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    drop(dead_stream);
    drop(dead);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_preserve_order_and_settle_per_message() {
    use ruststream::{BatchSubscriber as _, Buffered, SubscriptionSource as _};

    const COUNT: usize = 12;
    let Some(url) = kafka_url() else { return };
    let topic = unique("batches");
    create_topic(&url, &topic, 1).await;
    let broker = connected_broker(&url).await;

    for i in 0..COUNT {
        publish(&broker, &topic, format!("b{i:02}").as_bytes()).await;
    }

    let source = Buffered::new(tracked(&topic, &unique("group")))
        .max_size(NonZeroUsize::new(5).expect("non-zero cap"))
        .max_wait(Duration::from_millis(50));
    let mut subscriber = source.subscribe(&broker).await.expect("subscribe");

    let mut stream = Box::pin(subscriber.batches());
    let mut payloads = Vec::new();
    while payloads.len() < COUNT {
        let batch = tokio::time::timeout(WAIT, stream.next())
            .await
            .expect("batch within timeout")
            .expect("stream has next")
            .expect("batch ok");
        assert!(!batch.is_empty(), "a yielded batch must not be empty");
        assert!(
            batch.len() <= 5,
            "the batch cap must hold, got {}",
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

    let def = tracked(&orders, &unique("group")).and_topic(&cancels);
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

    let def = KafkaTopic::pattern(format!("^{prefix}-.*"))
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
        .subscribe_with(KafkaTopic::pattern("no-anchor").group(unique("group")))
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

/// Collects handled tags and wakes the test once the expected count for this run arrived.
#[derive(Clone)]
struct BatchPoolState {
    prefix: String,
    expected: usize,
    seen: Arc<Mutex<Vec<String>>>,
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
}

// Pages from a fixed topic, up to four in flight at once; the state filters by the run's
// prefix so reruns against a long-lived cluster stay isolated.
#[subscriber(
    // Native batches: a page is one delivery plus whatever librdkafka already fetched, and the
    // slice parameter below is what asks for one. A fixed group keeps reruns idempotent (only
    // this run's fresh publishes arrive), and the state filters by the run prefix.
    KafkaTopic::new("e2e-batch-pool")
        .group("e2e-batch-pool-group")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked),
    workers(4)
)]
async fn pool_page(items: &[Tagged], ctx: &mut Context<'_, (), BatchPoolState>) -> HandlerOutcome {
    for item in items {
        ctx.state().record(&item.tag);
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_pages_with_a_worker_pool_process_everything() {
    const COUNT: usize = 20;
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
        done: Arc::new(Notify::new()),
    };
    let app_state = state.clone();
    let app = RustStream::new(AppInfo::new("batch-pool", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(app_state))
        .with_broker(KafkaBroker::new([url.clone()]), |b| {
            b.include(pool_page);
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
    p0.publish(OutgoingMessage::new(&topic, b"from-p0".as_slice()))
        .await
        .expect("publish p0");
    p1.publish(OutgoingMessage::new(&topic, b"from-p1".as_slice()))
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
                .publish(&source, OutgoingMessage::new(&output, forwarded.as_slice()))
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
            .publish(&source, OutgoingMessage::new(&output, forwarded.as_slice()))
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
            .publish(&source, OutgoingMessage::new(&output, forwarded.as_slice()))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_partition_header_targets_the_partition() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("pinned");
    create_topic(&url, &topic, 2).await;
    let broker = connected_broker(&url).await;

    let mut subscriber = broker
        .subscribe_with(tracked(&topic, &unique("group")))
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());

    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_HEADER, "1");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(&topic, b"pinned".as_slice()).with_headers(headers))
        .await
        .expect("publish pinned");

    let msg = next_message(&mut stream).await;
    assert_eq!(msg.payload(), b"pinned");
    assert_eq!(msg.partition(), 1, "the explicit partition must win");
    assert!(
        msg.headers().get(PARTITION_HEADER).is_none(),
        "the partition header must not hit the wire",
    );
    msg.ack().await.expect("ack");

    // A malformed partition value fails the publish clearly instead of falling back.
    let mut bad = HeaderMap::new();
    bad.insert(PARTITION_HEADER, "one");
    let err = broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(&topic, b"nope".as_slice()).with_headers(bad))
        .await
        .expect_err("malformed partition must fail");
    assert!(matches!(err, KafkaError::InvalidOptions(_)));

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
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_HEADER, partition.to_string());
        broker
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(&topic, payload).with_headers(headers))
            .await
            .expect("publish pinned");
    }

    // A group-less reader pinned to partition 1: it must see p1 and never p0.
    let mut subscriber = broker
        .subscribe_with(
            KafkaTopic::new(&topic)
                .partitions([1])
                .start(StartOffset::Earliest),
        )
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
                KafkaTopic::new(&topic)
                    .partitions([0])
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
            KafkaTopic::new(&topic)
                .partitions([0])
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

    let multi = broker
        .subscribe_with(
            KafkaTopic::new("orders")
                .and_topic("cancellations")
                .partitions([0])
                .group("g"),
        )
        .await
        .expect_err("partitions with and_topic must fail");
    assert!(matches!(multi, KafkaError::InvalidOptions(_)));

    let tracked = broker
        .subscribe_with(
            KafkaTopic::new("orders")
                .partitions([0])
                .start(StartOffset::Earliest)
                .commit(Commit::Tracked),
        )
        .await
        .expect_err("tracked without a group must fail");
    assert!(matches!(tracked, KafkaError::InvalidOptions(_)));

    let committed = broker
        .subscribe_with(KafkaTopic::new("orders").partitions([0]))
        .await
        .expect_err("group-less committed start must fail");
    assert!(matches!(committed, KafkaError::InvalidOptions(_)));

    let transactional = broker
        .subscribe_with(
            KafkaTopic::new("orders")
                .partitions([0])
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
    KafkaTopic::new(std::env::var("ASSIGNED_LANES_TOPIC").expect("topic env"))
        .partitions([0, 1])
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

#[derive(Debug, Clone, serde::Serialize, Deserialize)]
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
            let mut headers = HeaderMap::new();
            headers.insert(PARTITION_HEADER, partition.to_string());
            let payload = format!(r#"{{"partition":{partition},"seq":{seq}}}"#);
            broker
                .publisher(KafkaPublish::default())
                .publish(OutgoingMessage::new(&topic, payload.as_bytes()).with_headers(headers))
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
            b.include(assigned_lane);
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
        .publish(&source, OutgoingMessage::new(output, payload.as_slice()))
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
async fn retry_after_republishes_through_the_retry_publisher() {
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
            // The early publisher is what makes this wiring possible before the connection
            // exists; `retry_via` takes a live publisher, not a policy.
            let retries = b.broker().retry_publisher();
            b.retry_via(retries);
            b.include(deferred_retry);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_early_publisher_errors_after_shutdown() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("early-publisher");
    create_topic(&url, &topic, 1).await;

    let broker = KafkaBroker::new([url.clone()]);
    let retries = broker.retry_publisher();
    let connected = broker.connect().await.expect("connect");
    retries
        .publish(OutgoingMessage::new(&topic, b"live".as_slice()))
        .await
        .expect("the cell resolves once the broker connects");

    connected.shutdown().await.expect("shutdown");

    let err = retries
        .publish(OutgoingMessage::new(&topic, b"after".as_slice()))
        .await
        .expect_err("publishing through a handle aliasing a closed connection must error");
    assert!(
        matches!(&err, KafkaError::Closed { topic: named } if named == &topic),
        "the error must name the topic it could not reach, got: {err}",
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
            .publish(OutgoingMessage::new(&topic, payload.as_bytes()))
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
        .publish(OutgoingMessage::new(&topic, payload.as_bytes()))
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
            .publish(OutgoingMessage::new(&input, payload.as_bytes()))
            .await
            .expect("publish input");
    }

    let app = RustStream::new(AppInfo::new("lanes", "0.0.0")).with_broker(
        KafkaBroker::new([url.clone()]),
        |b| {
            b.include(lane_forward).publisher(
                KafkaPublish::default()
                    .transactional_id(unique("lanes-txn"))
                    .per_partition(),
            );
        },
    );

    // The lane's transaction commits before a `read_committed` reader sees anything, so waiting
    // for the output records is the wire-effect assertion.
    let mut out_subscriber = broker
        .subscribe_with(tracked(&output, &unique("reader")))
        .await
        .expect("subscribe output");
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let consume = async move {
        let mut stream = Box::pin(out_subscriber.stream());
        for _ in 0..COUNT {
            let msg = next_message(&mut stream).await;
            let payload = String::from_utf8(msg.payload().to_vec()).expect("utf8");
            sink.lock().expect("seen mutex poisoned").push(payload);
            msg.ack().await.expect("ack output");
        }
    };
    App::run_until(app, consume).await.expect("run");

    let mut seen = seen.lock().expect("seen mutex poisoned").clone();
    seen.sort();
    let expected: Vec<String> = (0..COUNT)
        .map(|seq| format!(r#"{{"partition":0,"seq":{seq}}}"#))
        .collect();
    assert_eq!(
        seen, expected,
        "every delivery must be forwarded through its partition's transactional publisher",
    );

    broker.shutdown().await.expect("shutdown");
}

/// Collects handled sequence numbers and replays the marker record exactly once.
#[derive(Clone)]
struct SeekProbe {
    expected: usize,
    replayed: Arc<Mutex<bool>>,
    seen: Arc<Mutex<Vec<u32>>>,
    done: Arc<Notify>,
}

impl SeekProbe {
    fn new(expected: usize) -> Self {
        Self {
            expected,
            replayed: Arc::new(Mutex::new(false)),
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

    /// Whether this delivery is the one that repositions; true for the first caller only.
    fn claim_replay(&self) -> bool {
        let mut replayed = self.replayed.lock().expect("replay mutex poisoned");
        let first = !*replayed;
        *replayed = true;
        first
    }
}

// The per-delivery seek contract: `Position` reports where this record sits and `SeekHandle`
// hands out the subscription's reposition handle, both off the same context the runtime builds
// per delivery. Seeking to a delivery's own position redelivers exactly that record.
#[subscriber(
    KafkaTopic::new(std::env::var("CTX_SEEK_TOPIC").expect("topic env"))
        .group("ctx-seek-svc")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
)]
async fn ctx_seek(
    order: &OrderPayload,
    Ctx(here): Ctx<keys::Position>,
    Ctx(seeker): Ctx<keys::SeekHandle>,
    State(probe): State<SeekProbe>,
) -> HandlerOutcome {
    probe.record(order.seq);
    if order.seq == 1 && probe.claim_replay() && seeker.seek(here).await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[derive(FromRef)]
struct SeekApp {
    probe: SeekProbe,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_context_seek_handle_replays_a_delivery_position() {
    const COUNT: u32 = 3;

    let Some(url) = kafka_url() else { return };
    let topic = unique("ctx-seek");
    create_topic(&url, &topic, 1).await;
    unsafe { std::env::set_var("CTX_SEEK_TOPIC", &topic) };

    let broker = connected_broker(&url).await;
    for seq in 0..COUNT {
        let payload = format!(r#"{{"partition":0,"seq":{seq}}}"#);
        broker
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(&topic, payload.as_bytes()))
            .await
            .expect("publish");
    }
    broker.shutdown().await.expect("producer shutdown");

    // Three records plus the replayed pair behind the marker: 0, 1, then 1 and 2 again.
    let probe = SeekProbe::new(COUNT as usize + 1);
    let app_probe = probe.clone();
    let app = RustStream::new(AppInfo::new("ctx-seek", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(SeekApp { probe: app_probe }))
        .with_broker(KafkaBroker::new([url.clone()]), |b| {
            b.include(ctx_seek);
        });

    let done = Arc::clone(&probe.done);
    let wait = async move {
        tokio::time::timeout(WAIT, done.notified())
            .await
            .expect("the replayed deliveries must arrive within the timeout");
    };
    App::run_until(app, wait).await.expect("run");

    let seen = probe.seen.lock().expect("seen mutex poisoned").clone();
    assert_eq!(
        seen,
        vec![0, 1, 1, 2],
        "seeking to a delivery's own position must redeliver it and the suffix behind it",
    );
}

/// Collects the sequence numbers of every page and repositions the subscription once.
#[derive(Clone)]
struct PageSeekProbe {
    replayed: Arc<Mutex<bool>>,
    seen: Arc<Mutex<Vec<u32>>>,
    done: Arc<Notify>,
}

impl PageSeekProbe {
    fn record(&self, page: &[OrderPayload]) {
        let mut seen = self.seen.lock().expect("seen mutex poisoned");
        seen.extend(page.iter().map(|order| order.seq));
        if seen.iter().filter(|seq| **seq == 0).count() >= 2 {
            self.done.notify_waiters();
        }
    }

    fn claim_replay(&self) -> bool {
        let mut replayed = self.replayed.lock().expect("replay mutex poisoned");
        let first = !*replayed;
        *replayed = true;
        first
    }
}

// A page spans many deliveries, so it gets the subscription-scoped context instead of the
// per-delivery one: the same `SeekHandle` key, and no position (no single record to name).
#[subscriber(
    KafkaTopic::new(std::env::var("PAGE_SEEK_TOPIC").expect("topic env"))
        .group("page-seek-svc")
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
)]
async fn page_seek(
    page: &[OrderPayload],
    ctx: &mut Context<'_, KafkaBatchContext, PageSeekProbe>,
) -> HandlerOutcome {
    ctx.state().record(page);
    let replay = ctx.state().claim_replay();
    if replay
        && ctx
            .context(keys::SeekHandle)
            .seek(KafkaPosition::offset(0, 0))
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_body_repositions_through_its_subscription_context() {
    const COUNT: u32 = 2;

    let Some(url) = kafka_url() else { return };
    let topic = unique("page-seek");
    create_topic(&url, &topic, 1).await;
    unsafe { std::env::set_var("PAGE_SEEK_TOPIC", &topic) };

    let broker = connected_broker(&url).await;
    for seq in 0..COUNT {
        let payload = format!(r#"{{"partition":0,"seq":{seq}}}"#);
        broker
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(&topic, payload.as_bytes()))
            .await
            .expect("publish");
    }
    broker.shutdown().await.expect("producer shutdown");

    let probe = PageSeekProbe {
        replayed: Arc::new(Mutex::new(false)),
        seen: Arc::new(Mutex::new(Vec::new())),
        done: Arc::new(Notify::new()),
    };
    let app_probe = probe.clone();
    let app = RustStream::new(AppInfo::new("page-seek", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(app_probe))
        .with_broker(KafkaBroker::new([url.clone()]), |b| {
            b.include(page_seek);
        });

    let done = Arc::clone(&probe.done);
    let wait = async move {
        tokio::time::timeout(WAIT, done.notified())
            .await
            .expect("the replayed page must arrive within the timeout");
    };
    App::run_until(app, wait).await.expect("run");

    let seen = probe.seen.lock().expect("seen mutex poisoned").clone();
    assert!(
        seen.iter().filter(|seq| **seq == 0).count() >= 2,
        "the page's reposition must replay the log from the start, got {seen:?}",
    );
}

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
            .publish(OutgoingMessage::new(&input, payload.as_bytes()))
            .await
            .expect("publish input");
    }
    producer.shutdown().await.expect("producer shutdown");

    // Run the app until the replies are visible to a read_committed reader: the window must
    // have committed records and offsets atomically by then.
    // The pipeline is pure policy here; the runtime pairs it (and its reply publisher) with the
    // connected broker at startup.
    let replies_wiring = KafkaEosPublish::new(&pipeline_id)
        .commit_interval(Duration::from_millis(50))
        .replies();
    let app = RustStream::new(AppInfo::new("eos-sugar", "0.0.0")).with_broker(
        KafkaBroker::new([url.clone()]),
        |b| {
            b.include(eos_sugar).publisher(replies_wiring);
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
