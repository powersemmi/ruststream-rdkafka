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

use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use ruststream::{Broker, Headers, IncomingMessage, OutgoingMessage, Publisher, Subscriber};
use ruststream_rdkafka::{
    Commit, KafkaBroker, KafkaError, KafkaMessage, KafkaTopic, PARTITION_KEY_HEADER, StartOffset,
};

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

async fn connected_broker(url: &str) -> KafkaBroker {
    let broker = KafkaBroker::new([url.to_owned()]);
    Broker::connect(&broker).await.expect("connect");
    broker
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

async fn publish(broker: &KafkaBroker, topic: &str, payload: &[u8]) {
    broker
        .publisher()
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
        .subscribe(tracked(&topic, &unique("group")))
        .await
        .expect("subscribe");

    let mut headers = Headers::new();
    headers.insert("content-type", "application/json");
    headers.insert(PARTITION_KEY_HEADER, "order-1");
    broker
        .publisher()
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
    // The native record key comes back as the partition-key header and as partition_key().
    assert_eq!(msg.key(), Some(b"order-1".as_slice()));
    assert_eq!(
        IncomingMessage::partition_key(&msg),
        Some(b"order-1".as_slice())
    );
    assert_eq!(msg.topic(), topic);
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    Broker::shutdown(&broker).await.expect("shutdown");
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
            .subscribe(tracked(&topic, &group))
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
        .subscribe(tracked(&topic, &group))
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
    Broker::shutdown(&broker).await.expect("shutdown");
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
            .subscribe(tracked(&topic, &group))
            .await
            .expect("subscribe");
        let mut stream = Box::pin(subscriber.stream());
        let msg = next_message(&mut stream).await;
        assert_eq!(msg.payload(), b"poison");
        msg.nack(true).await.expect("nack requeue");
    }

    let mut subscriber = broker
        .subscribe(tracked(&topic, &group))
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
    Broker::shutdown(&broker).await.expect("shutdown");
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
            .subscribe(tracked(&topic, &group))
            .await
            .expect("subscribe");
        let mut stream = Box::pin(subscriber.stream());
        let msg = next_message(&mut stream).await;
        assert_eq!(msg.payload(), b"skip-me");
        msg.nack(false).await.expect("nack drop");
    }

    publish(&broker, &topic, b"next").await;

    let mut subscriber = broker
        .subscribe(tracked(&topic, &group))
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
    Broker::shutdown(&broker).await.expect("shutdown");
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
            .subscribe(tracked(&topic, &group))
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
        .subscribe(tracked(&topic, &group))
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
    Broker::shutdown(&broker).await.expect("shutdown");
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
    let mut subscriber = broker.subscribe(def).await.expect("subscribe");

    publish(&broker, &topic, b"auto-1").await;

    let mut stream = Box::pin(subscriber.stream());
    let msg = next_message(&mut stream).await;
    assert_eq!(msg.payload(), b"auto-1");
    msg.ack().await.expect("advisory ack always succeeds");

    drop(stream);
    drop(subscriber);
    Broker::shutdown(&broker).await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_key_lands_on_one_partition() {
    const COUNT: usize = 8;
    let Some(url) = kafka_url() else { return };
    let topic = unique("keyed");
    create_topic(&url, &topic, 4).await;
    let broker = connected_broker(&url).await;

    let mut subscriber = broker
        .subscribe(tracked(&topic, &unique("group")))
        .await
        .expect("subscribe");

    for i in 0..COUNT {
        let mut headers = Headers::new();
        headers.insert(PARTITION_KEY_HEADER, "same-key");
        broker
            .publisher()
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
    Broker::shutdown(&broker).await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_name_subscribe_uses_the_default_group() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("bare");
    create_topic(&url, &topic, 1).await;

    let broker = KafkaBroker::new([url.clone()]).default_group(unique("default-group"));
    Broker::connect(&broker).await.expect("connect");

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
    Broker::shutdown(&broker).await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_group_fails_subscription_clearly() {
    let Some(url) = kafka_url() else { return };
    let broker = connected_broker(&url).await;

    let err = broker
        .subscribe(KafkaTopic::new(unique("nogroup")))
        .await
        .expect_err("subscribing without a group must fail");
    let message = err.to_string();
    assert!(
        message.contains("consumer group"),
        "the error must name the missing option, got: {message}",
    );

    Broker::shutdown(&broker).await.expect("shutdown");
}
