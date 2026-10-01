//! When a subscription starts receiving, against a real Kafka: `subscribe` returns once the
//! subscription is assigned its partitions and knows where each starts, so everything published
//! after it returns reaches the subscription, under librdkafka's default `latest` reset included.
//!
//! Every test is a no-op unless `KAFKA_TEST_URL` points at a cluster (see `just test-brokers`).

use std::collections::HashSet;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::{StreamExt, stream};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_rdkafka::{
    ConnectedKafkaBroker, KafkaBroker, KafkaOptions, KafkaPartitions, KafkaPublish, KafkaTopic,
    StartOffset,
};

mod live;

/// Well inside the group's rebalance timeout (`max.poll.interval.ms`, five minutes), which is how
/// long a group waits for a member nobody polls: a stalled join fails the test at this bound.
const ASSIGNMENT: Duration = Duration::from_secs(20);

const WAIT: Duration = Duration::from_secs(15);

fn kafka_url() -> Option<String> {
    live::url("KAFKA_TEST_URL")
}

fn unique(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "{base}-{}-{}",
        process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

async fn create_topic(url: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("admin client");
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    for result in admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await
        .expect("create_topics call")
    {
        match result {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => panic!("creating topic {name} failed: {code}"),
        }
    }
}

async fn connected(url: &str) -> ConnectedKafkaBroker {
    KafkaBroker::new([url.to_owned()])
        .assignment_timeout(ASSIGNMENT)
        .connect()
        .await
        .expect("connect")
}

async fn publish_to(broker: &ConnectedKafkaBroker, topic: &str, partition: i32, payload: &str) {
    broker
        .publisher(KafkaPublish::default())
        .publish(
            OutgoingMessage::new(topic, payload.as_bytes()),
            Some(&KafkaOptions::default().partition(partition)),
        )
        .await
        .expect("publish");
}

/// Two members of a fresh group join one after the other, and neither stream is polled before
/// both have joined: the second join rebalances the group, which the first member has to answer
/// although nothing reads it yet. Both open, and each record published afterwards reaches the
/// group exactly once, with the default `latest` reset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_members_join_one_after_the_other() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("start-two-members");
    create_topic(&url, &topic, 2).await;
    let broker = connected(&url).await;
    let group = unique("start-two-members-group");

    let mut first = broker
        .subscribe_with(KafkaTopic::new(&topic).group(&group))
        .await
        .expect("the first member opens");
    let joined = Instant::now();
    let mut second = broker
        .subscribe_with(KafkaTopic::new(&topic).group(&group))
        .await
        .unwrap_or_else(|err| {
            panic!(
                "the second member must open while the first is not read yet, failed after \
                 {:?}: {err}",
                joined.elapsed(),
            )
        });

    for partition in 0..2 {
        publish_to(&broker, &topic, partition, &format!("p{partition}")).await;
    }

    let mut deliveries = Box::pin(stream::select(first.stream(), second.stream()));
    let mut seen = HashSet::new();
    while seen.len() < 2 {
        let delivery = tokio::time::timeout(WAIT, deliveries.next())
            .await
            .unwrap_or_else(|_| {
                panic!("every record published after both joined must arrive, got {seen:?}")
            })
            .expect("the streams do not end")
            .expect("delivery ok");
        let payload = String::from_utf8(delivery.payload().to_vec()).expect("utf-8 payload");
        assert!(seen.insert(payload.clone()), "{payload} arrived twice");
        delivery.ack().await.expect("ack");
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(2), deliveries.next())
            .await
            .is_err(),
        "nothing but the two records may arrive",
    );

    drop(deliveries);
    drop((first, second));
    broker.shutdown().await.expect("shutdown");
}

/// A subscription that names its partitions and starts at the end of the log receives what is
/// published once it is open: the end is where the log ends when `subscribe` returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn named_partitions_at_the_end_receive_what_follows() {
    let Some(url) = kafka_url() else { return };
    let topic = unique("start-named-partitions");
    create_topic(&url, &topic, 1).await;
    let broker = connected(&url).await;
    publish_to(&broker, &topic, 0, "before").await;

    let mut subscriber = broker
        .subscribe_with(KafkaPartitions::new(&topic, [0]).start(StartOffset::Latest))
        .await
        .expect("subscribe");
    publish_to(&broker, &topic, 0, "after").await;

    let mut stream = Box::pin(subscriber.stream());
    let delivery = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("the record published after subscribe returned must arrive")
        .expect("the stream does not end")
        .expect("delivery ok");
    assert_eq!(delivery.payload(), b"after");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}
