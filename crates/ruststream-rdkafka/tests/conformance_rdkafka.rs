//! Conformance: the in-process transport passes `run_suite` unconditionally; the lifecycle
//! and capability suites run against a real Kafka when `KAFKA_TEST_URL` is set (see
//! `docker-compose.test.yml` and `just test-brokers`).

#![cfg(feature = "testing")]

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use ruststream::conformance::{capabilities, harness};
use ruststream_rdkafka::testing::KafkaTestBroker;
use ruststream_rdkafka::{Commit, KafkaBroker, KafkaPublish, KafkaTopic, StartOffset};
use tokio::runtime::Handle;
use tokio::task;

fn kafka_url() -> Option<String> {
    std::env::var("KAFKA_TEST_URL").ok()
}

/// Creates `topic` on the cluster, accepting a topic that is already there.
async fn create_topic(url: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("admin client");
    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_test_broker_passes_conformance_suite() {
    harness::run_suite(KafkaTestBroker::new).await;
}

/// The subscription descriptor a suite subscribes through, over a topic created first.
///
/// Every suite publishes under a subject of its own, generated per run, and this factory is the
/// first place that name is known: creating the topic here puts it on the cluster before the
/// subscription opens. A consumer that subscribes to a name the cluster does not know waits for
/// a metadata refresh to notice the topic auto-created by the publish, and that wait is longer
/// than the suite's delivery deadline.
fn topic_created(url: &str, group: &str, name: &str) -> KafkaTopic {
    task::block_in_place(|| Handle::current().block_on(create_topic(url, name)));
    KafkaTopic::new(name)
        .group(group.to_owned())
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
}

/// The first consumer group on a fresh cluster makes the broker create `__consumer_offsets`
/// (50 partitions), which can take longer than the harness's short delivery deadline. A
/// throwaway group round-trip absorbs that one-time cost with a generous timeout.
async fn warm_up_group_coordinator(url: &str) {
    use futures::StreamExt as _;
    use ruststream::{Broker as _, ConnectedBroker as _, IncomingMessage as _};
    use ruststream::{OutgoingMessage, Publisher as _};
    use ruststream::{Subscriber as _, SubscriptionSource as _};

    let scratch = format!("conformance-warmup-{}", std::process::id());
    create_topic(url, &scratch).await;
    let broker = KafkaBroker::new([url.to_owned()])
        .connect()
        .await
        .expect("warm-up connect");
    let mut subscriber = KafkaTopic::new(&scratch)
        .group(&scratch)
        .start(StartOffset::Earliest)
        .subscribe(&broker)
        .await
        .expect("warm-up subscribe");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(&scratch, b"warm-up"), None)
        .await
        .expect("warm-up publish");
    let mut stream = Box::pin(subscriber.stream());
    let msg = tokio::time::timeout(std::time::Duration::from_secs(30), stream.next())
        .await
        .expect("warm-up delivery within timeout")
        .expect("warm-up stream has next")
        .expect("warm-up delivery ok");
    msg.ack().await.expect("warm-up ack");
    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("warm-up shutdown");
}

// The harness takes higher-ranked closures that method paths cannot satisfy.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_batches_capability() {
    let Some(url) = kafka_url() else { return };
    warm_up_group_coordinator(&url).await;
    let group = format!("conformance-batches-{}", std::process::id());
    capabilities::batches(
        || KafkaBroker::new([url.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}

// The harness takes higher-ranked closures that method paths cannot satisfy.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_transactions_capability() {
    let Some(url) = kafka_url() else { return };
    warm_up_group_coordinator(&url).await;
    let group = format!("conformance-tx-group-{}", std::process::id());
    let tx_id = format!("conformance-tx-{}", std::process::id());
    capabilities::transactions(
        || KafkaBroker::new([url.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| {
            // The harness factory is synchronous, while a Kafka transactional publisher does
            // real work when it comes alive (`init_transactions` fences earlier producers with
            // the same id, blocking), so the pairing is driven to completion here.
            let policy = KafkaPublish::default().transactional_id(tx_id.clone());
            task::block_in_place(|| {
                Handle::current().block_on(connected.transactional_publisher(policy))
            })
            .expect("transactional publisher must pair")
        },
    )
    .await;
}

// The harness takes higher-ranked closures that method paths cannot satisfy.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_seeking_capability() {
    let Some(url) = kafka_url() else { return };
    warm_up_group_coordinator(&url).await;
    let group = format!("conformance-seeking-{}", std::process::id());
    capabilities::seeking(
        || KafkaBroker::new([url.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}

// The harness takes higher-ranked closures that method paths cannot satisfy.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_lifecycle() {
    let Some(url) = kafka_url() else { return };
    warm_up_group_coordinator(&url).await;
    let group = format!("conformance-lifecycle-{}", std::process::id());
    harness::lifecycle(
        || KafkaBroker::new([url.clone()]),
        |name| topic_created(&url, &group, name),
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}
