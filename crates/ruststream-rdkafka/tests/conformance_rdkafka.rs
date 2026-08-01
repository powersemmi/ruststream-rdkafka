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

/// The lifecycle subject is fixed by the harness; create it up front so the first subscribe
/// does not race topic auto-creation (a missing topic surfaces as consume errors).
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

/// Drops and recreates `topic`, so a suite that asserts on exact publish order never sees a
/// previous run's messages. Deletion is asynchronous on the broker; creation is retried until
/// the name frees up.
async fn recreate_topic(url: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("admin client");
    let deleted = admin
        .delete_topics(&[topic], &AdminOptions::new())
        .await
        .expect("delete_topics call");
    for result in deleted {
        match result {
            Ok(_) | Err((_, RDKafkaErrorCode::UnknownTopicOrPartition)) => {}
            Err((name, code)) => panic!("deleting topic {name} failed: {code}"),
        }
    }
    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    for _ in 0..100 {
        let results = admin
            .create_topics([&new_topic], &AdminOptions::new())
            .await
            .expect("create_topics call");
        match results.into_iter().next().expect("one result") {
            Ok(_) => return,
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {
                // Still marked for deletion; poll the external state until the name frees up.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err((name, code)) => panic!("recreating topic {name} failed: {code}"),
        }
    }
    panic!("topic {topic} was not recreated in time");
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
        .publish(OutgoingMessage::new(&scratch, b"warm-up"))
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
    // The suite asserts exact publish order, so a previous run's messages must not survive.
    recreate_topic(&url, "conformance.batches").await;
    let group = format!("conformance-batches-{}", std::process::id());
    capabilities::batches(
        || KafkaBroker::new([url.clone()]),
        |name| {
            KafkaTopic::new(name)
                .group(group.clone())
                .start(StartOffset::Earliest)
                .commit(Commit::Tracked)
        },
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
    // The suite asserts nothing is visible before commit, so a previous run's committed
    // messages must not survive.
    recreate_topic(&url, "conformance.transactions").await;
    let group = format!("conformance-tx-group-{}", std::process::id());
    let tx_id = format!("conformance-tx-{}", std::process::id());
    capabilities::transactions(
        || KafkaBroker::new([url.clone()]),
        |name| {
            KafkaTopic::new(name)
                .group(group.clone())
                .start(StartOffset::Earliest)
                .commit(Commit::Tracked)
        },
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
async fn passes_lifecycle() {
    let Some(url) = kafka_url() else { return };
    warm_up_group_coordinator(&url).await;
    create_topic(&url, "conformance.lifecycle").await;
    // A per-run group: the lifecycle subject is fixed, and a group that already committed the
    // subject's tail would otherwise never see the fresh publish.
    let group = format!("conformance-lifecycle-{}", std::process::id());
    harness::lifecycle(
        || KafkaBroker::new([url.clone()]),
        |name| {
            KafkaTopic::new(name)
                .group(group.clone())
                .start(StartOffset::Earliest)
                .commit(Commit::Tracked)
        },
        |connected| connected.publisher(KafkaPublish::default()),
    )
    .await;
}
