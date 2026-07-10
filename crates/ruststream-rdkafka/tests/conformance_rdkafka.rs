//! Conformance: the in-process transport passes `run_suite` unconditionally; the lifecycle
//! suite runs against a real Kafka when `KAFKA_TEST_URL` is set (see `docker-compose.test.yml`
//! and `just test-brokers`).

#![cfg(feature = "testing")]

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use ruststream::conformance::harness;
use ruststream_rdkafka::testing::KafkaTestBroker;
use ruststream_rdkafka::{Commit, KafkaBroker, KafkaTopic, StartOffset};

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

// The harness takes higher-ranked closures that method paths cannot satisfy.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_lifecycle() {
    let Some(url) = kafka_url() else { return };
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
        |broker| broker.publisher(),
    )
    .await;
}
