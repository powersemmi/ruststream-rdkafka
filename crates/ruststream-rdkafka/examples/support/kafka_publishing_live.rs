//! What the live tests of the publishing examples share: the gate on the stand and the topic
//! administration a test does around the service it runs.
//!
//! A live test skips when `KAFKA_TEST_URL` is unset; `RUSTSTREAM_REQUIRE_LIVE` turns that skip
//! into a failure, so a run that stood a cluster up cannot pass without reaching it.

use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::RDKafkaErrorCode;

/// The stand's address, or `None` to skip the test.
///
/// # Panics
///
/// Panics when `RUSTSTREAM_REQUIRE_LIVE` is set and `KAFKA_TEST_URL` is not.
pub(crate) fn url() -> Option<String> {
    let required = std::env::var("RUSTSTREAM_REQUIRE_LIVE").is_ok_and(|value| !value.is_empty());
    match std::env::var("KAFKA_TEST_URL") {
        Ok(value) if !value.is_empty() => Some(value),
        _ => {
            assert!(
                !required,
                "RUSTSTREAM_REQUIRE_LIVE is set, so this test must run, but KAFKA_TEST_URL is unset",
            );
            None
        }
    }
}

/// Deletes `topic` and creates it again with `partitions` partitions, so the run reads only what
/// it publishes and the topic has the shape the test needs.
pub(crate) async fn recreate_topic(url: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("admin client");
    let _ = admin
        .delete_topics(&[topic], &AdminOptions::new())
        .await
        .expect("delete_topics call");
    // Deletion completes asynchronously; each metadata fetch waits for the cluster's answer, so
    // the loop is paced by the condition it waits for.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let metadata = admin
            .inner()
            .fetch_metadata(None, Duration::from_millis(500))
            .expect("fetch_metadata call");
        let present = metadata
            .topics()
            .iter()
            .any(|known| known.name() == topic && known.error().is_none());
        if !present {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "topic {topic} outlived its delete request"
        );
    }
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await
        .expect("create_topics call");
    match &results[0] {
        Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
        Err((name, code)) => panic!("recreating {name} failed: {code}"),
    }
}

/// How many records each partition of `topic` holds, partition 0 first.
pub(crate) fn records_per_partition(url: &str, topic: &str, partitions: i32) -> Vec<i64> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("consumer");
    (0..partitions)
        .map(|partition| {
            let (low, high) = consumer
                .fetch_watermarks(topic, partition, Duration::from_secs(5))
                .expect("fetch_watermarks call");
            high - low
        })
        .collect()
}
