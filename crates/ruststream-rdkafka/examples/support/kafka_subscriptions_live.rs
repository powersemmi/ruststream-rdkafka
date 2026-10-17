//! What the live tests of the subscription and position examples share: the gate on the stand,
//! a topic of the run's own, and a group no earlier run committed into.
//!
//! A live test skips when `KAFKA_TEST_URL` is unset, so `cargo test` without a cluster passes.
//! `RUSTSTREAM_REQUIRE_LIVE` turns that skip into a failure, for a run that stood a cluster up.

// Each example takes the helpers its own scenario needs.
#![allow(dead_code)]

use std::process;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;

/// How long a live start may take: a group forms on the stand in seconds.
pub(crate) const START: Duration = Duration::from_secs(30);

/// The stand's address, or `None` to skip the test.
///
/// # Panics
///
/// Panics when `RUSTSTREAM_REQUIRE_LIVE` is set and `KAFKA_TEST_URL` is not: the run stood a
/// cluster up, and a test that skipped past it would report a pass it never earned.
pub(crate) fn url() -> Option<String> {
    match std::env::var("KAFKA_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            let required = std::env::var("RUSTSTREAM_REQUIRE_LIVE").is_ok_and(|v| !v.is_empty());
            assert!(
                !required,
                "RUSTSTREAM_REQUIRE_LIVE is set, so this test must run, but KAFKA_TEST_URL is unset",
            );
            None
        }
    }
}

/// A consumer group no earlier run has committed into.
pub(crate) fn group(base: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    format!("{base}-{}-{nanos}", process::id())
}

/// The wall clock in epoch milliseconds, the unit a record timestamp is in.
pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Deletes `topic` and creates it again with `partitions` partitions, so the run reads only
/// what it publishes.
///
/// # Panics
///
/// Panics when the stand refuses the admin calls or the topic outlives its delete request.
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
