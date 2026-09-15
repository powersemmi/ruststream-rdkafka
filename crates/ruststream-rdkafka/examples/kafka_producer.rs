//! The publish side of keyed ordering: the partition-key header becomes Kafka's native record
//! key, so every message for one key lands on one partition, in order (consume it with the
//! `kafka_keys` example).
//!
//! Its subject is the low-level surface itself: a hand-written `main` climbing the lifecycle
//! ladder (`new` -> `connect` -> `shutdown`) and publishing through a paired handle, with no
//! application runtime around it. A service uses `#[ruststream::app]` instead, where the
//! runtime climbs the same ladder (see the other examples).
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_producer
//! ```

use ruststream::{Broker, ConnectedBroker};
use ruststream_rdkafka::KafkaPublish;

// --8<-- [start:producer]
use ruststream::{HeaderMap, OutgoingMessage, Publisher};
use ruststream_rdkafka::{
    KafkaBroker, KafkaError, KafkaOptions, KafkaPublisher, PARTITION_KEY_HEADER,
};

// The partition-key header becomes the record's native key on publish, so Kafka itself routes
// every message for one tenant to one partition (and therefore to one consumer, in order).
async fn publish_keyed(
    publisher: &KafkaPublisher,
    id: u64,
    tenant: &str,
) -> Result<(), KafkaError> {
    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, tenant.to_owned());
    let payload = format!(r#"{{"id":{id},"tenant":"{tenant}"}}"#);
    publisher
        .publish(
            OutgoingMessage::new("orders", payload.as_bytes()).with_headers(headers),
            None,
        )
        .await
}
// --8<-- [end:producer]

// --8<-- [start:partition]
// The partition is a per-record setting, so it travels as one: the publisher targets that
// partition explicitly, winning over the partitioner and the record key. The partition must
// exist, or the publish fails. On a publish builder the same setting is the `.partition(..)`
// step.
async fn publish_pinned(
    publisher: &KafkaPublisher,
    id: u64,
    partition: i32,
) -> Result<(), KafkaError> {
    let payload = format!(r#"{{"id":{id},"tenant":"pinned"}}"#);
    publisher
        .publish(
            OutgoingMessage::new("orders", payload.as_bytes()),
            Some(&KafkaOptions::default().partition(partition)),
        )
        .await
}
// --8<-- [end:partition]

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), KafkaError> {
    // `new` is pure configuration; `connect` does the I/O and yields the connected form, the
    // only place a live publisher comes from.
    let connected = KafkaBroker::new(["localhost:9092"]).connect().await?;
    let publisher = connected.publisher(KafkaPublish::default());

    for id in 0..8 {
        let tenant = if id % 2 == 0 { "acme" } else { "globex" };
        publish_keyed(&publisher, id, tenant).await?;
    }
    publish_pinned(&publisher, 8, 0).await?;

    // `shutdown` consumes the connected form, so publishing afterwards does not compile here;
    // the closed witness carries the teardown diagnostics.
    let closed = connected.shutdown().await?;
    println!(
        "published 8 keyed orders and 1 pinned order, {} records left unflushed",
        closed.unflushed_records()
    );
    Ok(())
}
