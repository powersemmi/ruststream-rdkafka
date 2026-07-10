//! The publish side of keyed ordering: the partition-key header becomes Kafka's native record
//! key, so every message for one key lands on one partition, in order (consume it with the
//! `kafka_keys` example).
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_producer
//! ```

use ruststream::Broker;

// --8<-- [start:producer]
use ruststream::{Headers, OutgoingMessage, Publisher};
use ruststream_rdkafka::{KafkaBroker, KafkaError, KafkaPublisher, PARTITION_KEY_HEADER};

// The partition-key header becomes the record's native key on publish, so Kafka itself routes
// every message for one tenant to one partition (and therefore to one consumer, in order).
async fn publish_keyed(
    publisher: &KafkaPublisher,
    id: u64,
    tenant: &str,
) -> Result<(), KafkaError> {
    let mut headers = Headers::new();
    headers.insert(PARTITION_KEY_HEADER, tenant.to_owned());
    let payload = format!(r#"{{"id":{id},"tenant":"{tenant}"}}"#);
    publisher
        .publish(OutgoingMessage::new("orders", payload.as_bytes()).with_headers(headers))
        .await
}
// --8<-- [end:producer]

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), KafkaError> {
    let broker = KafkaBroker::connect(["localhost:9092"]).await?;
    let publisher = broker.publisher();

    for id in 0..8 {
        let tenant = if id % 2 == 0 { "acme" } else { "globex" };
        publish_keyed(&publisher, id, tenant).await?;
    }

    broker.shutdown().await?;
    println!("published 8 keyed orders");
    Ok(())
}
