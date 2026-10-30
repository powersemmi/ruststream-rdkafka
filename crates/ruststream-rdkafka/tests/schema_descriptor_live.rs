//! Schema sets of a pattern subscription against a real cluster and registry: the topics the
//! cluster holds when the subscription opens are checked before it opens.

#![cfg(feature = "schema-registry")]

use std::sync::atomic::{AtomicU64, Ordering};

use ruststream::{Broker, ConnectedBroker, OutgoingMessage, Publisher};
use ruststream_rdkafka::{
    KafkaBroker, KafkaPublish, KafkaTopicPattern, SchemaPrefetch, SchemaRegistry, SchemaType,
    StartOffset,
};

use crate::live;

const FOREIGN_ORDER: &str = r#"{"type":"record","name":"Order","namespace":"com.other","fields":[{"name":"id","type":"long"}]}"#;

fn unique(base: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{base}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_a_pattern_checks_the_topics_the_cluster_holds_when_it_opens() {
    let Some(registry) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
        return;
    };
    let Some(kafka) = live::url("KAFKA_TEST_URL") else {
        return;
    };
    let prefix = unique("schemaset");
    let topic = format!("{prefix}.eu");
    SchemaRegistry::new(&registry)
        .register(&format!("{topic}-value"), SchemaType::Avro, FOREIGN_ORDER)
        .await
        .expect("register the topic's subject");

    let broker = KafkaBroker::new([kafka])
        .schema_prefetch(SchemaPrefetch::new(SchemaRegistry::new(&registry)))
        .connect()
        .await
        .expect("connect");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(&topic, b"created".as_slice()), None)
        .await
        .expect("the first publish creates the topic");

    let pattern = format!("^{prefix}\\..*");
    let set = format!("^{prefix}\\.");
    let err = broker
        .subscribe_with(
            KafkaTopicPattern::new(&pattern)
                .group(unique("schemaset-group"))
                .start(StartOffset::Earliest)
                .schema(&set, "com.acme.orders"),
        )
        .await
        .expect_err("the topic's subject declares another namespace");
    let message = err.to_string();
    assert!(
        message.contains(&format!(
            "topic `{topic}` uses subject `{topic}-value` (from its name), which declares no \
             record in namespace `com.acme.orders`"
        )),
        "{message}"
    );

    let opened = broker
        .subscribe_with(
            KafkaTopicPattern::new(&pattern)
                .group(unique("schemaset-group"))
                .start(StartOffset::Earliest)
                .schema(&set, "com.other"),
        )
        .await
        .expect("the subject declares the set's namespace");
    drop(opened);
    broker.shutdown().await.expect("shutdown");
}
