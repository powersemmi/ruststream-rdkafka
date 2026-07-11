//! Schema Registry client tests against a mock HTTP registry: caching (one network hit per
//! id/subject), registration, warm-only resolution, and authentication headers.

#![cfg(feature = "schema-registry")]

use ruststream_rdkafka::{KafkaError, SchemaRegistry, SchemaType};
use wiremock::matchers::{basic_auth, body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const ORDER_SCHEMA: &str =
    r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"}]}"#;

#[tokio::test]
async fn schema_by_id_caches_after_one_fetch() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/7"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "schema": ORDER_SCHEMA })),
        )
        .expect(1)
        .mount(&server)
        .await;

    let sr = SchemaRegistry::new(server.uri());
    assert!(sr.cached_schema(7).is_none());
    let first = sr.schema_by_id(7).await.expect("fetch");
    assert_eq!(first.id(), 7);
    assert_eq!(first.schema_type(), SchemaType::Avro, "untyped means Avro");
    assert_eq!(first.definition(), ORDER_SCHEMA);

    // The second lookup and the sync accessor must both hit the cache (expect(1) verifies).
    let second = sr.schema_by_id(7).await.expect("cached");
    assert_eq!(second.id(), 7);
    assert!(sr.cached_schema(7).is_some());
}

#[tokio::test]
async fn register_caches_subject_and_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/subjects/orders-value/versions"))
        .and(body_partial_json(
            serde_json::json!({ "schemaType": "AVRO" }),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "id": 42 })))
        .expect(1)
        .mount(&server)
        .await;

    let sr = SchemaRegistry::new(server.uri());
    let id = sr
        .register("orders-value", SchemaType::Avro, ORDER_SCHEMA)
        .await
        .expect("register");
    assert_eq!(id, 42);

    let cached = sr.cached_subject("orders-value").expect("subject cached");
    assert_eq!(cached.id(), 42);
    assert_eq!(cached.definition(), ORDER_SCHEMA);
    assert!(sr.cached_schema(42).is_some(), "id cache fed too");
}

#[tokio::test]
async fn warm_resolves_the_latest_version() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/subjects/orders-value/versions/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 9,
            "version": 3,
            "schema": ORDER_SCHEMA,
            "schemaType": "AVRO",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let sr = SchemaRegistry::new(server.uri());
    let schema = sr.warm("orders-value").await.expect("warm");
    assert_eq!(schema.id(), 9);
    assert_eq!(sr.cached_subject("orders-value").expect("cached").id(), 9);
}

#[tokio::test]
async fn basic_auth_rides_every_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/1"))
        .and(basic_auth("svc", "secret"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "schema": ORDER_SCHEMA })),
        )
        .expect(1)
        .mount(&server)
        .await;

    let sr = SchemaRegistry::new(server.uri()).basic_auth("svc", "secret");
    sr.schema_by_id(1).await.expect("authenticated fetch");
}

#[tokio::test]
async fn unknown_ids_and_subjects_error_clearly() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/404"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/subjects/nope-value/versions/latest"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let sr = SchemaRegistry::new(server.uri());
    let err = sr.schema_by_id(404).await.expect_err("unknown id");
    assert!(matches!(err, KafkaError::SchemaRegistry(_)));
    let err = sr.warm("nope-value").await.expect_err("unknown subject");
    assert!(matches!(err, KafkaError::SchemaRegistry(_)));
    assert!(sr.cached_schema(404).is_none(), "failures are not cached");
}

// --- live tests: a real registry (and, for the prefetch path, a real Kafka) ---

fn registry_url() -> Option<String> {
    std::env::var("SCHEMA_REGISTRY_TEST_URL").ok()
}

fn unique(base: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{base}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[tokio::test]
async fn live_registry_roundtrips_register_warm_and_fetch() {
    let Some(url) = registry_url() else { return };
    let subject = unique("ruststream-orders");

    let sr = SchemaRegistry::new(&url);
    let id = sr
        .register(&subject, SchemaType::Avro, ORDER_SCHEMA)
        .await
        .expect("register");

    // A fresh client (empty cache) must resolve both by subject and by id.
    let other = SchemaRegistry::new(&url);
    let latest = other.warm(&subject).await.expect("warm");
    assert_eq!(latest.id(), id);
    assert_eq!(latest.schema_type(), SchemaType::Avro);
    let by_id = other.schema_by_id(id).await.expect("by id");
    assert_eq!(by_id.definition(), latest.definition());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_json_transcode_end_to_end() {
    use futures::StreamExt;
    use ruststream::{Broker, IncomingMessage, OutgoingMessage, Publisher, Subscriber};
    use ruststream_rdkafka::{KafkaBroker, KafkaTopic, SchemaFormat, StartOffset};

    let Some(registry) = registry_url() else {
        return;
    };
    let Some(kafka) = std::env::var("KAFKA_TEST_URL").ok() else {
        return;
    };
    let topic = unique("sr-json");
    let subject = format!("{topic}-value");

    // The producer's subject exists in the registry; framing resolves it lazily on the
    // first publish - no startup ceremony.
    let sr = SchemaRegistry::new(&registry);
    let id = sr
        .register(
            &subject,
            SchemaType::Json,
            r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#,
        )
        .await
        .expect("register");

    let producer_broker = KafkaBroker::new([kafka.clone()]).schema_registry(sr.clone());
    Broker::connect(&producer_broker).await.expect("connect");
    let producer = producer_broker
        .publisher()
        .schema_format(SchemaFormat::Json);

    // The consumer's client starts cold: the subscriber middleware fetches the schema and
    // strips the envelope, so the delivery arrives as plain JSON.
    let consumer_sr = SchemaRegistry::new(&registry);
    let broker = KafkaBroker::new([kafka]).schema_registry(consumer_sr.clone());
    Broker::connect(&broker).await.expect("connect consumer");
    let mut subscriber = broker
        .subscribe(
            KafkaTopic::new(&topic)
                .group(unique("group"))
                .start(StartOffset::Earliest),
        )
        .await
        .expect("subscribe");

    producer
        .publish(OutgoingMessage::new(&topic, br#"{"id":7}"#.as_slice()))
        .await
        .expect("publish framed");

    let mut stream = Box::pin(subscriber.stream());
    let msg = tokio::time::timeout(std::time::Duration::from_secs(15), stream.next())
        .await
        .expect("delivery in time")
        .expect("stream has next")
        .expect("delivery ok");
    assert_eq!(
        IncomingMessage::payload(&msg),
        br#"{"id":7}"#,
        "the envelope must be stripped and the document arrive as plain JSON",
    );
    assert!(
        consumer_sr.cached_schema(id).is_some(),
        "the middleware fetched the schema into the cold client's cache",
    );
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    Broker::shutdown(&broker).await.expect("shutdown");
    Broker::shutdown(&producer_broker)
        .await
        .expect("producer shutdown");
}
