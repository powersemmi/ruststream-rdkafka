//! Schema Registry client tests against a mock HTTP registry: caching (one network hit per
//! id/subject), registration, warm-only resolution, and authentication headers. The live
//! tests at the bottom drive the full middleware round trip against a real registry and
//! Kafka.

#![cfg(feature = "schema-registry")]

use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber, subscriber,
};
use ruststream_rdkafka::schema_registry::{JsonSchema, parse_envelope};
use ruststream_rdkafka::{
    ConnectedKafkaBroker, KafkaBroker, KafkaError, KafkaPublish, KafkaTopic, SchemaFrame,
    SchemaRegistry, SchemaType, StartOffset,
};
use serde::{Deserialize, Serialize};
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

/// The fixed reply topic of the live framing test: the macro's `publish(..)` takes a string
/// literal, so runs share it and pick their own messages out by a unique marker id.
const FRAMED_TOPIC: &str = "sr-json-frames-placeholder";

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct SrOrder {
    id: i64,
}

// The producing side of the live test: a plain publishing handler; the app's `SchemaFrame`
// publish layer frames its replies for the wire.
#[subscriber(
    KafkaTopic::new(std::env::var("SR_JSON_TRIGGER").expect("trigger env"))
        .group(std::env::var("SR_JSON_GROUP").expect("group env"))
        .start(StartOffset::Earliest),
    publish("sr-json-frames-placeholder")
)]
async fn relay(order: &SrOrder) -> SrOrder {
    order.clone()
}

/// Scans `topic` from the earliest offset until `pick` accepts a payload, returning it.
async fn scan_topic(
    broker: &ConnectedKafkaBroker,
    topic: &str,
    pick: impl Fn(&[u8]) -> bool + Send + Sync,
) -> Vec<u8> {
    use futures::StreamExt;

    let mut subscriber = broker
        .subscribe_with(
            KafkaTopic::new(topic)
                .group(unique("scan"))
                .start(StartOffset::Earliest),
        )
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());
    let found = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let msg = stream
                .next()
                .await
                .expect("stream has next")
                .expect("delivery ok");
            let payload = IncomingMessage::payload(&msg).to_vec();
            msg.ack().await.expect("ack");
            if pick(&payload) {
                return payload;
            }
        }
    })
    .await
    .expect("marker message within the timeout");
    drop(stream);
    found
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_json_frame_and_transcode_end_to_end() {
    let Some(registry) = registry_url() else {
        return;
    };
    let Some(kafka) = std::env::var("KAFKA_TEST_URL").ok() else {
        return;
    };
    let trigger = unique("sr-json-trigger");
    unsafe {
        std::env::set_var("SR_JSON_TRIGGER", &trigger);
        std::env::set_var("SR_JSON_GROUP", unique("sr-json-group"));
    }
    let marker = i64::from(std::process::id()) * 1000 + 7;

    // The reply subject exists in the registry; the app's SchemaFrame resolves it lazily
    // (through its own cold client) on the first publish - no startup ceremony.
    let sr = SchemaRegistry::new(&registry);
    let id = sr
        .register(
            &format!("{FRAMED_TOPIC}-value"),
            SchemaType::Json,
            r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#,
        )
        .await
        .expect("register");

    // Seed the trigger before the app starts (its handler reads from the earliest offset).
    let seed_broker = KafkaBroker::new([kafka.clone()])
        .connect()
        .await
        .expect("connect seed");
    seed_broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(
            &trigger,
            format!(r#"{{"id":{marker}}}"#).as_bytes(),
        ))
        .await
        .expect("seed trigger");
    seed_broker.shutdown().await.expect("seed shutdown");

    let app = RustStream::new(AppInfo::new("sr-json", "0.0.0"))
        .publish_layer(SchemaFrame::new(SchemaRegistry::new(&registry)))
        .with_broker(KafkaBroker::new([kafka.clone()]), |b| {
            b.include(relay).publisher(KafkaPublish::default());
        });

    let registry_for_wait = registry.clone();
    let wait = async move {
        // A registry-less consumer sees the raw wire: the reply must carry the envelope.
        let raw_broker = KafkaBroker::new([kafka.clone()])
            .connect()
            .await
            .expect("connect raw");
        let framed = scan_topic(&raw_broker, FRAMED_TOPIC, |payload| {
            parse_envelope(payload).is_some_and(|(_, datum)| {
                serde_json::from_slice::<SrOrder>(datum).is_ok_and(|order| order.id == marker)
            })
        })
        .await;
        let (wire_id, _) = parse_envelope(&framed).expect("framed");
        assert_eq!(
            wire_id, id,
            "the envelope must carry the subject's schema id"
        );
        raw_broker.shutdown().await.expect("raw shutdown");

        // A transcoding consumer (cold client) sees plain JSON and warms its cache.
        let consumer_sr = SchemaRegistry::new(&registry_for_wait);
        let transcoding_broker = KafkaBroker::new([kafka])
            .schema_registry(consumer_sr.clone())
            .connect()
            .await
            .expect("connect transcoding");
        let plain = scan_topic(&transcoding_broker, FRAMED_TOPIC, |payload| {
            serde_json::from_slice::<SrOrder>(payload).is_ok_and(|order| order.id == marker)
        })
        .await;
        assert!(
            parse_envelope(&plain).is_none(),
            "the envelope must be stripped before the delivery reaches handlers",
        );
        assert!(
            consumer_sr.cached_schema(id).is_some(),
            "the middleware fetched the schema into the cold client's cache",
        );
        transcoding_broker
            .shutdown()
            .await
            .expect("transcoding shutdown");
    };
    App::run_until(app, wait).await.expect("run");
}
