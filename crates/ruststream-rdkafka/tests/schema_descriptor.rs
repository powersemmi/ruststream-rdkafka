//! Topics that opt into schema checking with `.schema(..)`: the subjects computed for each are
//! checked against the registry while the app starts, under `TestApp` with the production app and
//! a registry served over HTTP.

#![cfg(all(feature = "testing", feature = "schema-registry"))]

use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::testing::TestApp;
use ruststream::{Outgoing, subscriber};
use ruststream_rdkafka::schema::Schema;
use ruststream_rdkafka::{
    KafkaBroker, KafkaPublish, KafkaTopic, KafkaTopicPattern, KafkaTopics, SchemaPrefetch,
    SchemaRegistry,
};
use serde::{Deserialize, Serialize};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Order {
    id: i64,
}

const ORDER: &str = r#"{"type":"record","name":"Order","namespace":"com.acme.orders","fields":[{"name":"id","type":"long"}]}"#;
const FOREIGN_ORDER: &str = r#"{"type":"record","name":"Order","namespace":"com.other","fields":[{"name":"id","type":"long"}]}"#;

/// A registry holding `subject` at its newest version with `schema`.
async fn holds(server: &MockServer, subject: &str, schema: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/subjects/{subject}/versions/latest")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subject": subject, "version": 1, "id": 1, "schema": schema,
        })))
        .mount(server)
        .await;
}

/// The broker as a service configures it: a group, and the registry it reads its subjects from.
fn broker(server: &MockServer) -> KafkaBroker {
    KafkaBroker::new(["kafka:9092"])
        .default_group("svc")
        .schema_prefetch(SchemaPrefetch::new(SchemaRegistry::new(server.uri())))
}

async fn start_error(app: RustStream) -> String {
    match TestApp::start(app).await {
        Ok(_) => panic!("the app must not start"),
        Err(err) => format!("{err:#}"),
    }
}

#[subscriber(KafkaTopic::new("orders").schema("com.acme.orders"))]
async fn on_order(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_whose_subject_declares_its_namespace_starts_and_delivers() {
    let server = MockServer::start().await;
    holds(&server, "orders-value", ORDER).await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(&server), |b| {
        b.include(on_order);
    });
    let tb = TestApp::start(app)
        .await
        .expect("the subject declares the record");
    tb.broker::<KafkaBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<KafkaBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subject_declaring_another_namespace_fails_startup_with_the_fix() {
    let server = MockServer::start().await;
    holds(&server, "orders-value", FOREIGN_ORDER).await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(&server), |b| {
        b.include(on_order);
    });
    let err = start_error(app).await;
    assert!(
        err.contains(
            "topic `orders` uses subject `orders-value` (from its name), which declares no record \
             in namespace `com.acme.orders` (its newest schema declares `com.other.Order`); add a \
             migration or name the subject with `.value(..)`"
        ),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subject_the_registry_does_not_hold_fails_startup() {
    let server = MockServer::start().await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(&server), |b| {
        b.include(on_order);
    });
    let err = start_error(app).await;
    assert!(
        err.contains(
            "topic `orders` uses subject `orders-value` (from its name), which the registry does \
             not hold"
        ),
        "{err}"
    );
}

#[subscriber(
    KafkaTopic::new("orders").schema(Schema::namespace("com.acme.orders").value("orders-v2"))
)]
async fn on_order_v2(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_override_names_the_subject_checked() {
    let server = MockServer::start().await;
    holds(&server, "orders-value", FOREIGN_ORDER).await;
    holds(&server, "orders-v2", ORDER).await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(&server), |b| {
        b.include(on_order_v2);
    });
    let tb = TestApp::start(app)
        .await
        .expect("the override declares the record");
    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mismatched_override_is_reported_as_the_descriptors_name() {
    let server = MockServer::start().await;
    holds(&server, "orders-v2", FOREIGN_ORDER).await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(&server), |b| {
        b.include(on_order_v2);
    });
    let err = start_error(app).await;
    assert!(
        err.contains("topic `orders` uses subject `orders-v2` (named by the descriptor)"),
        "{err}"
    );
}

#[subscriber(
    KafkaTopic::new("orders").schema(Schema::namespace("com.acme.orders").key("order-keys"))
)]
async fn on_keyed_order(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_named_key_subject_must_exist_and_a_default_one_may_not() {
    let server = MockServer::start().await;
    holds(&server, "orders-value", ORDER).await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(&server), |b| {
        b.include(on_order);
    });
    let tb = TestApp::start(app)
        .await
        .expect("a topic with plain keys registers no key subject");
    tb.shutdown().await.expect("shutdown");

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(&server), |b| {
        b.include(on_keyed_order);
    });
    let err = start_error(app).await;
    assert!(
        err.contains(
            "topic `orders` uses subject `order-keys` (named by the descriptor), which the \
             registry does not hold; register its migrations (`cargo kafka-schema migrate run`) \
             or name the subject with `.key(..)`"
        ),
        "{err}"
    );
}

#[subscriber("plain")]
async fn on_plain(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_without_schema_asks_the_registry_nothing() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .expect(0)
        .mount(&server)
        .await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(&server), |b| {
        b.include(on_plain);
    });
    let tb = TestApp::start(app).await.expect("start");
    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checked_topic_on_a_broker_without_a_registry_fails_startup() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        KafkaBroker::new(["kafka:9092"]).default_group("svc"),
        |b| {
            b.include(on_order);
        },
    );
    let err = start_error(app).await;
    assert!(
        err.contains("`KafkaBroker::schema_prefetch`"),
        "the error must name the step that attaches a registry, got: {err}"
    );
}

#[subscriber(
    KafkaTopics::new(["orders", "returns"])
        .schema("orders", "com.acme.orders")
        .schema("returns", Schema::namespace("com.acme").value("returns-v2"))
)]
async fn on_list(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_topic_of_a_list_is_checked_against_its_own_subjects() {
    let server = MockServer::start().await;
    holds(&server, "orders-value", ORDER).await;
    holds(
        &server,
        "returns-v2",
        r#"{"type":"record","name":"Return","namespace":"com.acme","fields":[]}"#,
    )
    .await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(&server), |b| {
        b.include(on_list)
            .out_retry(KafkaPublish::default())
            .to("orders.retry");
    });
    let tb = TestApp::start(app)
        .await
        .expect("both subjects declare their records");
    tb.shutdown().await.expect("shutdown");

    let server = MockServer::start().await;
    holds(&server, "orders-value", ORDER).await;
    holds(&server, "returns-v2", FOREIGN_ORDER).await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(&server), |b| {
        b.include(on_list)
            .out_retry(KafkaPublish::default())
            .to("orders.retry");
    });
    let err = start_error(app).await;
    assert!(
        err.contains("topic `returns` uses subject `returns-v2` (named by the descriptor)"),
        "{err}"
    );
}

#[subscriber(KafkaTopics::new(["orders", "returns"]).schema("refunds", "com.acme"))]
async fn on_mistyped(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_list_refuses_a_topic_it_does_not_read() {
    let server = MockServer::start().await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(&server), |b| {
        b.include(on_mistyped)
            .out_retry(KafkaPublish::default())
            .to("orders.retry");
    });
    let err = start_error(app).await;
    assert!(
        err.contains(
            "`.schema(\"refunds\", ..)` names a topic the subscription does not read; its topics \
             are [\"orders\", \"returns\"]"
        ),
        "{err}"
    );
}

#[subscriber(
    KafkaTopicPattern::new(r"^(orders|returns)\..+")
        .schema(r"^orders\.", "com.acme.orders")
        .schema(r"^returns\.", Schema::namespace("com.acme.returns").value("returns-value"))
)]
async fn on_matched(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pattern_checks_the_topics_that_exist_when_it_opens() {
    let server = MockServer::start().await;
    holds(&server, "orders.eu-value", FOREIGN_ORDER).await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(&server), |b| {
        b.include(on_matched)
            .out_retry(KafkaPublish::default())
            .to("orders.retry");
    });
    // The harness creates a topic on its first publish, so the pattern opens over none and is
    // checked as its topics appear; with nothing published yet the app starts.
    let tb = TestApp::start(app)
        .await
        .expect("no matching topic exists yet");
    tb.broker::<KafkaBroker>()
        .message(&Order { id: 1 })
        .to("orders.eu")
        .publish()
        .await
        .expect("publish");
    tb.broker::<KafkaBroker>()
        .subscriber(r"^(orders|returns)\..+")
        .assert_called_once()
        .with(&Order { id: 1 });
    tb.shutdown().await.expect("shutdown");
}
