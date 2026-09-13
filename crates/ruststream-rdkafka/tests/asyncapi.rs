//! The generated document: what this crate says about itself in Kafka's own vocabulary.

#![cfg(all(feature = "asyncapi", feature = "testing", feature = "schema-registry"))]

use ruststream::asyncapi::build_spec;
use ruststream::conformance::harness;
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::subscriber;
use ruststream_rdkafka::{KafkaBroker, KafkaPartitions, KafkaTopic, KafkaTopics, SchemaRegistry};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// One topic, one group, and a client id through the raw passthrough: everything the Kafka
/// binding can say about a consumer without a connection.
#[subscriber(
    KafkaTopic::new("orders")
        .group("orders-svc")
        .config("client.id", "orders-reader")
)]
async fn confirm(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// A set of topics: one consumer group, and no single topic to name on the channel.
#[subscriber(KafkaTopics::new(["audit", "audit.eu"]).group("audit-svc"))]
async fn audit(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// Named partitions of one topic: the channel is that topic, and the group is only where the
/// reader commits.
#[subscriber(KafkaPartitions::new("ledger", [0]).group("ledger-svc"))]
async fn ledger(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn document() -> Value {
    let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker_labeled(
        "kafka",
        KafkaBroker::new(["broker-1:9092", "broker-2:9092"]).schema_registry(SchemaRegistry::new(
            "http://svc:hunter2@registry.internal:8081",
        )),
        |b| {
            b.include(confirm);
            b.include(audit);
            b.include(ledger);
        },
    );
    let json = build_spec(&app)
        .to_json()
        .expect("the document must serialize");
    serde_json::from_str(&json).expect("the document must be valid JSON")
}

#[test]
fn a_topic_describes_its_channel_and_its_consumer_group() {
    let value = document();

    let channel = &value["channels"]["orders"]["bindings"]["kafka"];
    assert_eq!(channel["topic"], "orders");
    // The core writes the version, so a binding cannot ship without one.
    assert_eq!(channel["bindingVersion"], "0.5.0");

    let operation = &value["operations"]["receive_orders"]["bindings"]["kafka"];
    assert_eq!(operation["groupId"]["type"], "string");
    assert_eq!(operation["groupId"]["enum"][0], "orders-svc");
    assert_eq!(operation["clientId"]["enum"][0], "orders-reader");
}

/// A subscription over several topics has a group but no one topic, so it says the group and
/// leaves the channel alone rather than naming one of the set.
#[test]
fn a_topic_set_describes_its_group_and_no_topic() {
    let value = document();

    assert!(
        value["channels"]["audit,audit.eu"]["bindings"].is_null(),
        "a set of topics has no single topic to report, got: {}",
        value["channels"]["audit,audit.eu"],
    );
    assert_eq!(
        value["operations"]["receive_audit_audit_eu"]["bindings"]["kafka"]["groupId"]["enum"][0],
        "audit-svc",
    );
}

#[test]
fn a_partition_reader_describes_the_topic_it_reads() {
    let value = document();

    assert_eq!(
        value["channels"]["ledger"]["bindings"]["kafka"]["topic"],
        "ledger",
    );
}

/// The Kafka protocol negotiates its version per API key between client and cluster, so no one
/// number describes what clients speak; the field stays out rather than claiming a version.
#[test]
fn a_kafka_server_reports_a_coordinate_and_no_protocol_version() {
    let value = document();
    let server = &value["servers"]["kafka"];

    assert_eq!(server["host"], "broker-1:9092,broker-2:9092");
    assert_eq!(server["protocol"], "kafka");
    assert!(server["protocolVersion"].is_null(), "got: {server}");

    // The registry is a coordinate a client needs; the password it is reached with is not.
    let binding = &server["bindings"]["kafka"];
    assert_eq!(
        binding["schemaRegistryUrl"],
        "http://registry.internal:8081",
    );
    assert_eq!(binding["schemaRegistryVendor"], "confluent");
}

/// The document is published and shared, so nothing the broker or a descriptor contributes may
/// carry the password they were configured with.
#[test]
fn the_description_carries_no_credentials() {
    harness::describes_without_credentials(
        &KafkaBroker::new(["kafka:9092"])
            .config("sasl.username", "orders")
            .config("sasl.password", "hunter2"),
        &KafkaTopic::new("orders").group("orders-svc"),
        "hunter2",
    );
}

/// A registry-backed publisher frames its payloads in the Confluent envelope, so the schema id
/// is in the payload and the naming strategy says which subject it belongs to.
#[cfg(feature = "protobuf")]
#[test]
fn a_registry_backed_publisher_says_where_its_schema_id_lives() {
    use ruststream::PublishPolicy;
    use ruststream_rdkafka::{ConnectedKafkaBroker, KafkaPublish, SubjectStrategy};

    let policy = KafkaPublish::framed(&SchemaRegistry::new("http://registry.internal:8081"))
        .subject_strategy(SubjectStrategy::TopicRecordName);
    let bindings = PublishPolicy::<ConnectedKafkaBroker>::message_bindings(&policy).expect_kafka();

    assert_eq!(bindings["schemaIdLocation"], "payload");
    assert_eq!(bindings["schemaIdPayloadEncoding"], "confluent");
    assert_eq!(bindings["schemaLookupStrategy"], "TopicRecordNameStrategy");
}

/// Reads one binding set back as JSON, the way the document carries it.
#[cfg(feature = "protobuf")]
trait ExpectKafka {
    fn expect_kafka(&self) -> Value;
}

#[cfg(feature = "protobuf")]
impl ExpectKafka for ruststream::asyncapi::Bindings {
    fn expect_kafka(&self) -> Value {
        let all: Value = serde_json::to_value(self).expect("bindings serialize");
        all["kafka"].clone()
    }
}

/// The excerpt the documentation shows, kept honest by the document itself.
#[test]
fn the_documented_excerpt_is_what_the_crate_emits() {
    let value = document();
    let excerpt = serde_json::json!({
        "servers": {
            "kafka": { "bindings": value["servers"]["kafka"]["bindings"] },
        },
        "channels": {
            "orders": { "bindings": value["channels"]["orders"]["bindings"] },
        },
        "operations": {
            "receive_orders": {
                "bindings": value["operations"]["receive_orders"]["bindings"],
            },
        },
    });

    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/snippets/asyncapi-bindings.json"
    );
    let shown: Value = serde_json::from_str(
        &std::fs::read_to_string(path).expect("the documented excerpt must exist"),
    )
    .expect("the documented excerpt must be valid JSON");

    assert_eq!(
        shown,
        excerpt,
        "the documentation shows a document this crate no longer emits; write back:\n{}",
        serde_json::to_string_pretty(&excerpt).expect("the excerpt serializes"),
    );
}
