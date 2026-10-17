//! A service logs its broker and its descriptors with `{:?}`; the credentials librdkafka marks
//! sensitive never reach that output, and every other setting does.

use ruststream_rdkafka::{KafkaBroker, KafkaPartitions, KafkaTopic, KafkaTopics};

/// Every property librdkafka marks sensitive, with a value no other setting prints.
const SECRETS: [(&str, &str); 6] = [
    ("sasl.password", "secret-sasl-password"),
    ("ssl.key.password", "secret-key-password"),
    ("ssl.key.pem", "secret-key-pem"),
    ("ssl.keystore.password", "secret-keystore-password"),
    ("sasl.oauthbearer.client.secret", "secret-client-secret"),
    ("sasl.oauthbearer.config", "secret-oauth-config"),
];

fn assert_redacted(printed: &str) {
    for (key, value) in SECRETS {
        assert!(printed.contains(key), "{key} is named: {printed}");
        assert!(!printed.contains(value), "{key}'s value leaks: {printed}");
    }
    assert!(
        printed.contains("orders-svc"),
        "a plain setting is shown: {printed}"
    );
}

#[test]
fn a_broker_prints_no_credential() {
    let mut broker = KafkaBroker::new(["kafka:9092"]).config("client.id", "orders-svc");
    for (key, value) in SECRETS {
        broker = broker.config(key, value).producer_config(key, value);
    }
    assert_redacted(&format!("{broker:?}"));
}

#[test]
fn a_descriptor_prints_no_credential() {
    let mut topic = KafkaTopic::new("orders").config("client.id", "orders-svc");
    let mut topics = KafkaTopics::new(["orders"]).config("client.id", "orders-svc");
    let mut partitions = KafkaPartitions::new("orders", [0]).config("client.id", "orders-svc");
    for (key, value) in SECRETS {
        topic = topic.config(key, value);
        topics = topics.config(key, value);
        partitions = partitions.config(key, value);
    }
    assert_redacted(&format!("{topic:?}"));
    assert_redacted(&format!("{topics:?}"));
    assert_redacted(&format!("{partitions:?}"));
}
