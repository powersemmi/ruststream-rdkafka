//! Schema checking per topic: `.schema(namespace)` on a descriptor makes the broker check, while
//! the service starts, that the topic's registry subjects exist and declare its records. Each
//! scenario is a test, against a registry served over HTTP.
//!
//! ```text
//! cargo test --example kafka_subscriptions_schema --features testing,schema-registry
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema::Schema;
use ruststream_rdkafka::{SchemaPrefetch, SchemaRegistry};
use serde::{Deserialize, Serialize};

/// The record `com.acme.orders.Order`.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: i64,
}

// --8<-- [start:schema]
/// `orders` is checked against `orders-value`, the subject its name gives.
#[subscriber(KafkaTopic::new("orders").schema("com.acme.orders"))]
async fn on_order(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

/// Each listed topic opts in by name; `returns` names its value subject.
#[subscriber(
    KafkaTopics::new(["refunds", "returns"])
        .schema("refunds", "com.acme.orders")
        .schema("returns", Schema::namespace("com.acme.orders").value("returns-v2"))
)]
async fn on_settlement(order: &Order, Ctx(topic): Ctx<Topic>) -> HandlerOutcome {
    tracing::info!(order.id, %topic, "settled");
    HandlerOutcome::ack()
}

fn app(registry: &str) -> impl App {
    let prefetch = SchemaPrefetch::new(SchemaRegistry::new(registry));
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("orders-svc")
        .schema_prefetch(prefetch);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(on_order);
        b.include(on_settlement)
            .out_retry(Publish::default())
            .to("settlements.retry");
    })
}
// --8<-- [end:schema]

// The scenarios are the tests above; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    /// The registry, holding each subject with `namespace.Order` as its newest schema.
    async fn registry(namespace: &str, subjects: &[&str]) -> MockServer {
        let server = MockServer::start().await;
        let schema = format!(
            r#"{{"type":"record","name":"Order","namespace":"{namespace}","fields":[{{"name":"id","type":"long"}}]}}"#
        );
        for subject in subjects {
            Mock::given(method("GET"))
                .and(path(format!("/subjects/{subject}/versions/latest")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({ "id": 1, "schema": schema })),
                )
                .mount(&server)
                .await;
        }
        server
    }

    #[tokio::test]
    async fn a_topic_whose_subjects_declare_its_records_starts() -> Result<(), Box<dyn Error>> {
        let server = registry(
            "com.acme.orders",
            &["orders-value", "refunds-value", "returns-v2"],
        )
        .await;
        let tb = TestApp::start(app(&server.uri())).await?;

        tb.broker::<KafkaBroker>()
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_subject_of_another_namespace_stops_the_service() {
        let server = registry(
            "com.other",
            &["orders-value", "refunds-value", "returns-v2"],
        )
        .await;

        let refused = TestApp::start(app(&server.uri()))
            .await
            .expect_err("`orders-value` declares no record of `com.acme.orders`");
        assert!(
            refused.to_string().contains(
                "topic `orders` uses subject `orders-value` (from its name), which declares no \
                 record in namespace `com.acme.orders`"
            ),
            "{refused}"
        );
    }
}
