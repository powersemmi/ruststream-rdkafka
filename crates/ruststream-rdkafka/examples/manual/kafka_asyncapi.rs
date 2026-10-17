//! The service of `examples/kafka_asyncapi.rs` without the macros: the handlers are named types,
//! the descriptors are mounted with `map_source`, and `main` is written by hand. The tests read
//! the same document.
//!
//! ```text
//! cargo run --example manual_kafka_asyncapi --features asyncapi,schema-registry -- asyncapi gen
//! cargo test --example manual_kafka_asyncapi --features asyncapi,schema-registry
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_rdkafka::SchemaRegistry;
use ruststream_rdkafka::prelude::*;

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Confirmation {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the mount site names the reply's topic.
impl OutgoingDestination for Confirmation {
    type Form = CallerName;
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation { id: order.id }))
    }
}

struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "audited");
        ready(Ok(()))
    }
}

struct Ledger;

impl Handle<Order> for Ledger {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "booked");
        ready(Ok(()))
    }
}

// --8<-- [start:server]
/// The production cluster over `SASL_SSL`, with its schema registry.
fn broker(password: &str) -> KafkaBroker {
    let registry =
        SchemaRegistry::new("https://registry.internal:8081").basic_auth("orders-svc", password);
    KafkaBroker::new([
        "SASL_SSL://kafka-1.internal:9093",
        "SASL_SSL://kafka-2.internal:9093",
    ])
    .default_group("orders-svc")
    .config("security.protocol", "SASL_SSL")
    .config("sasl.mechanism", "SCRAM-SHA-512")
    .config("sasl.username", "orders-svc")
    .config("sasl.password", password)
    .schema_registry(registry)
}
// --8<-- [end:server]

// --8<-- [start:channels]
// --8<-- [start:operations]
/// The topic `orders`, read in the group `orders-svc` by the client `orders-reader`.
fn orders() -> KafkaTopic {
    KafkaTopic::new("orders")
        .group("orders-svc")
        .config("client.id", "orders-reader")
}
// --8<-- [end:operations]

/// Both audit topics, through one consumer in the group `audit-svc`.
fn audit_topics() -> KafkaTopics {
    KafkaTopics::new(["audit", "audit.eu"]).group("audit-svc")
}

/// The first two partitions of `ledger`, read apart from any group.
fn ledger_partitions() -> KafkaPartitions {
    KafkaPartitions::new("ledger", [0, 1])
}
// --8<-- [end:channels]

// --8<-- [start:app]
fn service(broker: KafkaBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker_labeled("kafka", broker, |b| {
        let confirm = subscriber("orders", Confirm).map_source(|_| orders());
        b.include(confirm.reply().to("confirmations").build());
        b.include(
            subscriber("audit", Audit)
                .map_source(|_| audit_topics())
                .build(),
        );
        b.include(
            subscriber("ledger", Ledger)
                .map_source(|_| ledger_partitions())
                .build(),
        );
    })
}

fn app() -> impl App<State = ()> {
    let password = env::var("KAFKA_PASSWORD").unwrap_or_default();
    service(broker(&password))
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::{broker, service};

    const PASSWORD: &str = "hunter2";

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(
            &build_spec(&service(broker(PASSWORD))).to_json()?,
        )?)
    }

    #[test]
    fn every_part_of_the_document_speaks_kafka() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        for bindings in [
            &spec["servers"]["kafka"]["bindings"],
            &spec["channels"]["orders"]["bindings"],
            &spec["operations"]["receive_orders"]["bindings"],
        ] {
            assert_eq!(bindings["kafka"]["bindingVersion"], "0.5.0", "{bindings}");
        }
        Ok(())
    }

    #[test]
    fn the_server_is_a_coordinate_and_its_registry() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["kafka"],
            json!({
                "host": "kafka-1.internal:9093,kafka-2.internal:9093",
                "protocol": "kafka",
                "bindings": {
                    "kafka": {
                        "schemaRegistryUrl": "https://registry.internal:8081",
                        "schemaRegistryVendor": "confluent",
                        "bindingVersion": "0.5.0",
                    },
                },
            })
        );
        Ok(())
    }

    #[test]
    fn the_document_carries_no_credential() -> Result<(), Box<dyn Error>> {
        let text = document()?.to_string();

        assert!(!text.contains(PASSWORD), "{text}");
        Ok(())
    }

    #[test]
    fn each_channel_reports_its_topic() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let channels = &spec["channels"];

        assert_eq!(channels["orders"]["bindings"]["kafka"]["topic"], "orders");
        assert_eq!(
            channels["confirmations"]["bindings"]["kafka"]["topic"],
            "confirmations"
        );
        assert_eq!(channels["ledger"]["bindings"]["kafka"]["topic"], "ledger");
        // A set of topics has no one topic to report.
        assert!(
            channels["audit,audit.eu"]["bindings"].is_null(),
            "{channels}"
        );
        Ok(())
    }

    #[test]
    fn a_receive_operation_reports_its_group() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let operations = &spec["operations"];

        assert_eq!(
            operations["receive_orders"]["bindings"]["kafka"],
            json!({
                "groupId": { "type": "string", "enum": ["orders-svc"] },
                "clientId": { "type": "string", "enum": ["orders-reader"] },
                "bindingVersion": "0.5.0",
            })
        );
        assert_eq!(
            operations["receive_audit_audit_eu"]["bindings"]["kafka"]["groupId"]["enum"][0],
            "audit-svc"
        );
        // A reader that names no group and no client id has nothing to report.
        assert!(
            operations["receive_ledger"]["bindings"].is_null(),
            "{operations}"
        );
        Ok(())
    }
}
