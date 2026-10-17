//! `ProtobufFrame` as an app-wide publish layer: every Protobuf publish that leaves through a slot
//! is framed with its destination subject's schema id. The scenario is a test against the Schema
//! Registry at `SCHEMA_REGISTRY_TEST_URL`; the cluster is the in-process one.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://localhost:8081 \
//!     cargo test --example kafka_protobuf_frame --features protobuf,testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::protobuf::decode_confluent;
use ruststream_rdkafka::{ProtobufFrame, SchemaRegistry};

#[derive(Clone, PartialEq, prost::Message, Deserialized)]
#[wire(decode = decode_confluent)]
struct Order {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    item: String,
}

#[derive(Clone, PartialEq, prost::Message, Serialized, Outgoing)]
#[wire(encode = prost::Message::encode)]
#[outgoing(name = "order-audit")]
struct Audit {
    #[prost(int64, tag = "1")]
    order_id: i64,
}

// --8<-- [start:handler]
#[derive(OutSlot)]
#[publishes(Audit)]
struct Journal;

/// Records every order in the audit journal; the app's layer frames the record.
#[subscriber("orders")]
async fn audit(order: &Order, Out(journal): Out<impl Publisher, Journal>) -> HandlerOutcome {
    let record = Audit { order_id: order.id };
    if journal.message(&record).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app(registry: &SchemaRegistry) -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // Frames every publish whose destination subject holds a Protobuf schema.
        .publish_layer(ProtobufFrame::new(registry.clone()))
        .with_broker(
            KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
            |b| {
                b.include(audit).out(Journal, Publish::default()).build();
            },
        )
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use prost::Message as _;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::encode_envelope;
    use ruststream_rdkafka::{SchemaRegistry, SchemaType};

    use super::{Audit, Order, app};

    /// What the `order-audit-value` subject holds.
    const AUDIT_PROTO: &str = r#"
syntax = "proto3";
package shop;

message Audit {
  int64 order_id = 1;
}
"#;

    /// A record exactly as a producer wrote it.
    #[derive(Serialized, Outgoing)]
    #[outgoing(name = "orders")]
    struct Record(Vec<u8>);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_a_slot_publish_is_framed_by_the_layer() -> Result<(), Box<dyn Error>> {
        let Some(url) = live_registry() else {
            return Ok(());
        };
        let registry = SchemaRegistry::new(url);
        let id = registry
            .register("order-audit-value", SchemaType::Protobuf, AUDIT_PROTO)
            .await?;

        let tb = TestApp::start(app(&registry)).await?;
        let order = Order {
            id: 5,
            item: "anvil".to_owned(),
        };
        tb.broker::<KafkaBroker>()
            .message(&Record(order.encode_to_vec()))
            .publish()
            .await?;

        // The envelope: the schema id, then the index path of the subject's first message.
        let mut datum = vec![0];
        datum.extend(Audit { order_id: 5 }.encode_to_vec());
        tb.broker::<KafkaBroker>()
            .published::<()>("order-audit")
            .assert_called_once()
            .with_raw(&encode_envelope(id, &datum));
        tb.shutdown().await?;
        Ok(())
    }

    /// The registry at `SCHEMA_REGISTRY_TEST_URL`, or `None` to skip; under
    /// `RUSTSTREAM_REQUIRE_LIVE` a missing address fails the test instead.
    fn live_registry() -> Option<String> {
        match std::env::var("SCHEMA_REGISTRY_TEST_URL") {
            Ok(url) if !url.is_empty() => Some(url),
            _ => {
                assert!(
                    std::env::var("RUSTSTREAM_REQUIRE_LIVE").is_err(),
                    "RUSTSTREAM_REQUIRE_LIVE is set, but SCHEMA_REGISTRY_TEST_URL is not"
                );
                None
            }
        }
    }
}
