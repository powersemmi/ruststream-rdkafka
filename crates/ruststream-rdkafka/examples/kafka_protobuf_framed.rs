//! `KafkaPublish::framed` at a mount site: a Protobuf reply leaves with the Confluent envelope,
//! carrying the schema id its destination's subject holds. The scenario is a test against the
//! Schema Registry at `SCHEMA_REGISTRY_TEST_URL`; the cluster is the in-process one.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://localhost:8081 \
//!     cargo test --example kafka_protobuf_framed --features protobuf,testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_rdkafka::SchemaRegistry;
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::protobuf::decode_confluent;

#[derive(Clone, PartialEq, prost::Message, Deserialized)]
#[wire(decode = decode_confluent)]
struct Order {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    item: String,
}

// --8<-- [start:handler]
// The receipt names no destination, so the mount site's `reply(..)` does.
#[derive(Clone, PartialEq, prost::Message, Serialized, Outgoing)]
#[wire(encode = prost::Message::encode)]
struct Receipt {
    #[prost(int64, tag = "1")]
    order_id: i64,
    #[prost(bool, tag = "2")]
    accepted: bool,
}

#[subscriber("orders", reply("order-receipts"))]
async fn receipt(order: &Order) -> Receipt {
    Receipt {
        order_id: order.id,
        accepted: !order.item.is_empty(),
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app(registry: &SchemaRegistry) -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
        |b| {
            // The subject is the destination topic's, `order-receipts-value`, resolved on the
            // first reply and cached.
            b.include(receipt).out_reply(Publish::framed(registry));
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

    use super::{Order, Receipt, app};

    /// What the `order-receipts-value` subject holds.
    const RECEIPT_PROTO: &str = r#"
syntax = "proto3";
package shop;

message Receipt {
  int64 order_id = 1;
  bool accepted = 2;
}
"#;

    /// A record exactly as a producer wrote it.
    #[derive(Serialized, Outgoing)]
    #[outgoing(name = "orders")]
    struct Record(Vec<u8>);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_a_reply_carries_its_subjects_schema_id() -> Result<(), Box<dyn Error>> {
        let Some(url) = live_registry() else {
            return Ok(());
        };
        let registry = SchemaRegistry::new(url);
        let id = registry
            .register("order-receipts-value", SchemaType::Protobuf, RECEIPT_PROTO)
            .await?;

        let tb = TestApp::start(app(&registry)).await?;
        let order = Order {
            id: 9,
            item: "anvil".to_owned(),
        };
        tb.broker::<KafkaBroker>()
            .message(&Record(order.encode_to_vec()))
            .publish()
            .await?;

        // The envelope: the schema id, then the index path of the subject's first message.
        let mut datum = vec![0];
        let receipt = Receipt {
            order_id: 9,
            accepted: true,
        };
        datum.extend(receipt.encode_to_vec());
        tb.broker::<KafkaBroker>()
            .published::<()>("order-receipts")
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
