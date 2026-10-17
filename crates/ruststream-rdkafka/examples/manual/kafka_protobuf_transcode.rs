//! The service of `examples/kafka_protobuf_transcode.rs` with its handler written as a `Handle`
//! impl instead of `#[subscriber]`.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://localhost:8081 \
//!     cargo test --example manual_kafka_protobuf_transcode --features protobuf,testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::schemars::{self, JsonSchema};
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::{KafkaError, SchemaFrame, SchemaRegistry, SchemaType};
use serde::{Deserialize, Serialize};

// --8<-- [start:types]
/// What the `order-confirmations-value` subject holds: the registry keeps Protobuf as `.proto`
/// source.
const CONFIRMATION_PROTO: &str = r#"
syntax = "proto3";
package shop;

message Confirmation {
  int64 id = 1;
  bool accepted = 2;
}
"#;

// Plain serde structs whose field names match the `.proto` messages.
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
struct Order {
    id: i64,
    item: String,
}

#[derive(Debug, PartialEq, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "order-confirmations")]
struct Confirmation {
    id: i64,
    accepted: bool,
}
// --8<-- [end:types]

// --8<-- [start:handler]
/// An ordinary handler on the default JSON codec.
struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation {
            id: order.id,
            accepted: !order.item.is_empty(),
        }))
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app(registry: &SchemaRegistry) -> impl App {
    // Every subscription of this broker turns a framed delivery into JSON.
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("orders-svc")
        .schema_registry(registry.clone());
    let subjects = registry.clone();
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // Writes framed Protobuf where the destination's subject holds a Protobuf schema.
        .publish_layer(SchemaFrame::new(registry.clone()))
        .on_startup(async move |()| {
            subjects
                .register(
                    "order-confirmations-value",
                    SchemaType::Protobuf,
                    CONFIRMATION_PROTO,
                )
                .await?;
            Ok::<_, KafkaError>(())
        })
        .with_broker(broker, |b| {
            b.include(subscriber("orders", Confirm).reply().build());
        })
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::encode_envelope;
    use ruststream_rdkafka::{SchemaRegistry, SchemaType};
    use serde::Deserialize;

    use super::{CONFIRMATION_PROTO, app};

    const ORDER_PROTO: &str = r#"
syntax = "proto3";
package shop;

message Order {
  int64 id = 1;
  string item = 2;
}
"#;

    /// The order as a Protobuf producer writes it.
    #[derive(Clone, PartialEq, prost::Message)]
    struct ProtoOrder {
        #[prost(int64, tag = "1")]
        id: i64,
        #[prost(string, tag = "2")]
        item: String,
    }

    /// The confirmation as a Protobuf consumer reads it.
    #[derive(Clone, PartialEq, prost::Message)]
    struct ProtoConfirmation {
        #[prost(int64, tag = "1")]
        id: i64,
        #[prost(bool, tag = "2")]
        accepted: bool,
    }

    /// What the handler was handed, read back as JSON.
    #[derive(Debug, PartialEq, Deserialize)]
    struct SeenOrder {
        id: i64,
        item: String,
    }

    /// A record exactly as a producer wrote it.
    #[derive(Serialized, Outgoing)]
    #[outgoing(name = "orders")]
    struct Record(Vec<u8>);

    /// `datum` framed for `id`, as the schema's first message.
    fn framed(id: u32, datum: &impl prost::Message) -> Vec<u8> {
        let mut body = vec![0];
        body.extend(datum.encode_to_vec());
        encode_envelope(id, &body)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_protobuf_arrives_as_json_and_leaves_framed() -> Result<(), Box<dyn Error>> {
        let Some(url) = live_registry() else {
            return Ok(());
        };
        let registry = SchemaRegistry::new(url);
        // The order's subject is this run's own; a delivery names its schema by id alone.
        let order_id = registry
            .register(
                &format!("transcode-orders-{}", process::id()),
                SchemaType::Protobuf,
                ORDER_PROTO,
            )
            .await?;

        let tb = TestApp::start(app(&registry)).await?;
        let order = ProtoOrder {
            id: 3,
            item: "anvil".to_owned(),
        };
        tb.broker::<KafkaBroker>()
            .message(&Record(framed(order_id, &order)))
            .publish()
            .await?;

        tb.broker::<KafkaBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&SeenOrder {
                id: 3,
                item: "anvil".to_owned(),
            });
        let confirmation_id = registry
            .register(
                "order-confirmations-value",
                SchemaType::Protobuf,
                CONFIRMATION_PROTO,
            )
            .await?;
        let confirmation = ProtoConfirmation {
            id: 3,
            accepted: true,
        };
        tb.broker::<KafkaBroker>()
            .published::<()>("order-confirmations")
            .assert_called_once()
            .with_raw(&framed(confirmation_id, &confirmation));
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
