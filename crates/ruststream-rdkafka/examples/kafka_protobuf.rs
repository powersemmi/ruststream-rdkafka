//! Schema Registry Protobuf through the middleware: handlers and codecs stay plain JSON,
//! the edges convert - incoming Protobuf messages arrive as JSON, and the `SchemaFrame`
//! publish layer puts framed Protobuf on the wire because the reply topic's subject holds a
//! Protobuf schema. No code generation anywhere.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_protobuf --features protobuf -- run
//! ```

use ruststream::runtime::{App, AppInfo, RustStream, TypedPublisher};
use ruststream::subscriber;
use ruststream_rdkafka::{KafkaBroker, KafkaError, SchemaFrame, SchemaRegistry, SchemaType};
use serde::{Deserialize, Serialize};

// --8<-- [start:types]
// The registry stores Protobuf schemas as source; handlers stay plain serde structs whose
// field names match the .proto (prost code generation keeps working, but is not required).
const CONFIRMATIONS_PROTO: &str = r#"
syntax = "proto3";
package acme;

message Confirmation {
  int64 id = 1;
  bool accepted = 2;
}
"#;

#[derive(Debug, Deserialize)]
struct Order {
    id: i64,
}

#[derive(Debug, Serialize)]
struct Confirmation {
    id: i64,
    accepted: bool,
}
// --8<-- [end:types]

// An ordinary handler on the default JSON codec: the middleware converts both directions.
#[subscriber("orders", publish("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: true,
    }
}

#[ruststream::app]
fn app() -> impl App {
    // --8<-- [start:wiring]
    let sr = SchemaRegistry::new("http://localhost:8081");
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("orders-svc")
        .schema_registry(sr.clone());

    let confirmations = TypedPublisher::new(broker.publisher());

    // The reply subject holds a Protobuf schema, so replies go out as framed Protobuf; the
    // message defaults to the schema's first top-level one (pin another per topic with
    // `.message("confirmations", "acme.Confirmation")`).
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .publish_layer(SchemaFrame::new(sr.clone()))
        .on_startup(async move |()| {
            sr.register(
                "confirmations-value",
                SchemaType::Protobuf,
                CONFIRMATIONS_PROTO,
            )
            .await?;
            Ok::<_, KafkaError>(())
        })
        .with_broker(broker, |b| {
            b.include_publishing(confirm, confirmations);
        })
    // --8<-- [end:wiring]
}
