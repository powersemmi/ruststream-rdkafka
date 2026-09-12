//! Confluent Schema Registry as broker middleware: handlers and codecs stay plain JSON, the
//! broker's edges transcode. Consuming strips (and, with the avro/protobuf features,
//! converts) framed payloads on the subscription's async path; publishing frames the JSON
//! payload for the wire through the app's publish pipeline, with the subject resolved
//! lazily. No custom codecs anywhere.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_schema_registry --features schema-registry -- run
//! ```

use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream::{Outgoing, subscriber};
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{KafkaBroker, KafkaError, SchemaFrame, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:types]
// Plain serde structs; `JsonSchema` (schemars, re-exported) is only needed to register a
// subject straight from the type.
#[derive(Debug, Deserialize, JsonSchema)]
struct Order {
    id: i64,
}

// The reply topic is a property of the confirmation itself, so the type declares it and the
// subscriber's clause names none.
#[derive(Debug, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: i64,
    accepted: bool,
}
// --8<-- [end:types]

// --8<-- [start:handler]
// An ordinary handler on the default JSON codec: the broker middleware already stripped the
// Confluent envelope (and, with the avro/protobuf features, converted the datum to JSON).
#[subscriber("orders", publish)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: true,
    }
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    // --8<-- [start:wiring]
    // One registry client, shared by the two edges: the broker transcodes incoming framed
    // payloads to JSON, and the `SchemaFrame` publish middleware frames outgoing ones by
    // their subject's registered flavor (subject = "{topic}-value" by default, resolved
    // lazily on the first publish; topics without a subject publish plain).
    let registry = SchemaRegistry::new("http://localhost:8081");
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("orders-svc")
        .schema_registry(registry.clone());

    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .publish_layer(SchemaFrame::new(registry.clone()))
        // Producers own their schemas: register (or `warm`) the reply subject at startup.
        .on_startup(async move |()| {
            registry
                .register_json::<Confirmation>("confirmations-value")
                .await?;
            Ok::<_, KafkaError>(())
        })
        .with_broker(broker, |b| {
            // The reply rides the broker's default publish policy, so the include site names
            // no publisher.
            b.include(confirm);
        })
    // --8<-- [end:wiring]
}
