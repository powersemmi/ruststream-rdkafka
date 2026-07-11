//! Confluent Schema Registry as broker middleware: handlers and codecs stay plain JSON, the
//! broker transcodes. Consuming strips (and, with the avro/protobuf features, converts)
//! framed payloads on the async path; publishing frames the JSON payload for the wire, with
//! the subject resolved lazily. No custom codecs anywhere.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_schema_registry --features schema-registry -- run
//! ```

use ruststream::runtime::{App, AppInfo, RustStream, TypedPublisher};
use ruststream::subscriber;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{KafkaBroker, KafkaError, SchemaFormat, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:types]
// Plain serde structs; `JsonSchema` (schemars, re-exported) is only needed to register a
// subject straight from the type.
#[derive(Debug, Deserialize, JsonSchema)]
struct Order {
    id: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
struct Confirmation {
    id: i64,
    accepted: bool,
}
// --8<-- [end:types]

// --8<-- [start:handler]
// An ordinary handler on the default JSON codec: the broker middleware already stripped the
// Confluent envelope (and, with the avro/protobuf features, converted the datum to JSON).
#[subscriber("orders", publish("confirmations"))]
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
    // One registry client, shared: the broker side transcodes incoming framed payloads, the
    // publisher side frames outgoing ones (subject = "{topic}-value" by default, resolved
    // lazily on the first publish).
    let sr = SchemaRegistry::new("http://localhost:8081");
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("orders-svc")
        .schema_registry(sr.clone());

    let confirmations = TypedPublisher::new(broker.publisher().schema_format(SchemaFormat::Json));

    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // Producers own their schemas: register (or `warm`) the reply subject at startup.
        .on_startup(async move |()| {
            sr.register_json::<Confirmation>("confirmations-value")
                .await?;
            Ok::<_, KafkaError>(())
        })
        .with_broker(broker, |b| {
            b.include_publishing(confirm, confirmations);
        })
    // --8<-- [end:wiring]
}
