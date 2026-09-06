//! Schema Registry Avro through the middleware: handlers and codecs stay plain JSON, the
//! edges convert - incoming Avro datums arrive as JSON, and the `SchemaFrame` publish layer
//! puts Avro on the wire because the reply topic's subject holds an Avro schema.
//!
//! This is the compatibility path, for a service that must keep plain serde models on a
//! registry-backed topic. It costs a JSON hop per message and resolves no writer schema onto a
//! reader schema, because a JSON handler has none. `kafka_avro_lanes` is the canonical path.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_avro --features avro -- run
//! ```

use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream::subscriber;
use ruststream_rdkafka::avro::AvroSchema;
use ruststream_rdkafka::{KafkaBroker, KafkaError, SchemaFrame, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:types]
// Plain serde structs; `AvroSchema` (re-exported) is only needed to register a subject
// straight from the type.
#[derive(Debug, Deserialize, AvroSchema)]
struct Order {
    id: i64,
    item: String,
}

#[derive(Debug, Serialize, AvroSchema)]
struct Confirmation {
    id: i64,
    accepted: bool,
}
// --8<-- [end:types]

// --8<-- [start:handler]
// An ordinary handler on the default JSON codec: the middleware already converted the Avro
// datum to JSON on the way in, and converts the reply back to Avro on the way out.
#[subscriber("orders", publish("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    let _ = &order.item;
    Confirmation {
        id: order.id,
        accepted: true,
    }
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    // --8<-- [start:wiring]
    // The reply subject holds an Avro schema, so the SchemaFrame layer puts Avro on the
    // wire; nothing Avro-specific is declared on the publisher.
    let sr = SchemaRegistry::new("http://localhost:8081");
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("orders-svc")
        .schema_registry(sr.clone());

    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .publish_layer(SchemaFrame::new(sr.clone()))
        .on_startup(async move |()| {
            sr.register_avro::<Confirmation>("confirmations-value")
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
