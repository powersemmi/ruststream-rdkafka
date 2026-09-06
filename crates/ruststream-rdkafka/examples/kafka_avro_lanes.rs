//! Avro on the byte lanes: the delivery reaches the handler as the bytes it arrived as, and Avro
//! reads them against Avro's own schema. No codec is resolved for this topic, and no JSON
//! document exists anywhere on the path.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_avro_lanes --features avro -- run
//! ```

use apache_avro::AvroSchema;
use ruststream::prelude::*;
use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream_rdkafka::{IncomingFrame, KafkaBroker, OutgoingFrame, SchemaRegistry, avro};
use serde::{Deserialize, Serialize};

// --8<-- [start:types]
// Plain serde structs carrying their Avro schema. They deliberately do not declare the lanes:
// `apache-avro` reads and writes a Rust value through serde, and the core reserves the lanes for
// types that are not serde types - so the wire form rides the lane and the value's conversion is
// a call.
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

// --8<-- [start:wiring]
/// What the handler needs, resolved once at startup: the registry it reads writer schemas
/// through, and the subject its replies are framed under.
#[derive(Clone)]
struct Wiring {
    registry: SchemaRegistry,
    confirmations: avro::Subject<Confirmation>,
}

#[derive(FromRef)]
struct Orders {
    wiring: Wiring,
}
// --8<-- [end:wiring]

// --8<-- [start:handler]
// The body reads `IncomingFrame`, which is what is on the wire: the schema id the producer wrote
// with, and the datum. `decode_framed` fetches that writer schema and resolves the datum onto
// `Order`'s own, so a producer still on an older version of the subject stays readable.
#[subscriber("orders", publish("confirmations"))]
async fn confirm(
    frame: &IncomingFrame<'_>,
    State(wiring): State<Wiring>,
) -> Result<OutgoingFrame, HandlerOutcome> {
    let order: Order = avro::decode_framed(&wiring.registry, frame)
        .await
        .map_err(|_| HandlerOutcome::drop())?;
    println!("order {} of {}", order.id, order.item);

    // No I/O: the subject resolved its id at startup, so framing the reply is a byte operation.
    wiring
        .confirmations
        .frame(&Confirmation {
            id: order.id,
            accepted: true,
        })
        .map_err(|_| HandlerOutcome::drop())
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    // No `schema_registry(..)` on the broker: that attaches the JSON transcode to every
    // subscription, and this service reads the wire itself.
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("orders-svc");

    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| {
            let registry = SchemaRegistry::new("http://localhost:8081");
            // `register` publishes this type's schema and takes the id back; `resolve` looks the
            // id up without registering, for deployments where producers must not create
            // schemas. Either way a subject that does not fit fails startup, not the first
            // publish.
            let confirmations =
                avro::Subject::<Confirmation>::register(&registry, "confirmations-value").await?;
            Ok::<_, ruststream_rdkafka::KafkaError>(Orders {
                wiring: Wiring {
                    registry,
                    confirmations,
                },
            })
        })
        .with_broker(broker, |b| {
            b.include(confirm);
        })
}
// --8<-- [end:app]
