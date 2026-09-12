//! Avro as a codec: the schema lives in the codec, and handlers stay ordinary functions over
//! ordinary structs.
//!
//! This is the path to reach for first on a registry-backed Avro topic. Nothing about Avro
//! appears in a handler's signature, the models carry no derive of this crate's, and the wire
//! form is the codec's business - which is what a codec is for.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_avro_codec --features avro -- run
//! ```

use apache_avro::AvroSchema;
use ruststream::runtime::{App, AppInfo, HandlerOutcome, Router, RustStream};
use ruststream::subscriber;
use ruststream_rdkafka::avro::AvroCodec;
use ruststream_rdkafka::{KafkaBroker, SchemaPrefetch, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:types]
// Plain serde structs. `AvroSchema` derives the schema the codec registers; nothing here knows
// it will travel as Avro.
#[derive(Debug, Serialize, Deserialize, AvroSchema)]
struct Order {
    id: i64,
    item: String,
}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
struct Shipment {
    id: i64,
    carrier: String,
}
// --8<-- [end:types]

// --8<-- [start:handler]
// Ordinary handlers, and one codec serves both: decoding needs no registration at all, because
// the writer schema comes off each delivery's envelope.
#[subscriber("orders")]
async fn take_order(order: &Order) -> HandlerOutcome {
    println!("order {} of {}", order.id, order.item);
    HandlerOutcome::ack()
}

#[subscriber("shipments")]
async fn take_shipment(shipment: &Shipment) -> HandlerOutcome {
    println!("shipment {} via {}", shipment.id, shipment.carrier);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    // --8<-- [start:wiring]
    // The prefetch is the async half: `connect` resolves the subjects registered below, and each
    // delivery's writer schema is resolved on the consume path - so the codec itself, which is
    // synchronous, never reaches the network.
    let prefetch = SchemaPrefetch::new(SchemaRegistry::new("http://localhost:8081"));

    // One codec for the whole scope. Registration is the publish side only: it says which subject
    // a type's values are framed under, and captures that type's schema so a subject that has
    // gone missing can be put back.
    let codec = AvroCodec::registry(&prefetch).register::<Order>("orders-value");

    // --8<-- [start:cascade]
    // A reader schema applies to every delivery its codec decodes, so a codec carrying one can
    // only serve a single reading type. This handler wants Avro's resolution to fill the fields
    // older producers never wrote, so it gets its own codec, in its own router - and the scope's
    // codec, which carries no reader schema, keeps serving everything else.
    let shipments = AvroCodec::registry(&prefetch)
        .register::<Shipment>("shipments-value")
        .resolve_onto(Shipment::get_schema())
        .expect("the reader schema resolves");
    // --8<-- [end:cascade]

    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("orders-svc")
        .schema_prefetch(prefetch);

    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_codec(broker, codec, |b| {
        b.include(take_order);
        // Most specific wins, exactly as for any other codec.
        b.include_router(Router::new().with_codec(shipments).include(take_shipment));
    })
    // --8<-- [end:wiring]
}

// --8<-- [start:local]
/// A topic with no registry: one schema, pinned here, and a bare datum on the wire.
#[allow(
    dead_code,
    reason = "the other schema source, shown rather than wired into this app"
)]
fn local_codec() -> AvroCodec {
    AvroCodec::local(Order::get_schema()).expect("the schema resolves")
}
// --8<-- [end:local]
