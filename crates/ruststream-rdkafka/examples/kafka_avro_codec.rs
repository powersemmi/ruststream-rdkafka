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
use ruststream_rdkafka::schema_registry::RegistrySubject;
use ruststream_rdkafka::{KafkaBroker, SchemaPrefetch, SchemaRegistry};
use serde::Deserialize;

// --8<-- [start:types]
// Plain serde structs. `AvroSchema` derives the schema the codec is built from; nothing here
// knows it will travel as Avro.
#[derive(Debug, Deserialize, AvroSchema)]
struct Order {
    id: i64,
    #[allow(dead_code, reason = "part of the schema, unused by the audit handler")]
    item: String,
}

// The subject is a fact about the type, so it is written here once and every mount site names
// the type instead of repeating the string.
impl RegistrySubject for Order {
    const SUBJECT: &'static str = "orders-value";
}

/// The same record one version on. A consumer that has moved ahead of its producers names this
/// as its reader schema, so Avro's resolution fills what an older writer never wrote.
#[derive(Debug, Deserialize, AvroSchema)]
#[serde(rename = "Order")]
struct OrderV2 {
    id: i64,
    item: String,
    #[avro(default = r#""none""#)]
    note: String,
}
// --8<-- [end:types]

// --8<-- [start:handler]
// An ordinary handler: the codec decoded the delivery before it got here.
#[subscriber("orders")]
async fn take_order(order: &OrderV2) -> HandlerOutcome {
    println!("order {} of {} ({})", order.id, order.item, order.note);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    // --8<-- [start:wiring]
    // The prefetch is the async half: `connect` resolves the subjects the codecs publish under,
    // and each delivery's writer schema is resolved on the consume path - so the codec itself,
    // which is synchronous, never reaches the network.
    let prefetch = SchemaPrefetch::new(SchemaRegistry::new("http://localhost:8081"));
    let prefetch_for_audit = prefetch.clone();
    // The subject comes off the type: `for_type` reads `Order::SUBJECT` here, at construction,
    // where the type is known - which is the one place a codec can read a declaration on it.
    let codec = AvroCodec::for_type::<Order>(&prefetch)
        .resolve_onto(OrderV2::get_schema())
        .expect("the reader schema resolves");

    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("orders-svc")
        .schema_prefetch(prefetch);

    // --8<-- [start:cascade]
    // The registry was named once, above. `with_broker_codec` makes this codec the scope's, and
    // a router mounted inside overrides it for the handlers it carries - the core's own codec
    // cascade, with the prefetch supplying every codec in it.
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_codec(broker, codec, |b| {
        b.include(take_order);
        b.include_router(
            Router::new()
                .with_codec(AvroCodec::registry(&prefetch_for_audit, "audit-value"))
                .include(audit_order),
        );
    })
    // --8<-- [end:cascade]
    // --8<-- [end:wiring]
}

#[subscriber("audit")]
async fn audit_order(order: &Order) -> HandlerOutcome {
    println!("audited order {}", order.id);
    HandlerOutcome::ack()
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
