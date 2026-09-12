//! Protobuf with nothing manual in the handler: a generated message arrives as itself, the
//! handler returns its reply, and the Confluent envelope is put on and taken off around the body.
//!
//! Two halves make that work, and they are asymmetric on purpose. Reading needs no registry at
//! all - Protobuf is tag-addressed, so a delivery decodes against the reader's own type - so the
//! type names this crate's `decode_confluent` on its decode lane and the envelope never reaches
//! the signature. Writing does need one number, the subject's schema id, and a value cannot
//! fetch it: `Serialized::wire_bytes` is synchronous and has only `&self`. The publish path can,
//! because it knows the destination topic, so the value writes bare Protobuf through `prost` and
//! the reply's publisher puts the id and the message-index path in front of it.
//!
//! To set framing once for a whole app instead of per mount site, add `ProtobufFrame` as a
//! `publish_layer`; it covers every publish that leaves through a slot or a publisher the mount
//! never named. The two compose - whichever runs first frames the payload, and the other leaves
//! the envelope it finds alone.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_protobuf_plain --features protobuf -- run
//! ```

use ruststream::prelude::*;
use ruststream::runtime::{App, AppInfo, Reply, RustStream};
use ruststream_rdkafka::{KafkaBroker, KafkaPublish, SchemaRegistry};

// --8<-- [start:types]
// What `prost-build` emits, plus the lane derives. `#[wire(..)]` names the two halves
// separately, because they are not symmetric: `prost` owns the bytes on the way out, and reading
// has an envelope to step over first. The reply topic is a property of the confirmation itself,
// so the type declares it next to its encode half and the subscriber's clause names none.
#[derive(Clone, PartialEq, prost::Message, Deserialized)]
#[wire(decode = ruststream_rdkafka::protobuf::decode_confluent)]
struct Order {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    item: String,
}

#[derive(Clone, PartialEq, prost::Message, Serialized, Outgoing)]
#[wire(encode = ::prost::Message::encode)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(bool, tag = "2")]
    accepted: bool,
}
// --8<-- [end:types]

// --8<-- [start:handler]
// An ordinary function over ordinary types: nothing about the wire in the signature, and no
// publish call. The delivery arrived past its envelope, and the reply leaves before one.
#[subscriber("orders", publish)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: !order.item.is_empty(),
    }
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    // --8<-- [start:wiring]
    let registry = SchemaRegistry::new("http://localhost:8081");

    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
        |b| {
            // The framing is named where the reply's publisher is named. The message index
            // defaults to the schema's first top-level message, which Confluent optimises to a
            // single zero byte; `.message(topic, "pkg.Message")` pins another.
            b.include(confirm)
                .out(Reply, KafkaPublish::framed(&registry));
        },
    )
    // --8<-- [end:wiring]
}
