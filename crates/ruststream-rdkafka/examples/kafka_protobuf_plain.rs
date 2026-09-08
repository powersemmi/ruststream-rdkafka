//! Protobuf with nothing manual in the handler: a generated message arrives as itself and
//! leaves as itself, and the Confluent envelope is put on and taken off around the body.
//!
//! Two halves make that work, and they are asymmetric on purpose. Reading needs no registry at
//! all - Protobuf is tag-addressed, so a delivery decodes against the reader's own type - so the
//! type names this crate's `decode_confluent` on its decode lane and the envelope never reaches
//! the signature. Writing does need one number, the subject's schema id, and a value cannot
//! fetch it: `Serialized::wire_bytes` is synchronous and has only `&self`. The publish path can,
//! because it knows the destination topic, so the value writes bare Protobuf through `prost` and
//! `ProtobufFrame` puts the id and the message-index path in front of it.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_protobuf_plain --features protobuf -- run
//! ```

use ruststream::prelude::*;
use ruststream::runtime::{App, AppInfo, DefaultSlot, RustStream};
use ruststream_rdkafka::{KafkaBroker, KafkaPublish, ProtobufFrame, SchemaRegistry};

// --8<-- [start:types]
// What `prost-build` emits, plus the two lane derives. `#[wire(..)]` names the two halves
// separately, because they are not symmetric: `prost` owns the bytes on the way out, and reading
// has an envelope to step over first.
#[derive(Clone, PartialEq, prost::Message, Deserialized, Serialized)]
#[wire(
    encode = ::prost::Message::encode,
    decode = ruststream_rdkafka::protobuf::decode_confluent
)]
struct Order {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    item: String,
}

// The reply also declares a destination, because it leaves through a slot rather than a
// `publish(..)` clause - see the mount below for why.
#[derive(Clone, PartialEq, prost::Message, Serialized, Outgoing)]
#[wire(encode = ::prost::Message::encode)]
struct Confirmation {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(bool, tag = "2")]
    accepted: bool,
}
// --8<-- [end:types]

// --8<-- [start:handler]
// An ordinary function over ordinary types: no `IncomingFrame`, no `decode_framed`, no
// `Subject::frame`. The delivery arrived past its envelope and the reply leaves before one.
#[subscriber("orders")]
async fn confirm(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let sent = out
        .message(&Confirmation {
            id: order.id,
            accepted: !order.item.is_empty(),
        })
        .to("confirmations")
        .publish()
        .await;
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    // --8<-- [start:wiring]
    let sr = SchemaRegistry::new("http://localhost:8081");

    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // Frames every publish whose destination topic has a Protobuf subject, and leaves the
        // rest alone - so an Avro or JSON topic in the same app is untouched. The message index
        // defaults to the schema's first top-level message; `.message(topic, "pkg.Message")`
        // pins another.
        .publish_layer(ProtobufFrame::new(sr))
        .with_broker(
            KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
            |b| {
                // The reply goes out through a slot, not a `publish(..)` clause: the core routes a
                // byte-for-byte reply straight to its publisher, so a publish layer never sees it.
                b.include(confirm)
                    .out(DefaultSlot, KafkaPublish::default())
                    .build();
            },
        )
    // --8<-- [end:wiring]
}
