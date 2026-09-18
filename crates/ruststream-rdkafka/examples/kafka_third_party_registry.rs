//! Bringing your own Schema Registry stack: `schema_registry_converter` (or
//! `schema-registry-client`, or a hand-rolled one) doing the schema work, and this crate carrying
//! the result.
//!
//! # Why this works when depending on those crates would not
//!
//! Both of them pin their own `apache-avro` and `prost`, and those pins do not always match this
//! crate's - which is why neither is a dependency here. It does not matter on this path, because
//! **nothing but bytes crosses the boundary**. The converter turns a value into a
//! Confluent-framed `Vec<u8>` and back; ruststream's byte lanes carry that `Vec<u8>` to and from
//! the topic without looking inside it. A version skew becomes a type mismatch only where a type
//! crosses, and here none does: `apache_avro::types::Value` in the handler below belongs to the
//! converter's `apache-avro`, and is converted with the converter's, never with ours.
//!
//! The same shape works for any other client, including a binding to a non-Rust one: produce
//! bytes, hand them over.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_third_party_registry --features avro -- run
//! ```

use std::sync::Arc;

use apache_avro::types::Value as AvroValue;
use ruststream::prelude::*;
use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream_rdkafka::KafkaBroker;
use schema_registry_converter::async_impl::avro::{AvroDecoder, AvroEncoder};
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;

// --8<-- [start:lanes]
/// The delivery, as it arrived. This is the whole of ruststream's involvement with the wire
/// format: a view of the bytes, handed straight to the converter.
#[derive(Deserialized)]
struct Framed<'a>(&'a [u8]);

/// What the converter produced, on its way out. `#[derive(Serialized)]` over a byte buffer lends
/// the bytes as they are, so nothing re-frames or re-encodes what the converter already wrote.
#[derive(Serialized, Outgoing)]
#[outgoing(name = "confirmations")]
struct Encoded(Vec<u8>);
// --8<-- [end:lanes]

// --8<-- [start:state]
/// The user's own registry stack, injected like any other application state.
#[derive(Clone)]
struct Registry {
    decoder: Arc<AvroDecoder<'static>>,
    encoder: Arc<AvroEncoder<'static>>,
}

#[derive(FromRef)]
struct Orders {
    registry: Registry,
}
// --8<-- [end:state]

// --8<-- [start:handler]
#[subscriber("orders", publish("confirmations"))]
async fn relay(
    framed: &Framed<'_>,
    State(registry): State<Registry>,
) -> Result<Encoded, HandlerOutcome> {
    // The converter resolves the schema the envelope names, through its own registry client and
    // its own cache. It is async, and a handler is the right place for that.
    let decoded = registry
        .decoder
        .decode(Some(framed.0))
        .await
        .map_err(|_| HandlerOutcome::drop())?;

    // `decoded.value` is the converter's `apache_avro::types::Value`, not ours. It never crosses
    // into ruststream, so which `apache-avro` built it is the converter's business alone - read
    // it with the one your converter depends on.
    let id = match &decoded.value {
        AvroValue::Record(fields) => fields
            .iter()
            .find(|(name, _)| name == "id")
            .and_then(|(_, value)| match value {
                AvroValue::Long(id) => Some(*id),
                _ => None,
            })
            .unwrap_or_default(),
        _ => return Err(HandlerOutcome::drop()),
    };

    let bytes = registry
        .encoder
        .encode_struct(
            Confirmation { id, accepted: true },
            &SubjectNameStrategy::TopicNameStrategy("confirmations".to_owned(), false),
        )
        .await
        .map_err(|_| HandlerOutcome::drop())?;
    Ok(Encoded(bytes))
}

#[derive(serde::Serialize)]
struct Confirmation {
    id: i64,
    accepted: bool,
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    // No `schema_registry(..)` and no `schema_prefetch(..)`: this service's schemas are the
    // converter's business, so the broker is a plain one.
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("orders-svc");

    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| {
            let settings = SrSettings::new("http://localhost:8081".to_owned());
            Ok::<_, std::io::Error>(Orders {
                registry: Registry {
                    decoder: Arc::new(AvroDecoder::new(settings.clone())),
                    encoder: Arc::new(AvroEncoder::new(settings)),
                },
            })
        })
        .with_broker(broker, |b| {
            b.include(relay);
        })
}
// --8<-- [end:app]
