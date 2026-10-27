//! Bringing your own Schema Registry stack: `schema_registry_converter` doing the schema work,
//! and this crate carrying the bytes it produces.
//!
//! Nothing but bytes crosses between the two: the converter turns a value into a
//! Confluent-framed `Vec<u8>` and back, and the byte lanes carry that buffer to and from the topic
//! without looking inside it. The converter's own `apache-avro` and `prost` versions therefore
//! never meet this crate's types.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_third_party_registry --features avro -- run
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example kafka_third_party_registry --features avro,testing
//! ```

use std::convert::Infallible;
use std::sync::Arc;

use apache_avro::types::Value as AvroValue;
use ruststream_rdkafka::prelude::*;
use schema_registry_converter::async_impl::avro::{AvroDecoder, AvroEncoder};
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde::Serialize;

// --8<-- [start:app]
/// The delivery as it arrived: a view of the bytes, handed straight to the converter.
#[derive(Deserialized)]
struct Framed<'a>(&'a [u8]);

/// What the converter produced; the lane sends the bytes as they are.
#[derive(Serialized, Outgoing)]
#[outgoing(name = "rental-confirmations")]
struct Encoded(Vec<u8>);

#[derive(Serialize)]
struct Confirmation {
    id: i64,
    accepted: bool,
}

/// The service's own registry stack, held as application state.
#[derive(Clone)]
struct Converter {
    decoder: Arc<AvroDecoder>,
    encoder: Arc<AvroEncoder>,
}

#[derive(FromRef)]
struct Rentals {
    converter: Converter,
}

#[subscriber("rentals", reply)]
async fn confirm(
    framed: &Framed<'_>,
    State(converter): State<Converter>,
) -> Result<Encoded, HandlerOutcome> {
    // The converter resolves the schema the envelope names through its own client and cache.
    let decoded = converter
        .decoder
        .decode(Some(framed.0))
        .await
        .map_err(|error| {
            tracing::warn!(topic = "rentals", %error, "the converter could not decode a record");
            HandlerOutcome::drop()
        })?;
    let AvroValue::Record(fields) = &decoded.value else {
        return Err(HandlerOutcome::drop());
    };
    let id = fields
        .iter()
        .find_map(|(name, value)| match (name.as_str(), value) {
            ("id", AvroValue::Long(id)) => Some(*id),
            _ => None,
        })
        .ok_or(HandlerOutcome::drop())?;
    tracing::info!(id, "rental confirmed");

    let bytes = converter
        .encoder
        .encode_struct(
            Confirmation { id, accepted: true },
            &SubjectNameStrategy::TopicNameStrategy("rental-confirmations".to_owned(), false),
        )
        .await
        .map_err(|error| {
            tracing::warn!(topic = "rental-confirmations", %error, "the converter could not encode");
            HandlerOutcome::retry()
        })?;
    Ok(Encoded(bytes))
}

fn app(registry_url: String) -> impl App {
    // A plain broker: the schemas are the converter's business.
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("rentals-svc");
    RustStream::new(AppInfo::new("rentals", "0.1.0"))
        .on_startup(async move |()| {
            let settings = SrSettings::new(registry_url);
            Ok::<_, Infallible>(Rentals {
                converter: Converter {
                    decoder: Arc::new(AvroDecoder::new(settings.clone())),
                    encoder: Arc::new(AvroEncoder::new(settings)),
                },
            })
        })
        .with_broker(broker, |b| {
            b.include(confirm);
        })
}

#[ruststream::app]
fn service() -> impl App {
    app("http://localhost:8081".to_owned())
}
// --8<-- [end:app]

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::SchemaRegistry;
    use ruststream_rdkafka::avro::{self, AvroSchema};
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::{encode_envelope, parse_envelope};
    use serde::{Deserialize, Serialize};

    use super::{app, live};

    /// Bytes published as they are: what a registry-backed producer puts on the topic.
    #[derive(Serialized, Outgoing)]
    struct Wire(Vec<u8>);

    /// The producer's model of a rental, which owns the `rentals-value` subject.
    #[derive(Serialize, AvroSchema)]
    #[serde(rename = "Rental")]
    struct Rental {
        id: i64,
    }

    /// The model the confirmations subject holds.
    #[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
    #[serde(rename = "Confirmation")]
    struct Confirmed {
        id: i64,
        accepted: bool,
    }

    #[tokio::test]
    async fn the_converter_reads_and_writes_the_framed_records() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
            return Ok(());
        };
        let registry = SchemaRegistry::new(&url);
        let rentals = registry.register_avro::<Rental>("rentals-value").await?;
        let confirmations = registry
            .register_avro::<Confirmed>("rental-confirmations-value")
            .await?;
        let mut datum = BytesMut::new();
        avro::encode(&Rental { id: 42 }, &mut datum)?;
        let tb = TestApp::start(app(url)).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(encode_envelope(rentals, &datum)))
            .to("rentals")
            .publish()
            .await?;

        let published = kafka
            .published::<()>("rental-confirmations")
            .assert_called_once();
        let (id, datum) = parse_envelope(published.messages()[0].payload())
            .ok_or("the confirmation went out without an envelope")?;
        assert_eq!(id, confirmations);
        assert_eq!(
            avro::decode::<Confirmed>(datum)?,
            Confirmed {
                id: 42,
                accepted: true,
            },
        );
        Ok(())
    }
}
