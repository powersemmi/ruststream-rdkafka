//! The service of `examples/kafka_third_party_registry.rs` written without the macros.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example manual_kafka_third_party_registry --features avro,testing
//! ```

use std::convert::Infallible;
use std::process::ExitCode;
use std::sync::Arc;

use apache_avro::types::Value as AvroValue;
use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::prelude::*;
use schema_registry_converter::async_impl::avro::{AvroDecoder, AvroEncoder};
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde::Serialize;

// --8<-- [start:app]
/// The delivery as it arrived: a view of the bytes, handed straight to the converter.
struct Framed<'a>(&'a [u8]);

// `#[derive(Deserialized)]` over a borrowed byte slice: the view is the payload itself.
impl Deserialized for Framed<'_> {
    type Output<'a> = Framed<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Framed<'_>, Infallible> {
        Ok(Framed(payload))
    }
}

impl Input for Framed<'_> {
    type Axis = SoloDeserialized<Framed<'static>>;
}

/// What the converter produced; the lane sends the bytes as they are.
struct Encoded(Vec<u8>);

// `#[derive(Serialized)]`: the bytes are the payload, so no codec runs on them.
impl Serialized for Encoded {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(&self.0))
    }
}

impl MessageWire for Encoded {
    type Wire = SerializedWire;
}

impl ReplyShape for Encoded {
    type Body = Self;
    type Headers = ();
    type Wire = SerializedReply;
}

// `#[derive(Outgoing)]` with `#[outgoing(name = "rental-confirmations")]`.
impl OutgoingDestination for Encoded {
    type Form = FixedName;
    const DESTINATION: &'static str = "rental-confirmations";
}

impl MessageHeaders for Encoded {
    type Contract = NoHeaders;
}

#[derive(Serialize)]
struct Confirmation {
    id: i64,
    accepted: bool,
}

/// The service's own registry stack, held as application state.
#[derive(Clone)]
struct Converter {
    decoder: Arc<AvroDecoder<'static>>,
    encoder: Arc<AvroEncoder<'static>>,
}

/// The application state; the handler's state axis names it.
struct Rentals {
    converter: Converter,
}

/// The definition `#[subscriber("rentals", reply)]` would have minted.
struct Confirm;

impl Handle<Framed<'_>, Encoded, (), (), Rentals> for Confirm {
    async fn handle(
        &self,
        framed: &Framed<'_>,
        _outs: &(),
        ctx: &mut Context<'_, (), Rentals>,
    ) -> Result<Encoded, HandlerOutcome> {
        let converter = &ctx.state().converter;
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
            b.include(subscriber("rentals", Confirm).reply().build());
        })
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(|| app("http://localhost:8081".to_owned()))
}
// --8<-- [end:app]

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::SchemaRegistry;
    use ruststream_rdkafka::avro::{self, AvroSchema};
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::{encode_envelope, parse_envelope};
    use serde::{Deserialize, Serialize};

    use super::{app, live};

    /// Bytes published as they are: what a registry-backed producer puts on the topic.
    struct Wire(Vec<u8>);

    impl Serialized for Wire {
        type Error = Infallible;

        fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
            Ok(WireBytes::Own(&self.0))
        }
    }

    impl MessageWire for Wire {
        type Wire = SerializedWire;
    }

    impl OutgoingDestination for Wire {
        type Form = CallerName;
    }

    impl MessageHeaders for Wire {
        type Contract = NoHeaders;
    }

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
