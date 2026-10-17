//! The service of `examples/kafka_avro_resolve.rs` written without the macros.
//!
//! ```text
//! cargo test --example manual_kafka_avro_resolve --features avro,testing
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::process::ExitCode;

use apache_avro::Schema;
use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::avro::{AvroCodec, AvroSchema};
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
/// The schema the producers write, as the topic's owners publish it.
const WRITTEN: &str = r#"{"type": "record", "name": "Reading", "fields": [
    {"name": "sensor", "type": "string"},
    {"name": "celsius", "type": "double"}
]}"#;

/// This service's model: one field newer than what the producers write.
#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, JsonSchema)]
struct Reading {
    sensor: String,
    celsius: f64,
    #[avro(default = r#""unknown""#)]
    site: String,
}

/// The definition `#[subscriber("readings")]` would have minted.
struct Record;

impl Handle<Reading> for Record {
    fn handle(
        &self,
        reading: &Reading,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(
            reading.sensor,
            reading.site,
            reading.celsius,
            "reading recorded"
        );
        ready(Ok(()))
    }
}

fn codec() -> Result<AvroCodec, Box<dyn Error>> {
    let written = Schema::parse_str(WRITTEN)?;
    Ok(AvroCodec::local(written)?.resolve_onto(Reading::get_schema())?)
}

fn app() -> impl App {
    let codec = codec().expect("the model resolves against the written schema");
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("readings-svc");
    RustStream::new(AppInfo::new("readings", "0.1.0")).with_broker_codec(broker, codec, |b| {
        b.include(subscriber("readings", Record).build());
    })
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::avro::{self, AvroSchema};
    use ruststream_rdkafka::prelude::*;
    use serde::Serialize;

    use super::{Reading, app, codec};

    /// What the producers write: the record before `site` existed.
    #[derive(Serialize, AvroSchema)]
    #[serde(rename = "Reading")]
    struct Written {
        sensor: String,
        celsius: f64,
    }

    /// Bytes published as they are: what a producer puts on the topic.
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

    #[tokio::test]
    async fn a_field_the_writer_never_wrote_takes_its_default() -> Result<(), Box<dyn Error>> {
        let mut datum = BytesMut::new();
        avro::encode(
            &Written {
                sensor: "boiler".to_owned(),
                celsius: 71.5,
            },
            &mut datum,
        )?;
        let tb = TestApp::start(app()).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(datum.to_vec()))
            .to("readings")
            .publish()
            .await?;

        kafka
            .subscriber("readings")
            .assert_called_once()
            .with_codec(
                &codec()?,
                &Reading {
                    sensor: "boiler".to_owned(),
                    celsius: 71.5,
                    site: "unknown".to_owned(),
                },
            )
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
