//! The service of `examples/kafka_avro_local.rs` written without the macros.
//!
//! ```text
//! cargo test --example manual_kafka_avro_local --features avro,testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::avro::{AvroCodec, AvroSchema};
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, JsonSchema)]
struct Reading {
    sensor: String,
    celsius: f64,
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
        tracing::info!(reading.sensor, reading.celsius, "reading recorded");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    // The schema the wire carries, pinned here; nothing reaches a registry.
    let codec = AvroCodec::local(Reading::get_schema()).expect("a derived schema resolves");
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
    use ruststream_rdkafka::avro::{self, AvroCodec, AvroSchema};
    use ruststream_rdkafka::prelude::*;

    use super::{Reading, app};

    /// Bytes published as they are: what another Avro producer puts on the topic.
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
    async fn a_bare_datum_reaches_the_handler() -> Result<(), Box<dyn Error>> {
        let mut datum = BytesMut::new();
        avro::encode(
            &Reading {
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

        let codec = AvroCodec::local(Reading::get_schema())?;
        kafka
            .subscriber("readings")
            .assert_called_once()
            .with_codec(
                &codec,
                &Reading {
                    sensor: "boiler".to_owned(),
                    celsius: 71.5,
                },
            )
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
