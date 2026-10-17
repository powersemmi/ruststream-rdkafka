//! Avro with no registry: the codec pins one schema, and the wire carries a bare datum.
//!
//! ```text
//! cargo test --example kafka_avro_local --features avro,testing
//! ```

use ruststream_rdkafka::avro::{AvroCodec, AvroSchema};
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
struct Reading {
    sensor: String,
    celsius: f64,
}

#[subscriber("readings")]
async fn record(reading: &Reading) -> HandlerOutcome {
    tracing::info!(reading.sensor, reading.celsius, "reading recorded");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    // The schema the wire carries, pinned here; nothing reaches a registry.
    let codec = AvroCodec::local(Reading::get_schema()).expect("a derived schema resolves");
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("readings-svc");
    RustStream::new(AppInfo::new("readings", "0.1.0")).with_broker_codec(broker, codec, |b| {
        b.include(record);
    })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::avro::{self, AvroCodec, AvroSchema};
    use ruststream_rdkafka::prelude::*;

    use super::{Reading, app};

    /// Bytes published as they are: what another Avro producer puts on the topic.
    #[derive(Serialized, Outgoing)]
    struct Wire(Vec<u8>);

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
