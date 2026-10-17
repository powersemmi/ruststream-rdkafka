//! Confluent Schema Registry as broker middleware: handlers and codecs stay plain JSON, the
//! broker's edges transcode. Consuming strips the envelope of a framed payload; publishing frames
//! the JSON payload for the wire through the app's publish pipeline.
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_schema_registry --features schema-registry -- run
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example kafka_schema_registry --features schema-registry,testing
//! ```

use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{KafkaError, SchemaFrame, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Reservation {
    id: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[outgoing(name = "reservation-confirmations")]
struct Confirmation {
    id: i64,
    accepted: bool,
}

// An ordinary handler on the default JSON codec: the broker already took the envelope off.
#[subscriber("reservations", reply)]
async fn confirm(reservation: &Reservation) -> Confirmation {
    tracing::info!(reservation.id, "reservation confirmed");
    Confirmation {
        id: reservation.id,
        accepted: true,
    }
}

fn app(registry: SchemaRegistry) -> impl App {
    // Incoming: the broker transcodes framed payloads to plain JSON.
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("reservations-svc")
        .schema_registry(registry.clone());

    RustStream::new(AppInfo::new("reservations", "0.1.0"))
        // Outgoing: the layer frames each publish by its topic's subject, `{topic}-value`.
        .publish_layer(SchemaFrame::new(registry.clone()))
        .on_startup(async move |()| {
            registry
                .register_json::<Confirmation>("reservation-confirmations-value")
                .await?;
            Ok::<_, KafkaError>(())
        })
        .with_broker(broker, |b| {
            b.include(confirm);
        })
}

#[ruststream::app]
fn service() -> impl App {
    app(SchemaRegistry::new("http://localhost:8081"))
}
// --8<-- [end:app]

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::{JsonSchema, encode_envelope, parse_envelope};
    use ruststream_rdkafka::{SchemaRegistry, SchemaType};
    use serde::Serialize;

    use super::{Confirmation, Reservation, app, live};

    /// Bytes published as they are: what a registry-backed producer puts on the topic.
    #[derive(Serialized, Outgoing)]
    struct Wire(Vec<u8>);

    /// The producer's model of a reservation, which owns the `reservations-value` subject.
    #[derive(Serialize, JsonSchema)]
    #[serde(rename = "Reservation")]
    struct Produced {
        id: i64,
    }

    #[tokio::test]
    async fn a_framed_reservation_is_answered_with_a_framed_confirmation()
    -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
            return Ok(());
        };
        let registry = SchemaRegistry::new(&url);
        let reservations = registry
            .register_json::<Produced>("reservations-value")
            .await?;
        let tb = TestApp::start(app(registry.clone())).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(encode_envelope(reservations, br#"{"id":42}"#)))
            .to("reservations")
            .publish()
            .await?;

        kafka
            .subscriber("reservations")
            .assert_called_once()
            .with(&Reservation { id: 42 });
        let published = kafka
            .published::<()>("reservation-confirmations")
            .assert_called_once();
        let (id, datum) = parse_envelope(published.messages()[0].payload())
            .ok_or("the confirmation went out without an envelope")?;
        let confirmations = registry.warm("reservation-confirmations-value").await?;
        assert_eq!(confirmations.schema_type(), SchemaType::Json);
        assert_eq!(id, confirmations.id());
        assert_eq!(
            serde_json::from_slice::<Confirmation>(datum)?,
            Confirmation {
                id: 42,
                accepted: true,
            },
        );
        Ok(())
    }
}
