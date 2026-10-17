//! The service of `examples/kafka_schema_registry.rs` written without the macros.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example manual_kafka_schema_registry --features schema-registry,testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{KafkaError, SchemaFrame, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Reservation {
    id: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Confirmation {
    id: i64,
    accepted: bool,
}

// `#[derive(Outgoing)]` with `#[outgoing(name = "reservation-confirmations")]`.
impl OutgoingDestination for Confirmation {
    type Form = FixedName;
    const DESTINATION: &'static str = "reservation-confirmations";
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

/// The definition `#[subscriber("reservations", reply)]` would have minted.
struct Confirm;

impl Handle<Reservation, Confirmation> for Confirm {
    // An ordinary handler on the default JSON codec: the broker already took the envelope off.
    fn handle(
        &self,
        reservation: &Reservation,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        tracing::info!(reservation.id, "reservation confirmed");
        ready(Ok(Confirmation {
            id: reservation.id,
            accepted: true,
        }))
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
            b.include(subscriber("reservations", Confirm).reply().build());
        })
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(|| app(SchemaRegistry::new("http://localhost:8081")))
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
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::{encode_envelope, parse_envelope};
    use ruststream_rdkafka::{SchemaRegistry, SchemaType};

    use super::{Confirmation, Reservation, app, live};

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

    #[tokio::test]
    async fn a_framed_reservation_is_answered_with_a_framed_confirmation()
    -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
            return Ok(());
        };
        let registry = SchemaRegistry::new(&url);
        // The producer owns the subject of the topic this service reads.
        let reservations = registry
            .register_json::<Reservation>("reservations-value")
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
