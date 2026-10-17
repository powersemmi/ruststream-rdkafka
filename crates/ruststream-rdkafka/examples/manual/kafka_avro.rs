//! The service of `examples/kafka_avro.rs` written without the macros.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example manual_kafka_avro --features avro,testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::avro::AvroSchema;
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{KafkaError, SchemaFrame, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Ticket {
    id: i64,
    seat: String,
}

// `AvroSchema` gives the type the schema its subject is registered with.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema, AvroSchema)]
struct Confirmation {
    id: i64,
    accepted: bool,
}

// `#[derive(Outgoing)]` with `#[outgoing(name = "ticket-confirmations")]`.
impl OutgoingDestination for Confirmation {
    type Form = FixedName;
    const DESTINATION: &'static str = "ticket-confirmations";
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

/// The definition `#[subscriber("tickets", reply)]` would have minted.
struct Confirm;

impl Handle<Ticket, Confirmation> for Confirm {
    // An ordinary handler on the default JSON codec: the broker turned the Avro datum into JSON.
    fn handle(
        &self,
        ticket: &Ticket,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        tracing::info!(ticket.id, ticket.seat, "ticket confirmed");
        ready(Ok(Confirmation {
            id: ticket.id,
            accepted: true,
        }))
    }
}

fn app(registry: SchemaRegistry) -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("tickets-svc")
        .schema_registry(registry.clone());

    RustStream::new(AppInfo::new("tickets", "0.1.0"))
        // The reply's subject holds an Avro schema, so the layer writes the reply as Avro.
        .publish_layer(SchemaFrame::new(registry.clone()))
        .on_startup(async move |()| {
            registry
                .register_avro::<Confirmation>("ticket-confirmations-value")
                .await?;
            Ok::<_, KafkaError>(())
        })
        .with_broker(broker, |b| {
            b.include(subscriber("tickets", Confirm).reply().build());
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
    use ruststream_rdkafka::avro::{self, AvroSchema};
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::{encode_envelope, parse_envelope};
    use ruststream_rdkafka::{SchemaRegistry, SchemaType};

    use serde::Serialize;

    use super::{Confirmation, Ticket, app, live};

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

    /// The producer's model of a ticket, which owns the `tickets-value` subject.
    #[derive(Serialize, AvroSchema)]
    #[serde(rename = "Ticket")]
    struct Produced {
        id: i64,
        seat: String,
    }

    #[tokio::test]
    async fn an_avro_ticket_is_answered_with_an_avro_confirmation() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
            return Ok(());
        };
        let registry = SchemaRegistry::new(&url);
        let tickets = registry.register_avro::<Produced>("tickets-value").await?;
        let mut datum = BytesMut::new();
        avro::encode(
            &Produced {
                id: 42,
                seat: "12C".to_owned(),
            },
            &mut datum,
        )?;
        let tb = TestApp::start(app(registry.clone())).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(encode_envelope(tickets, &datum)))
            .to("tickets")
            .publish()
            .await?;

        kafka
            .subscriber("tickets")
            .assert_called_once()
            .with(&Ticket {
                id: 42,
                seat: "12C".to_owned(),
            });
        let published = kafka
            .published::<()>("ticket-confirmations")
            .assert_called_once();
        let (id, datum) = parse_envelope(published.messages()[0].payload())
            .ok_or("the confirmation went out without an envelope")?;
        let confirmations = registry.warm("ticket-confirmations-value").await?;
        assert_eq!(confirmations.schema_type(), SchemaType::Avro);
        assert_eq!(id, confirmations.id());
        assert_eq!(
            avro::decode::<Confirmation>(datum)?,
            Confirmation {
                id: 42,
                accepted: true,
            },
        );
        Ok(())
    }
}
