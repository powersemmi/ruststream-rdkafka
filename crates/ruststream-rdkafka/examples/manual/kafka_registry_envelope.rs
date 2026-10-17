//! The service of `examples/kafka_registry_envelope.rs` written without the macros: the byte
//! lanes and the handler spelled out as the impls the derives and the attribute would emit.
//!
//! ```text
//! cargo test --example manual_kafka_registry_envelope --features schema-registry,testing
//! ```

use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::{encode_envelope, parse_envelope};

// --8<-- [start:app]
/// A record as it arrived: a view of its bytes, envelope included.
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

/// The same datum under the id the new registry gave its schema.
struct Reframed(Vec<u8>);

// `#[derive(Serialized)]`: the bytes are the payload, so no codec runs on them.
impl Serialized for Reframed {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(&self.0))
    }
}

impl MessageWire for Reframed {
    type Wire = SerializedWire;
}

impl ReplyShape for Reframed {
    type Body = Self;
    type Headers = ();
    type Wire = SerializedReply;
}

// `#[derive(Outgoing)]` with `#[outgoing(name = "orders-migrated")]`.
impl OutgoingDestination for Reframed {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders-migrated";
}

impl MessageHeaders for Reframed {
    type Contract = NoHeaders;
}

/// Each schema id of the old registry, and the id the new registry gave the same schema.
const MIGRATED_IDS: &[(u32, u32)] = &[(7, 107), (8, 108)];

/// The definition `#[subscriber("orders", reply)]` would have minted.
struct Reframe;

impl Handle<Framed<'_>, Reframed> for Reframe {
    fn handle(
        &self,
        record: &Framed<'_>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Reframed, HandlerOutcome>> {
        ready(reframe(record.0))
    }
}

/// The body of the handler: it awaits nothing, so the future above is ready at once.
fn reframe(record: &[u8]) -> Result<Reframed, HandlerOutcome> {
    let Some((old, datum)) = parse_envelope(record) else {
        tracing::warn!(topic = "orders", "a record without the Confluent envelope");
        return Err(HandlerOutcome::drop());
    };
    let Some(&(_, new)) = MIGRATED_IDS.iter().find(|(from, _)| *from == old) else {
        tracing::warn!(
            topic = "orders",
            schema_id = old,
            "a schema id with no migration"
        );
        return Err(HandlerOutcome::drop());
    };
    tracing::info!(old, new, "record reframed");
    Ok(Reframed(encode_envelope(new, datum)))
}

fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("registry-migration");
    RustStream::new(AppInfo::new("registry-migration", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("orders", Reframe).reply().build());
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
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::encode_envelope;

    use super::app;

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
    async fn a_record_moves_under_its_new_schema_id() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(encode_envelope(7, br#"{"id":42}"#)))
            .to("orders")
            .publish()
            .await?;

        kafka
            .published::<()>("orders-migrated")
            .assert_called_once()
            .with_raw(&encode_envelope(107, br#"{"id":42}"#));
        Ok(())
    }

    #[tokio::test]
    async fn a_record_without_an_envelope_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(br#"{"id":42}"#.to_vec()))
            .to("orders")
            .publish()
            .await?;

        kafka
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        kafka.published::<()>("orders-migrated").assert_not_called();
        Ok(())
    }
}
