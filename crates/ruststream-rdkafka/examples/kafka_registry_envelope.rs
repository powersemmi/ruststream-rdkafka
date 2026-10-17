//! The Confluent envelope read and written by hand: a relay that moves records between two
//! registries, translating the schema id each record names and leaving the datum untouched.
//!
//! ```text
//! cargo test --example kafka_registry_envelope --features schema-registry,testing
//! ```

use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::{encode_envelope, parse_envelope};

// --8<-- [start:app]
/// A record as it arrived: a view of its bytes, envelope included.
#[derive(Deserialized)]
struct Framed<'a>(&'a [u8]);

/// The same datum under the id the new registry gave its schema.
#[derive(Serialized, Outgoing)]
#[outgoing(name = "orders-migrated")]
struct Reframed(Vec<u8>);

/// Each schema id of the old registry, and the id the new registry gave the same schema.
const MIGRATED_IDS: &[(u32, u32)] = &[(7, 107), (8, 108)];

#[subscriber("orders", reply)]
async fn reframe(record: &Framed<'_>) -> Result<Reframed, HandlerOutcome> {
    let Some((old, datum)) = parse_envelope(record.0) else {
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

#[ruststream::app]
fn app() -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("registry-migration");
    RustStream::new(AppInfo::new("registry-migration", "0.1.0")).with_broker(broker, |b| {
        b.include(reframe);
    })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::encode_envelope;

    use super::app;

    /// Bytes published as they are: what a registry-backed producer puts on the topic.
    #[derive(Serialized, Outgoing)]
    struct Wire(Vec<u8>);

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
