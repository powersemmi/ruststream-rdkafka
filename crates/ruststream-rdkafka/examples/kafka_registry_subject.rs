//! Naming the subject a topic publishes under: one subject per record type, shared by every topic
//! that carries it.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example kafka_registry_subject --features schema-registry,testing
//! ```

use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{SchemaFrame, SchemaRegistry, SubjectStrategy};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Return {
    order: i64,
    amount: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[outgoing(name = "refunds")]
struct Refund {
    order: i64,
    amount: i64,
}

#[subscriber("returns", reply)]
async fn reimburse(item: &Return) -> Refund {
    tracing::info!(item.order, item.amount, "refund issued");
    Refund {
        order: item.order,
        amount: item.amount,
    }
}

fn app(registry: SchemaRegistry) -> impl App {
    // `billing.Refund`: the record's own subject, whichever topic carries it.
    let refunds = SubjectStrategy::RecordName.subject("refunds", "billing.Refund");
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("refunds-svc")
        .schema_registry(registry.clone());
    RustStream::new(AppInfo::new("refunds", "0.1.0"))
        .publish_layer(SchemaFrame::new(registry).subject("refunds", refunds))
        .with_broker(broker, |b| {
            b.include(reimburse);
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
    use ruststream_rdkafka::SchemaRegistry;
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::parse_envelope;

    use super::{Refund, app, live};

    /// A return as its producer writes it: plain JSON.
    #[derive(Serialized, Outgoing)]
    struct Wire(Vec<u8>);

    #[tokio::test]
    async fn a_refund_is_framed_under_its_record_subject() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
            return Ok(());
        };
        let registry = SchemaRegistry::new(&url);
        let refunds = registry.register_json::<Refund>("billing.Refund").await?;
        let tb = TestApp::start(app(registry)).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(br#"{"order":42,"amount":1999}"#.to_vec()))
            .to("returns")
            .publish()
            .await?;

        let published = kafka.published::<()>("refunds").assert_called_once();
        let (id, datum) = parse_envelope(published.messages()[0].payload())
            .ok_or("the refund went out without an envelope")?;
        assert_eq!(id, refunds);
        assert_eq!(
            serde_json::from_slice::<Refund>(datum)?,
            Refund {
                order: 42,
                amount: 1999,
            },
        );
        Ok(())
    }
}
