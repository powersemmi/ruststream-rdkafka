//! Confluent-framed JSON as a codec: the core's JSON codec inside the envelope of a registered
//! subject.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example kafka_registry_framed --features schema-registry,testing
//! ```

use ruststream::codec::JsonCodec;
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{SchemaFramed, SchemaPrefetch, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Account {
    id: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema, Outgoing)]
#[outgoing(name = "statements")]
struct Statement {
    account: i64,
    balance: i64,
}

#[subscriber("accounts", reply)]
async fn issue(account: &Account) -> Statement {
    tracing::info!(account.id, "statement issued");
    Statement {
        account: account.id,
        balance: 0,
    }
}

fn app(registry: SchemaRegistry) -> impl App {
    let prefetch = SchemaPrefetch::new(registry);
    // Decoding strips the envelope; encoding frames with the id of the type's subject.
    let codec = SchemaFramed::new(&prefetch, JsonCodec).register::<Statement>("statements-value");
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("statements-svc")
        .schema_prefetch(prefetch);

    let replies = codec.clone();
    RustStream::new(AppInfo::new("statements", "0.1.0")).with_broker_codec(broker, codec, |b| {
        b.include(issue)
            .out_reply(Publish::default())
            .codec(replies);
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
    use ruststream_rdkafka::schema_registry::{encode_envelope, parse_envelope};

    use super::{Statement, app, live};

    /// Bytes published as they are: what a registry-backed producer puts on the topic.
    #[derive(Serialized, Outgoing)]
    struct Wire(Vec<u8>);

    #[tokio::test]
    async fn a_framed_account_is_answered_with_a_framed_statement() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
            return Ok(());
        };
        let registry = SchemaRegistry::new(&url);
        let statements = registry
            .register_json::<Statement>("statements-value")
            .await?;
        let tb = TestApp::start(app(registry)).await?;
        let kafka = tb.broker::<KafkaBroker>();

        // Decoding reads no schema, so the id the producer framed with is never looked up.
        kafka
            .message(&Wire(encode_envelope(1, br#"{"id":42}"#)))
            .to("accounts")
            .publish()
            .await?;

        let published = kafka.published::<()>("statements").assert_called_once();
        let (id, datum) = parse_envelope(published.messages()[0].payload())
            .ok_or("the statement went out without an envelope")?;
        assert_eq!(id, statements);
        assert_eq!(
            serde_json::from_slice::<Statement>(datum)?,
            Statement {
                account: 42,
                balance: 0,
            },
        );
        Ok(())
    }
}
