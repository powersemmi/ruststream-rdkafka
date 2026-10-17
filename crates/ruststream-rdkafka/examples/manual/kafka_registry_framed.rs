//! The service of `examples/kafka_registry_framed.rs` written without the macros.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example manual_kafka_registry_framed --features schema-registry,testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::codec::JsonCodec;
use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{SchemaFramed, SchemaPrefetch, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Account {
    id: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Statement {
    account: i64,
    balance: i64,
}

// `#[derive(Outgoing)]` with `#[outgoing(name = "statements")]`.
impl OutgoingDestination for Statement {
    type Form = FixedName;
    const DESTINATION: &'static str = "statements";
}

impl MessageHeaders for Statement {
    type Contract = NoHeaders;
}

/// The definition `#[subscriber("accounts", reply)]` would have minted.
struct Issue;

impl Handle<Account, Statement> for Issue {
    fn handle(
        &self,
        account: &Account,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Statement, HandlerOutcome>> {
        tracing::info!(account.id, "statement issued");
        ready(Ok(Statement {
            account: account.id,
            balance: 0,
        }))
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
        b.include(subscriber("accounts", Issue).reply().build())
            .out_reply(Publish::default())
            .codec(replies);
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
    use ruststream_rdkafka::SchemaRegistry;
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::{encode_envelope, parse_envelope};

    use super::{Statement, app, live};

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
