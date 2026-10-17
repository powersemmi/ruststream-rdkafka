//! The service of `examples/kafka_registry_client.rs` written without the macros.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example manual_kafka_registry_client --features schema-registry,testing
//! ```

use std::convert::Infallible;
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::{SchemaRegistry, parse_envelope};

// --8<-- [start:app]
/// A record as it arrived, envelope included.
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

/// The application state the handler reads.
struct Audit {
    registry: SchemaRegistry,
}

/// The definition `#[subscriber("orders")]` would have minted; the state axis names `Audit`.
struct AuditRecord;

impl Handle<Framed<'_>, (), (), (), Audit> for AuditRecord {
    async fn handle(
        &self,
        record: &Framed<'_>,
        _outs: &(),
        ctx: &mut Context<'_, (), Audit>,
    ) -> Result<(), HandlerOutcome> {
        let Some((id, _)) = parse_envelope(record.0) else {
            tracing::info!(topic = "orders", "a record without a schema");
            return Ok(());
        };
        // The first lookup of an id reaches the registry; every later one reads the cache.
        match ctx.state().registry.schema_by_id(id).await {
            Ok(schema) => {
                tracing::info!(schema_id = id, schema_type = ?schema.schema_type(), "written with");
                Ok(())
            }
            Err(error) => {
                tracing::warn!(topic = "orders", schema_id = id, %error, "schema lookup failed");
                Err(HandlerOutcome::retry())
            }
        }
    }
}

fn registry(url: &str, password: &str) -> SchemaRegistry {
    // Records the address and the settings; no request is sent until the first lookup.
    SchemaRegistry::new(url)
        .basic_auth("audit-svc", password)
        .request_timeout(Duration::from_secs(2))
}

fn app(registry: SchemaRegistry) -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"]).default_group("audit-svc");
    RustStream::new(AppInfo::new("audit", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Audit { registry }))
        .with_broker(broker, |b| {
            b.include(subscriber("orders", AuditRecord).build());
        })
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    let password = std::env::var("REGISTRY_PASSWORD").unwrap_or_default();
    run_main(|| app(registry("http://localhost:8081", &password)))
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
    use ruststream_rdkafka::schema_registry::{JsonSchema, SchemaRegistry, encode_envelope};
    use serde::Serialize;

    use super::{app, live, registry};

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

    #[derive(Serialize, JsonSchema)]
    #[serde(rename = "AuditedOrder")]
    struct Produced {
        id: i64,
    }

    #[tokio::test]
    async fn a_lookup_lands_in_the_cache_every_clone_shares() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
            return Ok(());
        };
        let id = SchemaRegistry::new(&url)
            .register_json::<Produced>("audited-orders-value")
            .await?;
        let client = registry(&url, "unused");
        let tb = TestApp::start(app(client.clone())).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(encode_envelope(id, br#"{"id":42}"#)))
            .to("orders")
            .publish()
            .await?;

        kafka
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let cached = client
            .cached_schema(id)
            .ok_or("the handler's lookup is not in this clone's cache")?;
        assert_eq!(cached.id(), id);
        Ok(())
    }

    #[tokio::test]
    async fn the_app_starts_with_no_registry_reachable() -> Result<(), Box<dyn Error>> {
        // Nothing listens on port 9: building the client and starting the app send nothing.
        let tb = TestApp::start(app(registry("http://127.0.0.1:9", "unused"))).await?;
        tb.assert_running();
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(br#"{"id":42}"#.to_vec()))
            .to("orders")
            .publish()
            .await?;

        kafka
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
