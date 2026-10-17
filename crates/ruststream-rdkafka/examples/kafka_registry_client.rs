//! The registry client as application state: an audit service that logs which schema each
//! record was written with.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example kafka_registry_client --features schema-registry,testing
//! ```

use std::convert::Infallible;
use std::time::Duration;

use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::{SchemaRegistry, parse_envelope};

// --8<-- [start:app]
/// A record as it arrived, envelope included.
#[derive(Deserialized)]
struct Framed<'a>(&'a [u8]);

#[derive(FromRef)]
struct Audit {
    registry: SchemaRegistry,
}

#[subscriber("orders")]
async fn audit(record: &Framed<'_>, State(registry): State<SchemaRegistry>) -> HandlerOutcome {
    let Some((id, _)) = parse_envelope(record.0) else {
        tracing::info!(topic = "orders", "a record without a schema");
        return HandlerOutcome::ack();
    };
    // The first lookup of an id reaches the registry; every later one reads the cache.
    match registry.schema_by_id(id).await {
        Ok(schema) => {
            tracing::info!(schema_id = id, schema_type = ?schema.schema_type(), "written with");
            HandlerOutcome::ack()
        }
        Err(error) => {
            tracing::warn!(topic = "orders", schema_id = id, %error, "schema lookup failed");
            HandlerOutcome::retry()
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
            b.include(audit);
        })
}

#[ruststream::app]
fn service() -> impl App {
    let password = std::env::var("REGISTRY_PASSWORD").unwrap_or_default();
    app(registry("http://localhost:8081", &password))
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
    use ruststream_rdkafka::schema_registry::{JsonSchema, SchemaRegistry, encode_envelope};
    use serde::Serialize;

    use super::{app, live, registry};

    /// Bytes published as they are: what a registry-backed producer puts on the topic.
    #[derive(Serialized, Outgoing)]
    struct Wire(Vec<u8>);

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
