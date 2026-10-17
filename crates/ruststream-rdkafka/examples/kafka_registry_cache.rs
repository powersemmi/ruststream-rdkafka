//! What the registry client remembers: ids up to a bound, a subject's latest version for a time.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example kafka_registry_cache --features schema-registry,testing
//! ```

use std::time::Duration;

use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::{SchemaCachePolicy, SchemaFrame, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Purchase {
    id: i64,
    total: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    purchase: i64,
    total: i64,
}

#[subscriber("purchases", reply)]
async fn acknowledge(purchase: &Purchase) -> Receipt {
    tracing::info!(purchase.id, purchase.total, "receipt issued");
    Receipt {
        purchase: purchase.id,
        total: purchase.total,
    }
}

fn registry(url: &str) -> SchemaRegistry {
    // Up to 256 schemas by id; a subject's latest version is looked up again after 5 minutes,
    // so a version registered meanwhile reaches the replies without a restart.
    SchemaRegistry::new(url).cache_policy(SchemaCachePolicy::Cached {
        capacity: nonzero!(256usize),
        subject_ttl: Some(Duration::from_secs(300)),
    })
}

fn app(registry: SchemaRegistry) -> impl App {
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("receipts-svc")
        .schema_registry(registry.clone());
    RustStream::new(AppInfo::new("receipts", "0.1.0"))
        .publish_layer(SchemaFrame::new(registry))
        .with_broker(broker, |b| {
            b.include(acknowledge);
        })
}

#[ruststream::app]
fn service() -> impl App {
    app(registry("http://localhost:8081"))
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
    use ruststream_rdkafka::schema_registry::{JsonSchema, encode_envelope};
    use serde::Serialize;

    use super::{app, live, registry};

    /// Bytes published as they are: what a registry-backed producer puts on the topic.
    #[derive(Serialized, Outgoing)]
    struct Wire(Vec<u8>);

    #[derive(Serialize, JsonSchema)]
    #[serde(rename = "Purchase")]
    struct Produced {
        id: i64,
        total: i64,
    }

    #[derive(Serialize, JsonSchema)]
    #[serde(rename = "Receipt")]
    struct Issued {
        purchase: i64,
        total: i64,
    }

    #[tokio::test]
    async fn what_the_edges_resolved_stays_in_the_cache() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
            return Ok(());
        };
        let owners = SchemaRegistry::new(&url);
        let purchases = owners.register_json::<Produced>("purchases-value").await?;
        let receipts = owners.register_json::<Issued>("receipts-value").await?;
        let client = registry(&url);
        let tb = TestApp::start(app(client.clone())).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(encode_envelope(
                purchases,
                br#"{"id":42,"total":1999}"#,
            )))
            .to("purchases")
            .publish()
            .await?;

        kafka.published::<()>("receipts").assert_called_once();
        let by_id = client
            .cached_schema(purchases)
            .ok_or("the delivery's schema is not remembered")?;
        assert_eq!(by_id.id(), purchases);
        let by_subject = client
            .cached_subject("receipts-value")
            .ok_or("the reply's subject is not remembered")?;
        assert_eq!(by_subject.id(), receipts);
        Ok(())
    }
}
