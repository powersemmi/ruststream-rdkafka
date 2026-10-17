//! The service of `examples/kafka_registry_cache.rs` written without the macros.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example manual_kafka_registry_cache --features schema-registry,testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{SchemaCachePolicy, SchemaFrame, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Purchase {
    id: i64,
    total: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Receipt {
    purchase: i64,
    total: i64,
}

// `#[derive(Outgoing)]` with `#[outgoing(name = "receipts")]`.
impl OutgoingDestination for Receipt {
    type Form = FixedName;
    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

/// The definition `#[subscriber("purchases", reply)]` would have minted.
struct Acknowledge;

impl Handle<Purchase, Receipt> for Acknowledge {
    fn handle(
        &self,
        purchase: &Purchase,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        tracing::info!(purchase.id, purchase.total, "receipt issued");
        ready(Ok(Receipt {
            purchase: purchase.id,
            total: purchase.total,
        }))
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
            b.include(subscriber("purchases", Acknowledge).reply().build());
        })
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(|| app(registry("http://localhost:8081")))
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
    use ruststream_rdkafka::schema_registry::{JsonSchema, encode_envelope};
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
