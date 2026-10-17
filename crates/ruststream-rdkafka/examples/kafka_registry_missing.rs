//! What a registry codec does when the registry no longer holds its subject: here, register the
//! schema the type carries and warn.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example kafka_registry_missing --features avro,testing
//! ```

use ruststream_rdkafka::avro::{AvroCodec, AvroSchema};
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::{MissingSubject, SchemaPrefetch, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
struct Order {
    id: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, Outgoing)]
#[outgoing(name = "parcels")]
struct Parcel {
    order: i64,
    weight_grams: i64,
}

#[subscriber("orders", reply)]
async fn pack(order: &Order) -> Parcel {
    tracing::info!(order.id, "order packed");
    Parcel {
        order: order.id,
        weight_grams: 500,
    }
}

fn app(registry: SchemaRegistry) -> impl App {
    // This service owns the parcels subject, so a missing one is put back at connect.
    let prefetch = SchemaPrefetch::new(registry).on_missing_subject(MissingSubject::AutoRegister);
    let codec = AvroCodec::registry(&prefetch).register::<Parcel>("parcels-value");
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("packing-svc")
        .schema_prefetch(prefetch);

    let replies = codec.clone();
    RustStream::new(AppInfo::new("packing", "0.1.0")).with_broker_codec(broker, codec, |b| {
        b.include(pack).out_reply(Publish::default()).codec(replies);
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
    use ruststream_rdkafka::{SchemaRegistry, SchemaType};

    use super::{app, live};

    /// Deletes `subject` from the registry for good, as an operator's cleanup would.
    async fn delete(url: &str, subject: &str) -> Result<(), Box<dyn Error>> {
        let http = reqwest::Client::new();
        for permanent in [false, true] {
            let response = http
                .delete(format!("{url}/subjects/{subject}?permanent={permanent}"))
                .send()
                .await?;
            // 404: already gone, which is the state this test wants.
            if response.status() != reqwest::StatusCode::NOT_FOUND {
                response.error_for_status()?;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_deleted_subject_is_registered_again_at_connect() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
            return Ok(());
        };
        delete(&url, "parcels-value").await?;

        let tb = TestApp::start(app(SchemaRegistry::new(&url))).await?;
        tb.assert_running();

        let restored = SchemaRegistry::new(&url).warm("parcels-value").await?;
        assert_eq!(restored.schema_type(), SchemaType::Avro);
        assert!(restored.definition().contains(r#""name":"Parcel""#));
        Ok(())
    }
}
