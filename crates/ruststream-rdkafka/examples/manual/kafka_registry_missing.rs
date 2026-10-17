//! The service of `examples/kafka_registry_missing.rs` written without the macros.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example manual_kafka_registry_missing --features avro,testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::avro::{AvroCodec, AvroSchema};
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{MissingSubject, SchemaPrefetch, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, JsonSchema)]
struct Order {
    id: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, JsonSchema)]
struct Parcel {
    order: i64,
    weight_grams: i64,
}

// `#[derive(Outgoing)]` with `#[outgoing(name = "parcels")]`.
impl OutgoingDestination for Parcel {
    type Form = FixedName;
    const DESTINATION: &'static str = "parcels";
}

impl MessageHeaders for Parcel {
    type Contract = NoHeaders;
}

/// The definition `#[subscriber("orders", reply)]` would have minted.
struct Pack;

impl Handle<Order, Parcel> for Pack {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Parcel, HandlerOutcome>> {
        tracing::info!(order.id, "order packed");
        ready(Ok(Parcel {
            order: order.id,
            weight_grams: 500,
        }))
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
        b.include(subscriber("orders", Pack).reply().build())
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
