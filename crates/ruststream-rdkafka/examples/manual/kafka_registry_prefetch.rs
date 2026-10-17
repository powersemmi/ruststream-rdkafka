//! The service of `examples/kafka_registry_prefetch.rs` written without the macros.
//!
//! ```text
//! SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
//!     cargo test --example manual_kafka_registry_prefetch --features avro,testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_rdkafka::avro::{AvroCodec, AvroSchema};
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::JsonSchema;
use ruststream_rdkafka::{SchemaPrefetch, SchemaRegistry};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, JsonSchema)]
struct Order {
    id: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, JsonSchema)]
struct Shipment {
    order: i64,
    carrier: String,
}

// `#[derive(Outgoing)]` with `#[outgoing(name = "shipments")]`.
impl OutgoingDestination for Shipment {
    type Form = FixedName;
    const DESTINATION: &'static str = "shipments";
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

/// The definition `#[subscriber("orders", reply)]` would have minted.
struct Ship;

impl Handle<Order, Shipment> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Shipment, HandlerOutcome>> {
        tracing::info!(order.id, "order shipped");
        ready(Ok(Shipment {
            order: order.id,
            carrier: "post".to_owned(),
        }))
    }
}

fn app(registry: SchemaRegistry) -> impl App {
    // At connect: resolve each registered subject and check the type's schema against it.
    let prefetch = SchemaPrefetch::new(registry).check_compatibility(true);
    let codec = AvroCodec::registry(&prefetch).register::<Shipment>("shipments-value");
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("shipping-svc")
        .schema_prefetch(prefetch);

    let replies = codec.clone();
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker_codec(broker, codec, |b| {
        b.include(subscriber("orders", Ship).reply().build())
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
    use ruststream_rdkafka::SchemaRegistry;

    use super::{Shipment, app, live};

    #[tokio::test]
    async fn connect_resolves_the_registered_subject() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("SCHEMA_REGISTRY_TEST_URL") else {
            return Ok(());
        };
        // The subject exists before the service starts, as its owner registered it.
        let shipments = SchemaRegistry::new(&url)
            .register_avro::<Shipment>("shipments-value")
            .await?;
        let registry = SchemaRegistry::new(&url);
        let tb = TestApp::start(app(registry.clone())).await?;
        tb.assert_running();

        // Nothing was published yet, and the subject is already in the shared cache.
        let resolved = registry
            .cached_subject("shipments-value")
            .ok_or("connect did not resolve the subject")?;
        assert_eq!(resolved.id(), shipments);
        Ok(())
    }
}
