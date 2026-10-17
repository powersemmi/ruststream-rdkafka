//! A registry client of your own: schemas shipped with the service answer the registry's
//! questions, for a deployment with no registry to ask.
//!
//! ```text
//! cargo test --example kafka_registry_custom_client --features avro,testing
//! ```

use std::collections::HashMap;
use std::future::ready;
use std::sync::Arc;

use futures::future::BoxFuture;
use ruststream_rdkafka::KafkaError;
use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::schema_registry::{
    RegisteredSchema, RegistryClient, SchemaRegistry, SchemaType,
};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
/// The schemas this service reads, by the id their records carry.
struct Catalog(HashMap<u32, Arc<RegisteredSchema>>);

impl Catalog {
    fn bundled() -> Self {
        let order =
            r#"{"type": "record", "name": "Order", "fields": [{"name": "id", "type": "long"}]}"#;
        Self(HashMap::from([(
            1,
            Arc::new(RegisteredSchema::new(1, SchemaType::Avro, order)),
        )]))
    }

    fn read_only() -> Result<u32, KafkaError> {
        Err(KafkaError::SchemaRegistry(
            "the bundled catalog is read-only".into(),
        ))
    }
}

impl RegistryClient for Catalog {
    fn schema_by_id(&self, id: u32) -> BoxFuture<'_, Result<Arc<RegisteredSchema>, KafkaError>> {
        let found = self.0.get(&id).cloned().ok_or_else(|| {
            KafkaError::SchemaRegistry(format!("schema id {id} is not in the catalog").into())
        });
        Box::pin(ready(found))
    }

    fn latest(
        &self,
        _subject: &str,
    ) -> BoxFuture<'_, Result<Option<Arc<RegisteredSchema>>, KafkaError>> {
        // No subjects: every topic this service publishes to is a plain one.
        Box::pin(ready(Ok(None)))
    }

    fn register(
        &self,
        _subject: &str,
        _schema_type: SchemaType,
        _definition: String,
    ) -> BoxFuture<'_, Result<u32, KafkaError>> {
        Box::pin(ready(Self::read_only()))
    }

    fn lookup_id(
        &self,
        _subject: &str,
        _schema_type: SchemaType,
        _definition: String,
    ) -> BoxFuture<'_, Result<u32, KafkaError>> {
        Box::pin(ready(Self::read_only()))
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Order {
    id: i64,
}

#[subscriber("orders")]
async fn take(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order taken");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    // Everything above the client (the transcode, the codecs, the cache) works unchanged.
    let registry = SchemaRegistry::with_client(Arc::new(Catalog::bundled()));
    let broker = KafkaBroker::new(["localhost:9092"])
        .default_group("orders-svc")
        .schema_registry(registry);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(take);
    })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::avro::{self, AvroSchema};
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::encode_envelope;
    use serde::Serialize;

    use super::{Order, app};

    /// Bytes published as they are: what a registry-backed producer puts on the topic.
    #[derive(Serialized, Outgoing)]
    struct Wire(Vec<u8>);

    /// The producer's model, whose schema is the catalog's id 1.
    #[derive(Serialize, AvroSchema)]
    #[serde(rename = "Order")]
    struct Produced {
        id: i64,
    }

    #[tokio::test]
    async fn the_catalog_answers_for_the_transcode() -> Result<(), Box<dyn Error>> {
        let mut datum = BytesMut::new();
        avro::encode(&Produced { id: 42 }, &mut datum)?;
        let tb = TestApp::start(app()).await?;
        let kafka = tb.broker::<KafkaBroker>();

        kafka
            .message(&Wire(encode_envelope(1, &datum)))
            .to("orders")
            .publish()
            .await?;

        kafka
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 42 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
