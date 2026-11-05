//! A message set of registry records under `TestApp`: the registry codec's frame names the
//! variant a delivery decodes into, by the record the payload's schema id stands for.
//!
//! The registry is a client of the test's own, holding one schema per record; the codecs resolve
//! the ids through it at connect, as they do against a Confluent registry.

#![cfg(all(feature = "testing", feature = "avro", feature = "schema-check"))]

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::AvroSchema;
use futures::future::BoxFuture;
use ruststream::codec::JsonCodec;
use ruststream::prelude::*;
use ruststream::testing::{Outcome, TestApp};
use ruststream_rdkafka::avro::AvroCodec;
use ruststream_rdkafka::schema::Schema;
use ruststream_rdkafka::schema_registry::{JsonSchema, RegisteredSchema, RegistryClient};
use ruststream_rdkafka::{
    KafkaBroker, KafkaError, SchemaFramed, SchemaPrefetch, SchemaRegistry, SchemaType,
};
use serde::{Deserialize, Serialize};

const CREATED: &str = "com.acme.frames.OrderCreated";
const CANCELLED: &str = "com.acme.frames.OrderCancelled";
const REFUNDED: &str = "com.acme.frames.OrderRefunded";

/// A registry that holds a fixed set of subjects, each with one schema.
struct Fixed {
    by_subject: HashMap<&'static str, Arc<RegisteredSchema>>,
}

impl Fixed {
    fn new(subjects: impl IntoIterator<Item = (&'static str, RegisteredSchema)>) -> Self {
        Self {
            by_subject: subjects
                .into_iter()
                .map(|(subject, schema)| (subject, Arc::new(schema)))
                .collect(),
        }
    }

    fn by_id(&self, id: u32) -> Option<Arc<RegisteredSchema>> {
        self.by_subject
            .values()
            .find(|schema| schema.id() == id)
            .cloned()
    }
}

impl RegistryClient for Fixed {
    fn schema_by_id(&self, id: u32) -> BoxFuture<'_, Result<Arc<RegisteredSchema>, KafkaError>> {
        Box::pin(async move {
            self.by_id(id)
                .ok_or_else(|| KafkaError::SchemaRegistry(format!("no schema {id}").into()))
        })
    }

    fn latest(
        &self,
        subject: &str,
    ) -> BoxFuture<'_, Result<Option<Arc<RegisteredSchema>>, KafkaError>> {
        let found = self.by_subject.get(subject).cloned();
        Box::pin(async move { Ok(found) })
    }

    fn register(
        &self,
        subject: &str,
        _schema_type: SchemaType,
        _definition: String,
    ) -> BoxFuture<'_, Result<u32, KafkaError>> {
        let found = self.by_subject.get(subject).map(|schema| schema.id());
        Box::pin(async move { found.ok_or_else(|| KafkaError::SchemaRegistry("read only".into())) })
    }

    fn lookup_id(
        &self,
        subject: &str,
        schema_type: SchemaType,
        definition: String,
    ) -> BoxFuture<'_, Result<u32, KafkaError>> {
        self.register(subject, schema_type, definition)
    }
}

/// Each subject holds its record's own schema, under the id the payloads carry.
fn avro_registry() -> SchemaRegistry {
    let schema = |id, record: &str| {
        let (namespace, name) = record.rsplit_once('.').expect("a full name");
        let fields = match name {
            "OrderCancelled" => {
                r#"[{"name":"id","type":"long"},{"name":"reason","type":"string"}]"#
            }
            _ => r#"[{"name":"id","type":"long"}]"#,
        };
        RegisteredSchema::new(
            id,
            SchemaType::Avro,
            format!(
                r#"{{"type":"record","name":"{name}","namespace":"{namespace}","fields":{fields}}}"#
            ),
        )
    };
    SchemaRegistry::with_client(Arc::new(Fixed::new([
        (CREATED, schema(1, CREATED)),
        (CANCELLED, schema(2, CANCELLED)),
        (REFUNDED, schema(3, REFUNDED)),
    ])))
}

#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, Schema, Outgoing)]
#[avro(namespace = "com.acme.frames")]
#[outgoing(name = "orders")]
struct OrderCreated {
    id: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, Schema, Outgoing)]
#[avro(namespace = "com.acme.frames")]
#[outgoing(name = "orders")]
struct OrderCancelled {
    id: i64,
    reason: String,
}

/// A record of the topic that the set below carries no variant for.
#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema, Outgoing)]
#[avro(namespace = "com.acme.frames")]
#[outgoing(name = "orders")]
struct OrderRefunded {
    id: i64,
}

/// Two records of one shape apart from `reason`: only the frame tells a delivery's variant.
#[derive(InMessages, Schema)]
#[in_messages(by_codec)]
enum OrderEvent {
    Created(OrderCreated),
    Cancelled(OrderCancelled),
}

/// Each variant settles its own way, so the outcomes say which one a delivery decoded into.
#[subscriber("orders")]
async fn on_event(event: &OrderEvent) -> HandlerOutcome {
    match event {
        OrderEvent::Created(created) => {
            tracing::info!(created.id, "created");
            HandlerOutcome::ack()
        }
        OrderEvent::Cancelled(cancelled) => {
            tracing::info!(cancelled.id, cancelled.reason, "cancelled");
            HandlerOutcome::drop()
        }
    }
}

/// The codec publishes every record under its own subject, and decodes the set by its frame.
fn avro_codec(prefetch: &SchemaPrefetch) -> AvroCodec {
    AvroCodec::registry(prefetch)
        .register::<OrderCreated>(CREATED)
        .register::<OrderCancelled>(CANCELLED)
        .register::<OrderRefunded>(REFUNDED)
}

fn avro_app() -> (RustStream, AvroCodec) {
    let prefetch = SchemaPrefetch::new(avro_registry());
    let codec = avro_codec(&prefetch);
    let broker = KafkaBroker::new(["kafka:9092"])
        .default_group("orders-svc")
        .schema_prefetch(prefetch);
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_codec(
        broker,
        codec.clone(),
        |b| b.include(on_event),
    );
    (app, codec)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_avro_registry_set_decodes_the_variant_its_schema_id_names() {
    let (app, codec) = avro_app();
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&OrderCancelled {
            id: 2,
            reason: "late".to_owned(),
        })
        .with_codec(codec.clone())
        .publish()
        .await
        .expect("publish");
    tb.broker::<KafkaBroker>()
        .message(&OrderCreated { id: 1 })
        .with_codec(codec)
        .publish()
        .await
        .expect("publish");

    assert_eq!(
        tb.broker::<KafkaBroker>().subscriber("orders").outcomes(),
        [Outcome::Drop, Outcome::Ack],
    );
    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_the_set_carries_no_variant_for_fails_to_decode() {
    let (app, codec) = avro_app();
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&OrderRefunded { id: 3 })
        .with_codec(codec)
        .publish()
        .await
        .expect("publish");

    assert_eq!(
        tb.broker::<KafkaBroker>().subscriber("orders").outcomes(),
        [Outcome::DecodeFailed],
    );
    tb.shutdown().await.expect("shutdown");
}

// ------------------------------------------------------------------- JSON under the envelope

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema, Schema, Outgoing)]
#[schema(namespace = "com.acme.frames")]
#[outgoing(name = "parcels")]
struct ParcelPacked {
    id: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema, Schema, Outgoing)]
#[schema(namespace = "com.acme.frames")]
#[outgoing(name = "parcels")]
struct ParcelShipped {
    id: i64,
}

/// Two records of one shape: only the frame tells them apart.
#[derive(InMessages, Schema)]
#[in_messages(by_codec)]
enum ParcelEvent {
    Packed(ParcelPacked),
    Shipped(ParcelShipped),
}

#[subscriber("parcels")]
async fn on_parcel(event: &ParcelEvent) -> HandlerOutcome {
    match event {
        ParcelEvent::Packed(packed) => {
            tracing::info!(packed.id, "packed");
            HandlerOutcome::ack()
        }
        ParcelEvent::Shipped(shipped) => {
            tracing::info!(shipped.id, "shipped");
            HandlerOutcome::drop()
        }
    }
}

/// JSON Schemas name their record by `title`, the type name schemars gives them.
fn json_registry() -> SchemaRegistry {
    let schema = |id, title: &str| {
        RegisteredSchema::new(
            id,
            SchemaType::Json,
            format!(r#"{{"title":"{title}","type":"object"}}"#),
        )
    };
    SchemaRegistry::with_client(Arc::new(Fixed::new([
        ("parcels-packed", schema(11, "ParcelPacked")),
        ("parcels-shipped", schema(12, "ParcelShipped")),
    ])))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_json_registry_set_decodes_the_variant_its_schema_title_names() {
    let prefetch = SchemaPrefetch::new(json_registry());
    let codec = SchemaFramed::new(&prefetch, JsonCodec)
        .register::<ParcelPacked>("parcels-packed")
        .register::<ParcelShipped>("parcels-shipped");
    let broker = KafkaBroker::new(["kafka:9092"])
        .default_group("parcels-svc")
        .schema_prefetch(prefetch);
    let app = RustStream::new(AppInfo::new("parcels", "0.1.0")).with_broker_codec(
        broker,
        codec.clone(),
        |b| b.include(on_parcel),
    );
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&ParcelShipped { id: 2 })
        .with_codec(codec.clone())
        .publish()
        .await
        .expect("publish");
    tb.broker::<KafkaBroker>()
        .message(&ParcelPacked { id: 1 })
        .with_codec(codec)
        .publish()
        .await
        .expect("publish");

    assert_eq!(
        tb.broker::<KafkaBroker>().subscriber("parcels").outcomes(),
        [Outcome::Drop, Outcome::Ack],
    );
    tb.shutdown().await.expect("shutdown");
}
