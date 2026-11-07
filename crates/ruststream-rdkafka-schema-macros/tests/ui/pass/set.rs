//! A set of input models maps each variant to its model's record, and decodes under a registry
//! codec by the record the delivery's schema names.

use apache_avro::AvroSchema;
use ruststream::prelude::*;
use ruststream_rdkafka::avro::AvroCodec;
use ruststream_rdkafka::schema::{RecordSet, Schema};
use ruststream_rdkafka::{KafkaBroker, SchemaPrefetch, SchemaRegistry};
use serde::Deserialize;

#[derive(Deserialize, AvroSchema, Schema, MessageInfo)]
#[avro(namespace = "com.acme.orders")]
pub struct OrderCreated {
    pub id: i64,
}

#[derive(Deserialize, AvroSchema, Schema, MessageInfo)]
#[avro(namespace = "com.acme.orders")]
pub struct OrderCancelled {
    pub id: i64,
    pub reason: String,
}

#[derive(InMessages, Schema)]
#[in_messages(by_codec)]
pub enum OrderEvent {
    Created(OrderCreated),
    Cancelled(OrderCancelled),
}

#[subscriber("orders")]
async fn on_event(event: &OrderEvent) -> HandlerOutcome {
    let _ = event;
    HandlerOutcome::ack()
}

fn app() -> RustStream {
    let prefetch = SchemaPrefetch::new(SchemaRegistry::new("http://localhost:8081"));
    let codec = AvroCodec::registry(&prefetch);
    let broker = KafkaBroker::new(["localhost:9092"]).schema_prefetch(prefetch);
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker_codec(broker, codec, |b| b.include(on_event))
}

fn main() {
    assert_eq!(
        OrderEvent::RECORDS,
        ["com.acme.orders.OrderCreated", "com.acme.orders.OrderCancelled"]
    );
    let _ = app;
}
