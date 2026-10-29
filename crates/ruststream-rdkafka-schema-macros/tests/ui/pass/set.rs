//! A set of input models maps each variant to its model's record.

use apache_avro::AvroSchema;
use ruststream::prelude::*;
use ruststream_rdkafka::schema::{RecordKind, RecordSet, Schema};
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
pub enum OrderEvent {
    Created(OrderCreated),
    Cancelled(OrderCancelled),
}

fn main() {
    assert_eq!(
        OrderEvent::RECORDS,
        ["com.acme.orders.OrderCreated", "com.acme.orders.OrderCancelled"]
    );
    assert_eq!(
        OrderEventKind::of_record("com.acme.orders.OrderCancelled"),
        Some(OrderEventKind::Cancelled)
    );
    assert_eq!(OrderEventKind::of_record("com.acme.orders.Order"), None);
}
