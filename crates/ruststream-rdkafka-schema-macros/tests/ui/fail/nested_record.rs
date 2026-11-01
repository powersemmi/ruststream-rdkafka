//! A nested record that differs is named by the whole-schema comparison.

use apache_avro::AvroSchema;
use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, AvroSchema, Schema)]
#[avro(namespace = "com.acme.orders")]
pub struct Order {
    pub id: i64,
    pub item: Item,
    pub note: Option<String>,
    pub status: Status,
    pub spare: Option<Item>,
}

/// No namespace: the record takes the enclosing one, so only `Order`'s check reaches it.
#[derive(Serialize, Deserialize, AvroSchema, Schema)]
pub struct Item {
    pub sku: i64,
    pub tags: Vec<String>,
}

#[derive(Serialize, Deserialize, AvroSchema, Schema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[avro(namespace = "com.acme.orders")]
pub enum Status {
    Placed,
    Shipped,
}

fn main() {}
