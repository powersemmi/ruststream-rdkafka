//! A field the record does not declare.

use apache_avro::AvroSchema;
use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, AvroSchema, Schema)]
#[avro(namespace = "com.acme.orders")]
pub struct Order {
    pub id: i64,
    pub qty: i32,
    pub item: Item,
    pub note: Option<String>,
    pub status: Status,
    pub spare: Option<Item>,
}

#[derive(Serialize, Deserialize, AvroSchema, Schema)]
#[avro(namespace = "com.acme.orders")]
pub struct Item {
    pub sku: String,
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
