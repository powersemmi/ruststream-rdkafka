//! A default that differs from the migration's fails the whole-schema comparison.

use apache_avro::AvroSchema;
use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, AvroSchema, Schema)]
#[avro(namespace = "com.acme.orders")]
pub struct Order {
    pub id: i64,
    pub item: Item,
    #[avro(default = "\"none\"")]
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
