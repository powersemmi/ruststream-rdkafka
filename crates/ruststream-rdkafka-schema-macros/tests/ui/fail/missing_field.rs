//! A field the record declares without a default is missing.

use apache_avro::AvroSchema;
use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, AvroSchema, Schema)]
#[avro(namespace = "com.acme.orders")]
pub struct Order {
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
