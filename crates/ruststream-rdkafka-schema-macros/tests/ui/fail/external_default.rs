//! A reader field an older version lacks needs a default.

use apache_avro::AvroSchema;
use ruststream_rdkafka::schema::Schema;
use serde::Deserialize;

#[derive(Deserialize, AvroSchema, Schema)]
#[avro(namespace = "com.acme.inventory")]
pub struct Stock {
    pub sku: String,
    pub count: i64,
    pub warehouse: String,
}

fn main() {}
