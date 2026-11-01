//! An external subject's reader cannot narrow a `long` the newest version writes.

use apache_avro::AvroSchema;
use ruststream_rdkafka::schema::Schema;
use serde::Deserialize;

#[derive(Deserialize, AvroSchema, Schema)]
#[avro(namespace = "com.acme.inventory")]
pub struct Stock {
    pub sku: String,
    pub count: i32,
    #[avro(default = "\"main\"")]
    pub warehouse: String,
}

fn main() {}
