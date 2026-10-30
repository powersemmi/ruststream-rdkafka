//! A reader of every pulled version of an external subject: `int` and `long` both read into
//! `i64`, `bytes` reads as a string, and a field an older version lacks has a default.

use apache_avro::AvroSchema;
use ruststream_rdkafka::schema::Schema;
use serde::Deserialize;

#[derive(Deserialize, AvroSchema, Schema)]
#[avro(namespace = "com.acme.inventory")]
pub struct Stock {
    pub sku: String,
    pub count: i64,
    #[avro(default = "\"main\"")]
    pub warehouse: String,
}

fn main() {}
