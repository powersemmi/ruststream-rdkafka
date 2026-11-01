//! A recursive record compiles: its fields are bound, and the whole-schema comparison is left
//! to the migrations tool.

use apache_avro::AvroSchema;
use ruststream_rdkafka::schema::Schema;
use serde::Serialize;

#[derive(Serialize, AvroSchema, Schema)]
#[avro(namespace = "com.acme")]
pub struct Tree {
    pub value: i64,
    pub children: Vec<Tree>,
}

fn main() {}
