//! An enum's variants against the symbols the migration declares.

use apache_avro::AvroSchema;
use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, AvroSchema, Schema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[avro(namespace = "com.acme.orders")]
pub enum Status {
    Placed,
    Lost,
}

fn main() {}
