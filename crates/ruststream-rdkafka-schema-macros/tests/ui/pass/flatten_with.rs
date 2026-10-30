//! A `with` field and a flattened type are checked for presence.

use std::collections::HashSet;

use apache_avro::AvroSchema;
use apache_avro::schema::{Name, NamespaceRef, Schema as AvroType};
use ruststream_rdkafka::schema::Schema;
use serde::Serialize;

fn timestamp(_: &mut HashSet<Name>, _: NamespaceRef) -> AvroType {
    u64::get_schema()
}

#[derive(Serialize, AvroSchema, Schema)]
#[avro(namespace = "com.acme.audit")]
pub struct Audited {
    pub id: i64,
    #[avro(with = timestamp)]
    pub at: u64,
    #[serde(flatten)]
    pub origin: Origin,
}

#[derive(Serialize, AvroSchema, Schema)]
pub struct Origin {
    pub by: String,
    pub host: String,
}

fn main() {}
