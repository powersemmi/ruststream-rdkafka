//! A `$ref` to another subject resolves through `subject.toml`.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.reference")]
pub struct Shipment {
    pub part: Part,
}

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.reference")]
pub struct Part {
    pub sku: String,
    #[schema(unchecked)]
    pub code: String,
}

fn main() {}
