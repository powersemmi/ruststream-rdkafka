//! A construct in a referenced subject is named by that subject's pointer.

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
    pub code: String,
}

fn main() {}
