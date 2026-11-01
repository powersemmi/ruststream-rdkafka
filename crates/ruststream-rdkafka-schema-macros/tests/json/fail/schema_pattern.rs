//! A `pattern` is a constraint no Rust type promises.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.pattern")]
pub struct Order {
    pub sku: String,
    pub qty: u32,
}

fn main() {}
