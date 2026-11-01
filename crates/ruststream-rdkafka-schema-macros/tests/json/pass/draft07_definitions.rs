//! Draft-07 `definitions` resolve like `$defs`.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.draft07")]
pub struct Order {
    pub items: Vec<Item>,
}

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.draft07")]
pub struct Item {
    pub sku: String,
    pub qty: u32,
}

fn main() {}
