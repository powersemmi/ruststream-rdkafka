//! A generic type has no single shape to compare.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.generic")]
pub struct Page<Item> {
    pub items: Vec<Item>,
}

fn main() {}
