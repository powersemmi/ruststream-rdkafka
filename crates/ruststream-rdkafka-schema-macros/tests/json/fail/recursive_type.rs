//! A recursive Rust type is rejected unless the field is unchecked.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.recursive")]
pub struct Node {
    pub name: String,
    pub next: Option<Box<Node>>,
}

fn main() {}
