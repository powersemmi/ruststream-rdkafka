//! A recursive `$ref` has no finite shape.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.recursive")]
pub struct Node {
    pub name: String,
    pub next: Option<Child>,
}

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.recursive")]
pub struct Child {
    pub name: String,
}

fn main() {}
