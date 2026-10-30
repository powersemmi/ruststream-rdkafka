//! A recursive type and a recursive `$ref` meet on an unchecked field.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.recursive")]
pub struct Node {
    pub name: String,
    #[schema(unchecked)]
    pub next: Option<Box<Node>>,
}

fn main() {}
