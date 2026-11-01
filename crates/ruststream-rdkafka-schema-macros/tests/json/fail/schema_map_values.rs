//! The map holds strings, the schema integers.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.map")]
pub struct Order {
    pub tags: BTreeMap<String, String>,
}

fn main() {}
