//! A closed object refuses a flattened map, which writes undeclared properties.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.closed")]
pub struct Order {
    pub id: i64,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

fn main() {}
