//! `prefixItems` (a tuple) is rejected.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.tuple")]
pub struct Order {
    pub pair: Vec<i64>,
}

fn main() {}
