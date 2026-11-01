//! A draft-04 boolean `exclusiveMinimum` is rejected; its `id` is an annotation.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.draft04")]
pub struct Order {
    pub qty: u64,
}

fn main() {}
