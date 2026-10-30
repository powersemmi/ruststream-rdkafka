//! `anyOf` of two types is not "T or null".

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.any_of")]
pub struct Order {
    pub id: String,
}

fn main() {}
