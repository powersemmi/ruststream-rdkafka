//! The schema lets `note` be absent and never null, and an `Option` writes null.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.nullable")]
pub struct Order {
    pub note: Option<String>,
}

fn main() {}
