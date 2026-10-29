//! `#[schema(unchecked)]` limits the field to its presence and optionality.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.pattern")]
pub struct Order {
    #[schema(unchecked)]
    pub sku: String,
    pub qty: u32,
}

fn main() {}
