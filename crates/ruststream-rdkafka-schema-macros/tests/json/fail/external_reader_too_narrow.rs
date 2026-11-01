//! An external subject still has to deserialize into the type.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.external")]
pub struct Stock {
    pub sku: String,
    pub qty: i16,
}

fn main() {}
