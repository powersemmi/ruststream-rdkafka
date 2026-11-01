//! An external subject is read only: the type reads wider integers and ignores nothing it needs.

use ruststream_rdkafka::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
#[schema(namespace = "schema.external")]
pub struct Stock {
    pub sku: String,
    pub qty: i64,
    pub note: Option<String>,
    #[serde(default)]
    pub warehouse: String,
}

fn main() {}
