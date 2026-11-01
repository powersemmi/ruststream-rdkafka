//! The flattened `Audit` gained a field.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use ruststream_rdkafka::schema::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, JsonSchema, Schema)]
#[serde(rename_all = "camelCase")]
#[schema(namespace = "com.acme.orders")]
pub struct Order {
    pub order_id: i64,
    pub items: Vec<Item>,
    pub note: Option<String>,
    #[serde(default)]
    pub priority: i32,
    pub payment: Payment,
    pub channel: Channel,
    pub delivery: Delivery,
    pub status: Status,
    pub tags: BTreeMap<String, String>,
    #[serde(flatten)]
    pub audit: Audit,
}

#[derive(Serialize, Deserialize, JsonSchema, Schema)]
#[schema(namespace = "com.acme.orders")]
pub struct Item {
    pub sku: String,
    pub qty: u32,
    pub price: f64,
}

/// Externally tagged: a oneOf of a string enum and single-property objects.
#[derive(Serialize, Deserialize, JsonSchema, Schema)]
#[schema(namespace = "com.acme.orders")]
pub enum Payment {
    Card { number: String },
    Cash,
    Voucher(Voucher),
}

#[derive(Serialize, Deserialize, JsonSchema, Schema)]
#[schema(namespace = "com.acme.orders")]
pub struct Voucher {
    pub code: String,
}

/// Internally tagged: a oneOf keyed by a const.
#[derive(Serialize, Deserialize, JsonSchema, Schema)]
#[serde(tag = "kind")]
#[schema(namespace = "com.acme.orders")]
pub enum Channel {
    Web { url: String },
    Store(Store),
    Phone,
}

#[derive(Serialize, Deserialize, JsonSchema, Schema)]
#[schema(namespace = "com.acme.orders")]
pub struct Store {
    pub id: u16,
}

/// Adjacently tagged.
#[derive(Serialize, Deserialize, JsonSchema, Schema)]
#[serde(tag = "t", content = "c")]
#[schema(namespace = "com.acme.orders")]
pub enum Delivery {
    Pickup,
    Courier { eta_minutes: u32 },
    Mail(Address),
}

#[derive(Serialize, Deserialize, JsonSchema, Schema)]
#[schema(namespace = "com.acme.orders")]
pub struct Address {
    pub line: String,
}

/// A string enum.
#[derive(Serialize, Deserialize, JsonSchema, Schema)]
#[serde(rename_all = "snake_case")]
#[schema(namespace = "com.acme.orders")]
pub enum Status {
    New,
    InProgress,
}

#[derive(Serialize, Deserialize, JsonSchema, Schema)]
#[schema(namespace = "com.acme.orders")]
pub struct Audit {
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub trace: Uuid,
    pub source: String,
}

fn main() {}
