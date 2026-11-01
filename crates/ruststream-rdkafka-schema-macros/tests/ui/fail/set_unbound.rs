//! A variant whose model derives no `Schema` carries no record.

use ruststream::prelude::*;
use ruststream_rdkafka::schema::Schema;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
pub struct OrderCreated {
    pub id: i64,
}

#[derive(InMessages, Schema)]
pub enum OrderEvent {
    Created(OrderCreated),
}

fn main() {}
