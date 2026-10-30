//! A tuple struct is no record the derive checks.

use ruststream_rdkafka::schema::Schema;

#[derive(Schema)]
pub struct Pair(i64, i64);

fn main() {}
