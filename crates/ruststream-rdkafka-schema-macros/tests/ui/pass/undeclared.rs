//! A type no migration declares carries no check, whatever its fields are.

use ruststream_rdkafka::schema::Schema;

pub struct Opaque;

#[derive(Schema)]
pub struct Unlisted {
    pub opaque: Opaque,
}

fn main() {}
