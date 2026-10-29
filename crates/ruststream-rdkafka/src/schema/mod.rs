//! Schemas as code: the subjects a topic is checked against, and the `Schema` derive that checks
//! a message type against the newest migration of every subject in `schemas/` that declares its
//! record when the crate compiles.

#[cfg(feature = "schema-check")]
pub use ruststream_rdkafka_schema_macros::Schema;

#[cfg(feature = "schema-check")]
pub mod avro;
#[cfg(feature = "schema-check")]
mod record;
#[cfg(feature = "schema-check")]
pub use record::{Record, RecordKind, RecordSet};

#[cfg(feature = "schema-check")]
#[doc(hidden)]
pub mod __private;
