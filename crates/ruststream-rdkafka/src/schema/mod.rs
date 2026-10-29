//! Schemas as code.
//!
//! The subjects a topic is checked against, and the `Schema` derive that checks a message type
//! against the newest migration of every subject in `schemas/` that declares its record when the
//! crate compiles.
//!
//! - [Schema checking][checking]: `Schema`, the namespace and subjects a descriptor's
//!   `.schema(..)` step names; the broker checks them against its registry at startup.
//!
//! [checking]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/subscriptions/schema/

#[cfg(feature = "schema-check")]
pub use ruststream_rdkafka_schema_macros::Schema;

#[cfg(feature = "schema-registry")]
mod descriptor;
#[cfg(feature = "schema-registry")]
mod runtime;

#[cfg(feature = "schema-registry")]
pub use descriptor::Schema;
#[cfg(feature = "schema-registry")]
pub(crate) use descriptor::{PatternSchemas, SubjectAfterSchema, TopicSchemas};
#[cfg(feature = "schema-registry")]
#[doc(hidden)]
pub use runtime::RecordKind;
#[cfg(feature = "schema-registry")]
pub(crate) use runtime::{SchemaWatch, check_topic};
