//! The traits the `Schema` derive's Avro assertions name.
//!
//! - [`Carries`]: a field of an owned subject's type writes the Avro type its newest migration
//!   declares.
//! - [`Reads`]: a field of an external subject's type reads what every pulled version writes.
//! - [`kind`]: the Avro types as marker types those bounds name.
//! - [`AvroForm`]: the canonical form a type carries as a constant, composed from its fields'
//!   forms, which the derive compares with the migration as a whole.
//!
//! None of it is written by hand: the derive emits the bounds and the comparison, and the
//! compiler checks them. They are constants, uncalled functions and trait bounds, so nothing of
//! the check reaches the binary.

mod compare;
mod form;
pub mod kind;

#[doc(hidden)]
pub use compare::{Site, assert_equals, assert_reads};
#[doc(hidden)]
pub use form::{EnumForm, FieldForm, FixedForm, Form, Member, Name, RecordForm, Shape};
pub use form::{AvroFields, AvroForm};
pub use kind::{Carries, Reads};
