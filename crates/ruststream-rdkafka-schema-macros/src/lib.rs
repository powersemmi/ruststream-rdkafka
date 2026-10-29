//! The `Schema` derive of `ruststream-rdkafka`.
//!
//! Use it through `ruststream-rdkafka`'s `schema-check` feature, as
//! `ruststream_rdkafka::schema::Schema`: the assertions it emits name traits that live there.

// The derive is a stub until the checks land; these modules are what it will call.
#[allow(dead_code)]
mod lock;
#[allow(dead_code)]
mod migrations;

use proc_macro::TokenStream;

/// Checks a message type against the newest migration of every subject in `schemas/` that
/// declares its record, when the crate compiles.
///
/// See `ruststream_rdkafka::schema` for the full documentation.
#[proc_macro_derive(Schema, attributes(schema))]
pub fn derive_schema(item: TokenStream) -> TokenStream {
    let _ = item;
    TokenStream::new()
}
