//! The `Schema` derive of `ruststream-rdkafka`.
//!
//! Use it through `ruststream-rdkafka`'s `schema-check` feature, as
//! `ruststream_rdkafka::schema::Schema`: the assertions it emits name traits that live there.

mod json;
mod migrations;
mod online;

use proc_macro::TokenStream;

/// Checks a message type against the newest migration of every subject in `schemas/` that
/// declares its record, when the crate compiles.
///
/// See `ruststream_rdkafka::schema` for the full documentation.
#[proc_macro_derive(Schema, attributes(schema))]
pub fn derive_schema(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::DeriveInput);
    if !json::has_namespace(&input) {
        return TokenStream::new();
    }
    json::derive(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
