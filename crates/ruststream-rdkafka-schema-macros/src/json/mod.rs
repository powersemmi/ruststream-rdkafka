//! The JSON Schema check of the `Schema` derive.

use proc_macro2::TokenStream;
use syn::DeriveInput;

/// Expands the derive for a JSON Schema type (one that carries `#[schema(namespace = ..)]`).
// The signature the JSON Schema check fills in; the stub has nothing to fail on yet.
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn derive(input: &DeriveInput) -> syn::Result<TokenStream> {
    let _ = input;
    Ok(TokenStream::new())
}
