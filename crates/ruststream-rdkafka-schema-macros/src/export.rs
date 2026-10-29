//! Export mode, shared by every format.
//!
//! The migrations tool exports the schema the runtime itself builds: it compiles the crate with
//! `RUSTSTREAM_SCHEMA_EXPORT=<dir>` and runs `cargo test <cargo args> -- __ruststream_schema_export`.
//! While the variable is set at compile time the derive emits no checks, so a changed type
//! exports before its migration exists.
//!
//! The functions the other formats call (keep their signatures):
//!
//! - `active() -> bool`: the variable is set, so the derive emits no checks.
//! - `test(ident: &Ident, record: &str, extension: &str, schema: &TokenStream) -> TokenStream`:
//!   the hidden test `__ruststream_schema_export_<type snake case>` in a hidden module beside the
//!   type. `schema` is an expression of type `String` evaluated inside that module (the type is
//!   `super::<Type>` there); the test writes it to `<dir>/<record>.<extension>`.

use std::env;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::Ident;

/// The variable that names the export directory.
pub(crate) const EXPORT_VAR: &str = "RUSTSTREAM_SCHEMA_EXPORT";

/// True when the crate is compiled for an export: the derive then emits no checks.
pub(crate) fn active() -> bool {
    env::var_os(EXPORT_VAR).is_some_and(|dir| !dir.is_empty())
}

/// The hidden test that writes `schema` to `<dir>/<record>.<extension>` when the crate was
/// compiled with `RUSTSTREAM_SCHEMA_EXPORT=<dir>`, and does nothing otherwise.
pub(crate) fn test(
    ident: &Ident,
    record: &str,
    extension: &str,
    schema: &TokenStream,
) -> TokenStream {
    let name = format_ident!(
        "__ruststream_schema_export_{}",
        snake_case(&ident.to_string())
    );
    quote! {
        #[cfg(test)]
        #[doc(hidden)]
        mod #name {
            #[test]
            fn #name() {
                if let ::core::option::Option::Some(dir) = option_env!("RUSTSTREAM_SCHEMA_EXPORT") {
                    ::ruststream_rdkafka::schema::__private::export(dir, #record, #extension, #schema);
                }
            }
        }
    }
}

/// `OrderKey` -> `order_key`.
fn snake_case(name: &str) -> String {
    let mut out = String::new();
    for (index, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if index > 0 && !out.ends_with('_') {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_test_is_named_after_the_type_in_snake_case() {
        assert_eq!(snake_case("OrderKey"), "order_key");
        assert_eq!(snake_case("Order"), "order");
        let tokens = test(
            &format_ident!("OrderKey"),
            "com.acme.OrderKey",
            "avsc",
            &quote!(String::new()),
        )
        .to_string();
        assert!(
            tokens.contains("fn __ruststream_schema_export_order_key"),
            "{tokens}"
        );
        assert!(
            tokens.contains("\"com.acme.OrderKey\" , \"avsc\""),
            "{tokens}"
        );
    }
}
