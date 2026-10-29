//! The `Schema` derive of `ruststream-rdkafka`.
//!
//! Use it through `ruststream-rdkafka`'s `schema-check` feature, as
//! `ruststream_rdkafka::schema::Schema`: the assertions it emits name traits that live there.

mod avro;
mod export;
mod json;
mod lock;
mod migrations;
mod model;
mod online;

use proc_macro::TokenStream;
use serde_json::Value;
use syn::{DeriveInput, parse_macro_input};

/// Checks a message type against the newest migration of every subject in `schemas/` that
/// declares its record, when the crate compiles.
///
/// See `ruststream_rdkafka::schema` for the full documentation.
#[proc_macro_derive(Schema, attributes(schema))]
pub fn derive_schema(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let expanded = if input.attrs.iter().any(|attr| attr.path().is_ident("schema")) {
        json::derive(&input)
    } else {
        avro::derive(&input)
    };
    expanded.unwrap_or_else(syn::Error::into_compile_error).into()
}

/// JSON as both sides of a comparison write it: minified, object keys sorted.
pub(crate) fn json_text(value: &Value) -> String {
    fn write(value: &Value, out: &mut String) {
        match value {
            Value::Object(object) => {
                let mut keys: Vec<_> = object.keys().collect();
                keys.sort();
                out.push('{');
                for (index, key) in keys.into_iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(&Value::String(key.clone()).to_string());
                    out.push(':');
                    write(&object[key], out);
                }
                out.push('}');
            }
            Value::Array(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    write(item, out);
                }
                out.push(']');
            }
            other => out.push_str(&other.to_string()),
        }
    }
    let mut out = String::new();
    write(value, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_text_sorts_keys_and_drops_whitespace() {
        let value: Value =
            serde_json::from_str(r#"{ "b": [1, {"d": 2, "c": null}], "a": "x" }"#).expect("JSON");
        assert_eq!(json_text(&value), r#"{"a":"x","b":[1,{"c":null,"d":2}]}"#);
    }
}
