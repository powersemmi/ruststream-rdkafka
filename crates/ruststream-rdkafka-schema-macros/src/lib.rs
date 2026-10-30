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
/// The type is bound to its migrations by its record's full name, the name `AvroSchema` gives
/// it: `#[avro(namespace = ..)]` plus serde's `rename` or the type's name
/// (`com.acme.orders.Order`). The derive reads `schemas/` beside the crate's `Cargo.toml`
/// (`RUSTSTREAM_SCHEMA_DIR` names another directory) and checks the type against the newest
/// migration of every subject that declares the record. A type no migration declares carries
/// no check.
///
/// - An owned subject: the type equals the migration. Field names, kinds, nullability (an
///   `Option` against `["null", T]`), nested record names, `#[avro(default = ..)]` defaults and
///   the whole canonical form are compared. A field with `#[avro(with = ..)]` or
///   `#[serde(flatten)]` is checked for presence only.
/// - An external subject (`owner = "external"` in `subject.toml`): the type reads every pulled
///   version under Avro's resolution rules.
/// - A unit-variant enum is an Avro enum: its symbols equal the migration's.
/// - An enum whose variants each wrap one model (`#[derive(InMessages)]`) maps each variant to
///   its model's record; each model is checked by its own derive.
///
/// A mismatch is a compile error that names the field, the subject and the migration. The check
/// is constants, uncalled closures and trait bounds, so nothing of it reaches the binary.
///
/// Switches, read when the crate compiles: `RUSTSTREAM_SCHEMA_EXPORT=<dir>` emits no checks and
/// lets `cargo kafka-schema` export each type's schema through a hidden test;
/// `RUSTSTREAM_SCHEMA_REGISTRY=<url>` also compares the lock with the registry's versions
/// (`RUSTSTREAM_SCHEMA_OFFLINE=true` turns that off).
///
/// # Examples
///
/// ```
/// use ruststream_rdkafka::avro::{AvroCodec, AvroSchema};
/// use ruststream_rdkafka::prelude::*;
/// use ruststream_rdkafka::schema::Schema;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, AvroSchema, Schema)]
/// #[avro(namespace = "com.acme.orders")] // the record is com.acme.orders.Order
/// pub struct Order {
///     pub id: i64,
///     pub item: Item,
///     pub note: Option<String>,
/// }
///
/// #[derive(Serialize, Deserialize, AvroSchema, Schema)]
/// #[avro(namespace = "com.acme.orders")]
/// pub struct Item {
///     pub sku: String,
/// }
///
/// #[subscriber("orders")]
/// async fn on_order(order: &Order) -> HandlerOutcome {
///     let _ = (order.id, &order.item.sku);
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> impl App {
///     let codec = AvroCodec::local(Order::get_schema()).expect("a derived schema resolves");
///     let broker = KafkaBroker::new(["localhost:9092"]).default_group("orders-svc");
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_codec(broker, codec, |b| {
///         b.include(on_order);
///     })
/// }
/// # fn main() {}
/// ```
#[proc_macro_derive(Schema, attributes(schema))]
pub fn derive_schema(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let expanded = if json::has_namespace(&input) {
        json::derive(&input)
    } else {
        avro::derive(&input)
    };
    expanded
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
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
