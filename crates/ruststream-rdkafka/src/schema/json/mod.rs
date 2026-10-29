//! The JSON Schema check: the serde shape of a type against the schema of every subject that
//! declares its record.
//!
//! ```
//! use std::collections::BTreeMap;
//!
//! use ruststream::codec::JsonCodec;
//! use ruststream_rdkafka::prelude::*;
//! use ruststream_rdkafka::schema::Schema;
//! use ruststream_rdkafka::{SchemaFramed, SchemaPrefetch, SchemaRegistry};
//! use schemars::JsonSchema;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, JsonSchema, Schema)]
//! #[serde(rename_all = "camelCase")]
//! #[schema(namespace = "com.acme.orders")] // the record is com.acme.orders.Order
//! pub struct Order {
//!     pub order_id: i64,
//!     pub items: Vec<Item>,
//!     pub note: Option<String>,
//!     #[serde(default)]
//!     pub priority: i32,
//!     pub tags: BTreeMap<String, String>,
//! }
//!
//! #[derive(Serialize, Deserialize, JsonSchema, Schema)]
//! #[schema(namespace = "com.acme.orders")]
//! pub struct Item {
//!     pub sku: String,
//!     pub qty: u32,
//! }
//!
//! #[subscriber("orders")]
//! async fn on_order(order: &Order) -> HandlerOutcome {
//!     tracing::info!(order = order.order_id, items = order.items.len(), "received");
//!     HandlerOutcome::ack()
//! }
//!
//! fn app() -> RustStream {
//!     let prefetch = SchemaPrefetch::new(SchemaRegistry::new("http://localhost:8081"));
//!     let codec = SchemaFramed::new(&prefetch, JsonCodec).register::<Order>("orders-value");
//!     let broker = KafkaBroker::new(["localhost:9092"])
//!         .default_group("orders-svc")
//!         .schema_prefetch(prefetch);
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_codec(broker, codec, |b| {
//!         b.include(on_order);
//!     })
//! }
//! # let _ = app;
//! ```
//!
//! JSON Schema has no record name, so `#[schema(namespace = ..)]` gives one: the namespace plus
//! the type name, `com.acme.orders.Order`. A JSON subject names the record it declares with
//! `record = ".."` in its `subject.toml`, and the derive compares the type with the newest
//! migration of every such subject when the crate compiles.
//!
//! The claim is about serde, not about the struct's fields:
//! - every value the type serializes is valid under the schema (an owned subject);
//! - every value the schema admits deserializes into the type (owned and external subjects).
//!
//! The derive reads the type's serde attributes with the parser serde's own derive uses, so
//! `rename`, `rename_all`, `default`, `skip`, `skip_serializing_if`, `flatten`,
//! `deny_unknown_fields`, `transparent` and enum tagging mean what serde makes of them. A field of
//! another type takes that type's [`JsonShape`]: the standard types carry one, and so does every
//! type that derives `Schema` with a namespace. The first difference fails the build with its
//! path:
//!
//! ```text
//! error[E0080]: evaluation panicked: `Order` does not match subject `orders-value`
//!               (migration 0001_create_order): Order.items[].qty: the integer type writes
//!               values outside the schema's range (`u64` holds 0..=18446744073709551615, the
//!               schema admits 0..=4294967295)
//! ```
//!
//! What the check compares:
//! - properties and `required`: property names equal the serde names; a required property is
//!   written on every value, an optional one reads when absent (an `Option` or a default);
//! - null: the schema admits null exactly when the field is an `Option`;
//! - integers: the range the `format` (`int8` .. `uint64`), `minimum` and `maximum` leave equals
//!   the type's, a bare `integer` reading as signed 64-bit;
//! - numbers against `f32` (`float`) and `f64` (`double`), strings by their `format` (`uuid`,
//!   `date-time`);
//! - an `enum` of strings against a unit-only enum, by the names serde gives the variants;
//! - `oneOf` against an externally, internally or adjacently tagged enum, branch for variant;
//! - `items`, the three forms of `additionalProperties`, flattened structs and maps;
//! - `$ref` into `$defs` or `definitions`, and to another subject through the `[[references]]`
//!   of `subject.toml`; drafts 04 to 2020-12.
//!
//! A construct a Rust type cannot promise (`pattern`, lengths, `multipleOf`, `not`, `anyOf` other
//! than "T or null", an untagged enum, a recursive `$ref` or Rust type, tuples, ..) fails the
//! build with the keyword and its JSON pointer. `#[schema(unchecked)]` on the field limits the
//! check to the field's presence and optionality.
//!
//! The check is constants evaluated by the compiler: the binary holds none of it.

// The comparison runs in the compiler's constant evaluation and nowhere else: the binary never
// calls it, so its by-value message buffer costs build time only, and a `const fn` has no
// allocation to shrink it with.
#![allow(clippy::result_large_err, clippy::large_types_passed_by_value)]

mod compare;
mod impls;
mod message;
mod node;
mod shape;

use std::fs;
use std::path::Path;

/// The serde shape of a type, which the `Schema` derive compares with a JSON subject's schema
/// when the crate compiles.
///
/// The standard types carry it: integers, floats, `bool`, `String`, `Option`, `Vec`, maps from
/// `String`, `Box` and `serde_json::Value`, with `uuid::Uuid` and the `chrono` date types under
/// this crate's `uuid` and `chrono` features. A type that derives `Schema` with
/// `#[schema(namespace = ..)]` carries it too, built from its serde attributes and its fields'
/// own shapes.
///
/// # Examples
///
/// ```
/// use ruststream::codec::JsonCodec;
/// use ruststream_rdkafka::prelude::*;
/// use ruststream_rdkafka::schema::Schema;
/// use ruststream_rdkafka::{SchemaFramed, SchemaPrefetch, SchemaRegistry};
/// use schemars::JsonSchema;
/// use serde::{Deserialize, Serialize};
///
/// // `Payment` carries a JSON shape through the derive, so `Invoice` can hold it.
/// #[derive(Serialize, Deserialize, JsonSchema, Schema)]
/// #[serde(tag = "kind", rename_all = "snake_case")]
/// #[schema(namespace = "com.acme.billing")]
/// pub enum Payment {
///     Card { last4: String },
///     Cash,
/// }
///
/// #[derive(Serialize, Deserialize, JsonSchema, Schema, Outgoing)]
/// #[schema(namespace = "com.acme.billing")]
/// #[outgoing(name = "invoices")]
/// pub struct Invoice {
///     pub order_id: i64,
///     pub payment: Payment,
/// }
///
/// #[subscriber("orders", reply)]
/// async fn invoice(order: &Invoice) -> Invoice {
///     Invoice { order_id: order.order_id, payment: Payment::Cash }
/// }
///
/// fn app() -> RustStream {
///     let prefetch = SchemaPrefetch::new(SchemaRegistry::new("http://localhost:8081"));
///     let codec = SchemaFramed::new(&prefetch, JsonCodec).register::<Invoice>("invoices-value");
///     let broker = KafkaBroker::new(["localhost:9092"])
///         .default_group("billing-svc")
///         .schema_prefetch(prefetch);
///     let replies = codec.clone();
///     RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker_codec(broker, codec, |b| {
///         b.include(invoice).out_reply(Publish::default()).codec(replies);
///     })
/// }
/// # let _ = app;
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no JSON shape for the schema check",
    label = "this field's type has no JSON shape",
    note = "derive `Schema` with `#[schema(namespace = \"..\")]` on `{Self}`, or mark the field `#[schema(unchecked)]` to check only its presence and optionality"
)]
pub trait JsonShape {
    /// What `serde_json` makes of the type's values.
    #[doc(hidden)]
    const SHAPE: shape::Shape;
}

/// What the `Schema` derive's generated code names. Not a public API.
#[doc(hidden)]
pub mod __private {
    pub use super::JsonShape;
    pub use super::compare::{Check, Direction, compare};
    pub use super::message::Message;
    pub use super::node::{Additional, Node, Object, Property};
    pub use super::shape::{Enum, Field, Payload, Read, Shape, Struct, Tagging, Variant, Write};

    /// Writes the `schemars` schema of `T` to `$RUSTSTREAM_SCHEMA_EXPORT/<record>.json`: the
    /// export the derive's hidden test runs for `cargo kafka-schema`.
    ///
    /// # Panics
    ///
    /// When the directory cannot be written: the test fails with the reason.
    pub fn export<T: schemars::JsonSchema>(record: &str) {
        super::export::<T>(record);
    }
}

fn export<T: schemars::JsonSchema>(record: &str) {
    if let Some(dir) = std::env::var_os("RUSTSTREAM_SCHEMA_EXPORT") {
        export_to::<T>(Path::new(&dir), record);
    }
}

fn export_to<T: schemars::JsonSchema>(dir: &Path, record: &str) {
    let schema = schemars::SchemaGenerator::default().into_root_schema_for::<T>();
    let text = serde_json::to_string_pretty(&schema)
        .unwrap_or_else(|err| panic!("the JSON Schema of `{record}` does not serialize: {err}"));
    let file = dir.join(format!("{record}.json"));
    fs::create_dir_all(dir)
        .and_then(|()| fs::write(&file, text))
        .unwrap_or_else(|err| {
            panic!(
                "writing the schema of `{record}` to {}: {err}",
                file.display()
            )
        });
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::process;

    use super::*;

    #[derive(schemars::JsonSchema)]
    #[allow(dead_code)]
    struct Order {
        order_id: i64,
    }

    #[test]
    fn the_export_writes_the_schemars_schema_under_the_record_name() {
        let dir = env::temp_dir().join(format!("ruststream-json-export-{}", process::id()));
        export_to::<Order>(&dir, "com.acme.orders.Order");
        let text = fs::read_to_string(dir.join("com.acme.orders.Order.json"))
            .expect("the export wrote the file");
        let schema: serde_json::Value = serde_json::from_str(&text).expect("the export is JSON");
        assert_eq!(schema["properties"]["order_id"]["format"], "int64");
        fs::remove_dir_all(&dir).expect("the scratch directory is removable");
    }
}
