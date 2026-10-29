//! Schemas as code: a message type checked against the schema migrations of its subjects when
//! the crate compiles.
//!
//! - [`Schema`](derive@Schema): the derive that binds a type to its migrations by its record's
//!   full name and checks it against the newest migration of every subject in `schemas/` that
//!   declares the record (feature `schema-check`).
//! - [`Record`], [`RecordSet`], [`RecordKind`]: what the derive tells the runtime, the record a
//!   type is and the record of each variant of a set of input models.
//! - [`avro`]: the traits the derive's Avro assertions name.
//! - [Schema checking][checking]: `Schema`, the namespace and subjects a descriptor's
//!   `.schema(..)` step names; the broker checks them against its registry at startup
//!   (feature `schema-registry`).
//!
//! # Examples
//!
//! The crate's `schemas/orders-value/` holds the migrations of the record
//! `com.acme.orders.Order`; its newest migration declares `id`, the nested record `Item` and an
//! optional `note`. The model below equals it, so the crate compiles; an `i32` id, a missing
//! field or an `Item` under another name fails the build.
//!
//! ```
//! # #[cfg(all(feature = "avro", feature = "schema-check"))]
//! # mod demo {
//! use ruststream_rdkafka::avro::{AvroCodec, AvroSchema};
//! use ruststream_rdkafka::prelude::*;
//! use ruststream_rdkafka::schema::Schema;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, AvroSchema, Schema)]
//! #[avro(namespace = "com.acme.orders")] // the record is com.acme.orders.Order
//! pub struct Order {
//!     pub id: i64,
//!     pub item: Item,
//!     pub note: Option<String>,
//! }
//!
//! #[derive(Serialize, Deserialize, AvroSchema, Schema)]
//! #[avro(namespace = "com.acme.orders")]
//! pub struct Item {
//!     pub sku: String,
//! }
//!
//! #[subscriber("orders")]
//! async fn on_order(order: &Order) -> HandlerOutcome {
//!     tracing::info!(order.id, order.item.sku, "order received");
//!     HandlerOutcome::ack()
//! }
//!
//! fn app() -> impl App {
//!     let codec = AvroCodec::local(Order::get_schema()).expect("a derived schema resolves");
//!     let broker = KafkaBroker::new(["localhost:9092"]).default_group("orders-svc");
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_codec(broker, codec, |b| {
//!         b.include(on_order);
//!     })
//! }
//! # }
//! # fn main() {}
//! ```
//!
//! A field that drifts from the migration is a compile error naming the site:
//!
//! ```compile_fail
//! # #[cfg(all(feature = "avro", feature = "schema-check"))]
//! # mod demo {
//! use ruststream_rdkafka::avro::AvroSchema;
//! use ruststream_rdkafka::schema::Schema;
//! use serde::Serialize;
//!
//! #[derive(Serialize, AvroSchema, Schema)]
//! #[avro(namespace = "com.acme.orders")]
//! pub struct Order {
//!     pub id: i32, // `i32` cannot carry the Avro type `long` declared at
//!                  // `Order__id__in__orders_value__0002_add_item`
//!     pub item: Item,
//!     pub note: Option<String>,
//! }
//!
//! #[derive(Serialize, AvroSchema, Schema)]
//! #[avro(namespace = "com.acme.orders")]
//! pub struct Item {
//!     pub sku: String,
//! }
//! # }
//! # #[cfg(not(all(feature = "avro", feature = "schema-check")))]
//! # compile_error!("the example needs the avro and schema-check features");
//! # fn main() {}
//! ```
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
pub(crate) use runtime::{SchemaWatch, check_topic};

#[cfg(feature = "schema-check")]
pub mod avro;
#[cfg(feature = "schema-check")]
mod record;
#[cfg(feature = "schema-check")]
pub use record::{Record, RecordKind, RecordSet};

#[cfg(feature = "schema-check")]
#[doc(hidden)]
pub mod __private;
