//! The imports a service on Kafka writes every time, in one glob.
//!
//! `use ruststream_rdkafka::prelude::*;` brings in the core's own prelude (the application
//! object and its builder, the handler surface, the publishing types, and - with the `macros`
//! feature - the attribute macros and derives) plus this crate's user-facing surface: the
//! broker, the subscription descriptor and its options, the publish policies in every mode, the
//! reply transform, the per-delivery context keys a Kafka handler reads, and the capability an
//! `Out` slot names.
//!
//! It deliberately stops there. The optional integration modules (`schema_registry`, `avro`,
//! `protobuf`) stay explicit imports, exactly as the core keeps its own feature modules out of
//! its prelude: a service reaches them through their module path, and says by that import which
//! integration it has taken on.
//!
//! # Examples
//!
//! ```
//! use ruststream_rdkafka::prelude::*;
//!
//! fn broker() -> KafkaBroker {
//!     KafkaBroker::new(["localhost:9092"]).default_group("orders-svc")
//! }
//!
//! let orders = KafkaTopic::new("orders")
//!     .commit(Commit::Tracked)
//!     .start(StartOffset::Earliest);
//! # let _ = (broker, orders);
//! ```

// The core's prelude says brokers stay explicit imports, because which broker a service runs on
// is the one thing every service states for itself. Importing *this* prelude is that statement:
// the broker-specificity has moved into the crate path, so the core glob rides along and one
// import line serves a whole service file.
pub use ruststream::prelude::*;

// The two context keys the per-partition paths name (`Ctx<Partition>` on a lane-scoped handler,
// `Ctx<Source>` for an exactly-once publish). The rest of the delivery's fields stay behind
// `context::keys`, whose names (`Key`, `Topic`, `Offset`) a service is more likely to own.
pub use crate::context::keys::{Partition, Source};
pub use crate::{
    Assignment, Commit, KafkaBroker, KafkaEosPublish, KafkaPartitionedPublish, KafkaPosition,
    KafkaPublish, KafkaSeeker, KafkaTopic, KafkaTransactionalPublish, LaneKey, PartitionLanes,
    Retry, RoundRobin, StartOffset,
};

// Three groups are deliberately absent:
//
// - The `testing` module: broker-author and test tooling behind its own feature, not the API a
//   service writes against, so it is imported where a test needs it.
// - The record-level machinery - the header constants, the live publisher and subscriber types,
//   and the connected and closed broker forms. A service publishes through the builder and
//   receives its publishers through `Out`, so it never names them; a publish transform, a
//   middleware, or this crate used on its own does, and says by that import which layer it is
//   working at.
// - `KafkaError`: a service names errors where it handles them, not at the top of every file.
