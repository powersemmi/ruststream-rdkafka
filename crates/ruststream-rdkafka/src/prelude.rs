//! The imports a service on Kafka writes every time, in one glob.
//!
//! `use ruststream_rdkafka::prelude::*;` brings in the core's own prelude (the application
//! object and its builder, the handler surface, the publishing types, and - with the `macros`
//! feature - the attribute macros and derives) plus this crate's user-facing surface: the
//! broker, the subscription descriptor and its options, the publish policies in every mode, the
//! reply transform, and the per-delivery context keys a Kafka handler reads.
//!
//! # The policy vocabulary
//!
//! Publish policies arrive under their concept names, with the broker prefix stripped:
//! [`Publish`], [`TransactionalPublish`], [`PartitionedPublish`], [`EosPublish`]. An include site
//! therefore reads the same on every broker - `.publisher(Publish)`, `.out(Shipments, Publish)` -
//! and moving a service between brokers touches the descriptor, not the wiring.
//!
//! The stripping is only of the prefix. Which mode a site picked stays in the name, because that
//! is the choice a reader has to see: `TransactionalPublish` is not `Publish`, and `EosPublish`
//! is not either. And the list obeys the same rule as the capability manifest below, one layer
//! down: every policy this broker supports appears under its concept name, so a concept name that
//! is absent is a mode the broker does not have.
//!
//! Two names deserve a note, once:
//!
//! - [`Publish`] here is a *policy* - pure declaration, paired into a live publisher at startup.
//!   It is not the core's `runtime::Publish`, the builder returned mid-call by `message(..)` /
//!   `raw(..)`, which a service never names (its own documentation says so), which is why the two
//!   do not meet.
//! - [`TransactionalPublish`] is that policy in its transactional mode;
//!   [`TransactionalPublisher`] is the core capability trait a handler writes in a bound. The
//!   suffix is the whole distinction, and it is the fleet convention: policies end in `Publish`,
//!   capabilities in `Publisher`.
//!
//! It is also this broker's capability manifest, drawn where a service can use it: a core
//! capability trait is re-exported when a service writes it in a bound, or calls its methods on
//! a value the runtime hands it. So a capability the broker cannot honour is never in scope -
//! [`RequestReply`](ruststream::RequestReply) is the visible absence, since Kafka has no
//! request-reply primitive. The reverse does not follow: a trait the broker implements can still
//! be missing here, either because only the runtime's plumbing consumes it or because the core
//! already surfaces its method elsewhere. Each such case is named in the comments below, so the
//! list stays a statement about what a service writes rather than about what the crate impls.
//!
//! A service on two brokers globs both preludes safely as far as the capabilities go: those are
//! the same core items, so the overlap resolves to one item rather than a conflict. The policy
//! names above are the deliberate exception - two brokers both export `Publish`, and glob
//! imports only fall out at the point one is *used* (E0659), not at the import. That is what the
//! prefixed originals at the crate root are for: a file mixing brokers writes
//! [`KafkaPublish`](crate::KafkaPublish) and the other crate's own spelling, and the ambiguity is
//! gone.
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
//!
//! // The policy vocabulary, under the names every broker's prelude uses.
//! let replies = Publish::default();
//! let shipments: TransactionalPublish = Publish::default().transactional_id("shipments-svc-1");
//! let lanes: PartitionedPublish = shipments.clone().per_partition();
//! let pipeline = EosPublish::new("enrich-svc-1");
//! # let _ = (broker, orders, replies, shipments, lanes, pipeline);
//! ```

// The core's prelude says brokers stay explicit imports, because which broker a service runs on
// is the one thing every service states for itself. Importing *this* prelude is that statement:
// the broker-specificity has moved into the crate path, so the core glob rides along and one
// import line serves a whole service file.
pub use ruststream::prelude::*;

// The capability manifest: the core capabilities Kafka honours that a service actually names -
// `TransactionalPublisher` in a bound on a handler's publisher, and `Seeker` and `Positioned`
// for the methods a handler calls on what it is handed (`seeker.seek(..)` on an injected
// `Seek<..>`, `message.position()`). Kafka runs no request-reply protocol and hands out no owned
// transaction scopes, so `RequestReply` and `OwnedTransactions` are absent - a handler that
// reaches for one gets "not in scope" here rather than a runtime failure on a broker that cannot
// do it.
pub use ruststream::{Positioned, Seeker, TransactionalPublisher};

// The two context keys the per-partition paths name (`Ctx<Partition>` on a lane-scoped handler,
// `Ctx<Source>` for an exactly-once publish). The rest of the delivery's fields stay behind
// `context::keys`, whose names (`Key`, `Topic`, `Offset`) a service is more likely to own.
pub use crate::context::keys::{Partition, Source};
// The policy vocabulary: the concept names, so an include site reads identically on every
// broker. The prefixed originals stay exported at the crate root, which is what a file mixing
// two brokers reaches for.
pub use crate::{
    KafkaEosPublish as EosPublish, KafkaPartitionedPublish as PartitionedPublish,
    KafkaPublish as Publish, KafkaTransactionalPublish as TransactionalPublish,
};

// `PartitionLanes` rides beside the core capabilities above as this broker's own: the core's
// vocabulary has no name for a producer cache, so the capability is declared here and named the
// same way in an `Out` slot. `KafkaTopic` keeps its prefix: it is a descriptor, not a policy, and
// this broker has one descriptor family rather than a set to choose a concept name within.
pub use crate::{
    Assignment, Commit, KafkaBroker, KafkaPosition, KafkaSeeker, KafkaTopic, LaneKey,
    PartitionLanes, Retry, RoundRobin, StartOffset,
};

// Deliberately absent:
//
// - `Partitioned`, implemented here, but the core surfaces `partition_key` through
//   `IncomingMessage`'s defaulted method - re-exporting the trait would make the natural call
//   ambiguous (E0034) on a concrete delivery and force every caller into UFCS.
// - `Seekable` and `BatchSubscriber`, both implemented here: they sit on the subscriber, which
//   the runtime's plumbing consumes. A service names the seeker type (`KafkaSeeker`) and
//   declares the batch form in the subscriber attribute, never these traits.
// - `Subscribe`, `DefaultPublish` and `DescribeServer`: contract machinery the runtime calls,
//   not vocabulary a service writes.
// - The `testing` module: broker-author and test tooling behind its own feature, not the API a
//   service writes against, so it is imported where a test needs it.
// - The record-level machinery - the header constants, the live publisher and subscriber types,
//   and the connected and closed broker forms. A service publishes through the builder and
//   receives its publishers through `Out`, so it never names them; a publish transform, a
//   middleware, or this crate used on its own does, and says by that import which layer it is
//   working at.
// - `KafkaError`: a service names errors where it handles them, not at the top of every file.
