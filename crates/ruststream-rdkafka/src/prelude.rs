//! The imports a service on Kafka writes every time, in one glob.
//!
//! `use ruststream_rdkafka::prelude::*;` carries the core's own prelude plus this crate's broker,
//! subscription descriptor and its options, publish policies, reply transform, per-delivery
//! context keys, and the capability traits a handler names.
//!
//! # Policy names
//!
//! Publish policies are exported under their concept names:
//!
//! | Prelude name | Type |
//! | --- | --- |
//! | [`Publish`] | [`KafkaPublish`](crate::KafkaPublish) |
//! | [`TransactionalPublish`] | [`KafkaTransactionalPublish`](crate::KafkaTransactionalPublish) |
//! | [`PartitionedPublish`] | [`KafkaPartitionedPublish`](crate::KafkaPartitionedPublish) |
//! | [`EosPublish`] | [`KafkaEosPublish`](crate::KafkaEosPublish) |
//!
//! [`Publish`] is a publish policy, not the core's `runtime::Publish` builder.
//!
//! Globbing two broker preludes conflicts on these names where one is used (E0659); the prefixed
//! types above are the disambiguation.
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
//! let replies = Publish::default();
//! let shipments: TransactionalPublish = Publish::default().transactional_id("shipments-svc-1");
//! let lanes: PartitionedPublish = shipments.clone().per_partition();
//! let pipeline = EosPublish::new("enrich-svc-1");
//! # let _ = (broker, orders, replies, shipments, lanes, pipeline);
//! ```

pub use ruststream::prelude::*;

pub use ruststream::{Positioned, Seeker, TransactionalPublisher};

pub use crate::context::keys::{Partition, Source};
pub use crate::{
    Assignment, Commit, KafkaBroker, KafkaEosPublish as EosPublish,
    KafkaPartitionedPublish as PartitionedPublish, KafkaPosition, KafkaPublish as Publish,
    KafkaSeeker, KafkaTopic, KafkaTransactionalPublish as TransactionalPublish, LaneKey,
    PartitionLanes, Retry, RoundRobin, StartOffset,
};

// `Partitioned` stays out: the core's defaulted `IncomingMessage::partition_key` is already in
// scope, so re-exporting the trait makes the natural call ambiguous (E0034).
