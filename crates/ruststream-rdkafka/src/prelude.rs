//! The imports a service on Kafka writes every time, in one glob.
//!
//! `use ruststream_rdkafka::prelude::*;` carries the core's own prelude plus this crate's broker,
//! subscription descriptor and its options, publish policies, reply transform, per-delivery
//! context keys, and the capability traits a handler names.
//!
//! # Policy names
//!
//! Publish policies keep their `Kafka` prefix here - [`KafkaPublish`](crate::KafkaPublish),
//! [`KafkaTransactionalPublish`](crate::KafkaTransactionalPublish),
//! [`KafkaPartitionedPublish`](crate::KafkaPartitionedPublish),
//! [`KafkaEosPublish`](crate::KafkaEosPublish). The unprefixed concept names belong to the core:
//! `Publish` is its slot capability trait, and an alias here would shadow it silently (an
//! explicit re-export wins over the glob above), turning `fn f<T: Publish>()` in a service into
//! `E0404: expected trait, found struct`. The prefix also means two broker preludes can be
//! globbed side by side without an E0659 ambiguity on a policy name.
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
//! let replies = KafkaPublish::default();
//! let shipments = KafkaPublish::default().transactional_id("shipments-svc-1");
//! let lanes: KafkaPartitionedPublish = shipments.clone().per_partition();
//! let pipeline = KafkaEosPublish::new("enrich-svc-1");
//! # let _ = (broker, orders, replies, shipments, lanes, pipeline);
//! ```

pub use ruststream::prelude::*;

pub use ruststream::{Positioned, Seeker, TransactionalPublisher};

pub use crate::context::keys::{Partition, Position, SeekHandle, Source};
pub use crate::{
    Assignment, Commit, KafkaBroker, KafkaEosPublish, KafkaPartitionedPublish, KafkaPosition,
    KafkaPublish, KafkaSeeker, KafkaTopic, KafkaTransactionalPublish, LaneKey, PartitionLanes,
    Retry, RoundRobin, StartOffset,
};

// `Partitioned` stays out: the core's defaulted `IncomingMessage::partition_key` is already in
// scope, so re-exporting the trait makes the natural call ambiguous (E0034).
