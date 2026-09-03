//! The imports a service on Kafka writes every time, in one glob.
//!
//! `use ruststream_rdkafka::prelude::*;` carries the core's own prelude plus this crate's broker,
//! subscription descriptor and its options, publish policies, reply transform, per-delivery
//! context keys, and the capability traits a handler names.
//!
//! # Two vocabularies, two files
//!
//! A handler body imports `ruststream::prelude::*` and bounds an injected slot with the
//! **capability trait** it needs - `Out<impl Publisher>`, `Out<impl TransactionalPublisher>`,
//! `Out<impl OwnedTransactions>`, `Out<impl RequestReply>` - so it names no broker type at all.
//! A mount site imports this prelude, which carries the core one plus the **policies** under
//! their concept names: [`Publish`], [`TransactionalPublish`], [`PartitionedPublish`],
//! [`EosPublish`]. Include sites therefore read the same on every broker, and the two
//! vocabularies never meet in one file.
//!
//! | Prelude name | Type |
//! | --- | --- |
//! | [`Publish`] | [`KafkaPublish`](crate::KafkaPublish) |
//! | [`TransactionalPublish`] | [`KafkaTransactionalPublish`](crate::KafkaTransactionalPublish) |
//! | [`PartitionedPublish`] | [`KafkaPartitionedPublish`](crate::KafkaPartitionedPublish) |
//! | [`EosPublish`] | [`KafkaEosPublish`](crate::KafkaEosPublish) |
//!
//! Globbing two broker preludes conflicts on these names where one is used (E0659); the prefixed
//! types at the crate root are the disambiguation.
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

pub use crate::context::keys::{Partition, Position, SeekHandle, Source};
pub use crate::{
    Assignment, Commit, KafkaBroker, KafkaEosPublish as EosPublish,
    KafkaPartitionedPublish as PartitionedPublish, KafkaPosition, KafkaPublish as Publish,
    KafkaSeeker, KafkaTopic, KafkaTransactionalPublish as TransactionalPublish, LaneKey,
    PartitionLanes, Retry, RoundRobin, StartOffset,
};

// `Partitioned` stays out: the core's defaulted `IncomingMessage::partition_key` is already in
// scope, so re-exporting the trait makes the natural call ambiguous (E0034).
