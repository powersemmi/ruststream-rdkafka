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
//! `Out<impl PartitionLanes>` - so it names no broker type at all. A mount site imports this
//! prelude, which carries the core one plus the **policies** under their concept names:
//! [`Publish`], [`TransactionalPublish`], [`PartitionedPublish`], [`EosPublish`]. Include sites
//! therefore read the same on every broker, and the two vocabularies never meet in one file.
//!
//! | Prelude name | Type |
//! | --- | --- |
//! | [`Publish`] | [`KafkaPublish`](crate::KafkaPublish) |
//! | [`TransactionalPublish`] | [`KafkaTransactionalPublish`](crate::KafkaTransactionalPublish) |
//! | [`PartitionedPublish`] | [`KafkaPartitionedPublish`](crate::KafkaPartitionedPublish) |
//! | [`EosPublish`] | [`KafkaEosPublish`](crate::KafkaEosPublish) |
//!
//! One handler body imports this prelude too: the one that adjusts a per-record setting. Naming
//! [`KafkaPublishSteps`]'s `partition(..)` step needs the trait in
//! scope and the slot bounded as `Out<impl Publisher<Options = KafkaOptions>, Marker>`, and that
//! bound is the stated exception to the rule above.
//!
//! Globbing two broker preludes conflicts on these names where one is used (E0659); the prefixed
//! types at the crate root are the disambiguation.
//!
//! # Examples
//!
//! ```
//! use ruststream_rdkafka::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[derive(Serialize, Outgoing)]
//! #[outgoing(name = "confirmations")]
//! struct Confirmation {
//!     id: u64,
//! }
//!
//! #[subscriber(
//!     KafkaTopic::new("orders")
//!         .commit(Commit::Tracked)
//!         .start(StartOffset::Earliest),
//!     publish
//! )]
//! async fn confirm(order: &Order) -> Confirmation {
//!     Confirmation { id: order.id }
//! }
//!
//! fn app() -> RustStream {
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
//!         KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
//!         |b| {
//!             b.include(confirm).out_reply(Publish::default());
//!         },
//!     )
//! }
//! # let _ = app;
//! ```

pub use ruststream::prelude::*;

pub use ruststream::{Positioned, Seeker, TransactionalPublisher};

pub use crate::context::keys::{Partition, Position, SeekHandle, Source, Topic};
pub use crate::{
    Assignment, Commit, EosReplies, KafkaBroker, KafkaEosPublish as EosPublish, KafkaOptions,
    KafkaPartitionedPublish as PartitionedPublish, KafkaPartitions, KafkaPosition,
    KafkaPublish as Publish, KafkaPublishSteps, KafkaSeeker, KafkaTopic, KafkaTopics,
    KafkaTransactionalPublish as TransactionalPublish, LaneKey, PartitionLanes, RoundRobin,
    StartOffset, ToSourceTopic,
};

// `Partitioned` stays out: the core's defaulted `IncomingMessage::partition_key` is already in
// scope, so re-exporting the trait makes the natural call ambiguous (E0034).
