//! Apache Kafka broker for the [RustStream](https://github.com/powersemmi/ruststream) messaging
//! framework, backed by [`rdkafka`] / librdkafka.
//!
//! # Transport model
//!
//! A subscription is one consumer joining one consumer group on one topic; [`KafkaTopic`]
//! describes it, and the bare-string `#[subscriber("orders")]` form consumes the topic named
//! `orders` through [`KafkaBroker::default_group`]. On the publish side
//! [`OutgoingMessage::name`](ruststream::OutgoingMessage) is the destination topic, and a
//! [`PARTITION_KEY_HEADER`] header becomes the record's native key, so Kafka itself keeps
//! per-key ordering.
//!
//! Settlement follows Kafka's committed-position model instead of per-message frames; the
//! [`Commit`] mode picks how:
//!
//! - [`Commit::Auto`] (the default): librdkafka auto-commit; `ack` and both `nack` forms are
//!   advisory no-ops (the position is stored when a message is handed to the application, so
//!   `nack(true)` does not cause a redelivery).
//! - [`Commit::Tracked`]: precise at-least-once. `ack` settles its delivery and the stored
//!   position advances across everything settled below it, staying correct under concurrent
//!   handler lanes and across offset gaps (transaction markers, compacted topics).
//!   `nack(false)` settles the offset (drop); `nack(true)` leaves it unsettled, so Kafka
//!   redelivers from the committed position on the next fetch of the partition.
//!
//! Configuration delegates to librdkafka: unset options mean librdkafka defaults, and the raw
//! `config(key, value)` passthroughs on the broker, the producer, and the descriptor reach
//! every property this crate does not surface as a typed option.
//!
//! # Lazy startup
//!
//! [`KafkaBroker::new`] is synchronous and I/O-free, so a service composes with the synchronous
//! `#[ruststream::app]` builder; the real network work happens in the idempotent async
//! `Broker::connect`, called once by the runtime at startup. Publishers handed out before that
//! resolve the shared connection on first use.
//!
//! [`rdkafka`]: https://docs.rs/rdkafka

#![forbid(unsafe_code)]

mod broker;
mod convert;
mod distribution;
mod eos;
mod error;
mod message;
mod publisher;
mod retry;
mod subscriber;
mod topic;
mod tracker;

#[cfg(feature = "avro")]
pub mod avro;
pub mod context;
#[cfg(feature = "protobuf")]
pub mod protobuf;
#[cfg(feature = "schema-registry")]
pub mod schema_registry;
#[cfg(feature = "testing")]
pub mod testing;

pub use broker::KafkaBroker;
pub use distribution::RoundRobin;
pub use eos::{EOS_SOURCE_HEADER, EosPipeline, EosReplies, SourceOffset};
pub use error::KafkaError;
pub use message::{KafkaMessage, PARTITION_HEADER, PARTITION_KEY_HEADER};
pub use publisher::{KafkaPublisher, TransactionalPartitions};
#[cfg(feature = "schema-registry")]
pub use schema_registry::{
    RegisteredSchema, SchemaFrame, SchemaRegistry, SchemaType, SubjectStrategy,
};

pub use retry::{
    DLQ_SOURCE_OFFSET_HEADER, DLQ_SOURCE_PARTITION_HEADER, DLQ_SOURCE_TOPIC_HEADER,
    RETRY_COUNT_HEADER, Retry,
};
pub use subscriber::KafkaSubscriber;
pub use topic::{Assignment, Commit, KafkaTopic, LaneKey, StartOffset};
