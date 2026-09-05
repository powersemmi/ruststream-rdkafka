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
//! # The lifecycle ladder
//!
//! Each state of the connection is its own type, so out-of-order use does not compile:
//!
//! ```text
//! KafkaBroker::new(servers)      configuration only, synchronous, no I/O
//!   .connect()   -> ConnectedKafkaBroker   the live connection: subscriptions and publishers
//!   .shutdown()  -> ClosedKafkaBroker      the terminal witness, carrying the flush result
//! ```
//!
//! [`KafkaBroker::new`] being synchronous is what lets a service compose with the synchronous
//! `#[ruststream::app]` builder; the runtime calls `connect` once at startup.
//!
//! Publishers follow the same split: [`KafkaPublish`] (and its transactional and per-partition
//! transitions) is pure policy, constructible anywhere, with no publish surface at all; pairing
//! it with the connected broker produces the live [`KafkaPublisher`]. Handles paired before a
//! shutdown keep aliasing the closed connection, so their operations report
//! [`KafkaError::Closed`] rather than succeeding against a dead connection.
//!
//! One publisher stands outside that split: [`KafkaRetryPublisher`], minted from
//! the unconnected broker for builder-time wiring that takes a live publisher rather than a
//! policy - `retry_via`, the deferred republish behind `retry_after` that Kafka needs because
//! it has no native delayed redelivery. See its documentation.
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
mod seek;
mod subscriber;
mod topic;
mod tracker;

#[cfg(feature = "avro")]
pub mod avro;
pub mod context;
pub mod prelude;
#[cfg(feature = "protobuf")]
pub mod protobuf;
#[cfg(feature = "schema-registry")]
pub mod schema_registry;
#[cfg(feature = "testing")]
pub mod testing;

pub use broker::{ClosedKafkaBroker, ConnectedKafkaBroker, KafkaBroker};
pub use distribution::RoundRobin;
pub use eos::{EOS_SOURCE_HEADER, EosPipeline, EosReplies, KafkaEosPublish, SourceOffset};
pub use error::KafkaError;
pub use message::{KafkaMessage, PARTITION_HEADER, PARTITION_KEY_HEADER};
pub use publisher::{
    KafkaPartitionedPublish, KafkaPublish, KafkaPublisher, KafkaRetryPublisher,
    KafkaTransactionalPublish, KafkaTransactionalPublisher, PartitionLanes,
    TransactionalPartitions,
};
#[cfg(feature = "schema-registry")]
pub use schema_registry::{
    RegisteredSchema, SchemaFrame, SchemaRegistry, SchemaType, SubjectStrategy,
};
pub use seek::{KafkaPosition, KafkaSeeker};

pub use retry::{
    DLQ_SOURCE_OFFSET_HEADER, DLQ_SOURCE_PARTITION_HEADER, DLQ_SOURCE_TOPIC_HEADER,
    RETRY_COUNT_HEADER, Retry,
};
pub use subscriber::KafkaSubscriber;
pub use topic::{Assignment, Commit, KafkaTopic, LaneKey, StartOffset};
