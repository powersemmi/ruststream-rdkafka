//! In-process test broker, behind the `testing` feature.
//!
//! [`KafkaTestBroker`] implements the core `TestableBroker` contract over an in-memory router
//! so application handlers wired against Kafka descriptors can be exercised without a cluster:
//! messages fan out synchronously to subscribers matched by exact topic name.
//!
//! Scope: topic-name routing, settlement, headers, and the partition-key header. Consumer
//! groups, partitions, committed offsets, start offsets, and rebalancing are transport
//! behavior; exercise them against a real cluster (see the crate's integration tests and
//! `KAFKA_TEST_URL`).

mod broker;
mod publisher;
mod router;
mod subscriber;

pub use broker::KafkaTestBroker;
pub use publisher::KafkaTestPublisher;
pub use subscriber::{KafkaTestMessage, KafkaTestSubscriber};
