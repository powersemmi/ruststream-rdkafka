//! In-process test broker, behind the `testing` feature.
//!
//! [`KafkaTestBroker`] follows the same ladder as the real broker and implements the core
//! `TestableBroker` contract on its connected form, over an in-memory router, so application
//! handlers wired against Kafka descriptors can be exercised without a cluster: messages fan
//! out synchronously to subscribers matched by exact topic name. The real
//! [`KafkaPublish`](crate::KafkaPublish) policy pairs against it too, so include sites need no
//! test-only variant.
//!
//! Scope: topic-name routing, settlement, headers, and the partition-key header. Consumer
//! groups, partitions, committed offsets, start offsets, rebalancing, and everything
//! transactional (transactions and the exactly-once pipeline) are transport behavior; exercise
//! them against a real cluster (see the crate's integration tests and `KAFKA_TEST_URL`).

mod broker;
mod publisher;
mod router;
mod subscriber;

pub use broker::{ConnectedKafkaTestBroker, KafkaTestBroker};
pub use publisher::KafkaTestPublisher;
pub use subscriber::{KafkaTestMessage, KafkaTestSubscriber};
