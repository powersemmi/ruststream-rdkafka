//! In-process test broker, behind the `testing` feature.
//!
//! [`KafkaTestBroker`] follows the same ladder as the real broker and implements the core
//! `TestableBroker` contract on its connected form, over an in-memory router, so application
//! handlers wired against Kafka descriptors can be exercised without a cluster: messages fan
//! out synchronously to subscribers matched by exact topic name. The real
//! [`KafkaPublish`](crate::KafkaPublish) policy pairs against it too, so include sites need no
//! test-only variant.
//!
//! The transport retains what it routes, which is what gives it the crate's own position
//! vocabulary: a delivery reports its topic and its index in that topic's log as its offset, and
//! a subscription is seekable over the log through the same [`KafkaSeeker`](crate::KafkaSeeker)
//! and [`KafkaContext`](crate::context::KafkaContext) keys the real broker publishes. So a
//! service that reads `Ctx<Partition>`, replays with `Ctx<SeekHandle>` or opens at a
//! `start_at(..)` position mounts here unchanged and is tested with `TestApp`.
//!
//! Scope: topic-name routing, settlement, headers, the partition-key header, and repositioning
//! over the retained log. Consumer groups, real partitions, committed offsets, record
//! timestamps, rebalancing, and everything transactional (transactions and the exactly-once
//! pipeline) are cluster behavior; exercise them against a real Kafka (see the crate's
//! integration tests and `KAFKA_TEST_URL`). Where a position cannot be resolved honestly here -
//! a timestamp, a partition other than zero - the seek reports an error instead of pretending.

mod broker;
mod publisher;
mod router;
pub(crate) mod seek;
mod subscriber;

pub use broker::{ConnectedKafkaTestBroker, KafkaTestBroker};
pub use publisher::KafkaTestPublisher;
pub use subscriber::{KafkaTestMessage, KafkaTestSubscriber};
