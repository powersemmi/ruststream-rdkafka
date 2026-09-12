//! In-process test broker, behind the `testing` feature.
//!
//! [`KafkaTestBroker`] follows the same ladder as the real broker and implements the core
//! `TestableBroker` contract on its connected form, over an in-memory router, so application
//! handlers wired against Kafka descriptors can be exercised without a cluster: a record routes
//! synchronously by exact topic name to one member of every consumer group reading it. The
//! crate's real publish
//! policies pair against it - [`KafkaPublish`](crate::KafkaPublish),
//! [`KafkaTransactionalPublish`](crate::KafkaTransactionalPublish) and
//! [`KafkaPartitionedPublish`](crate::KafkaPartitionedPublish) - so include sites need no
//! test-only variant, and a handler bounded `Out<impl TransactionalPublisher>` or
//! `Out<impl PartitionLanes>` mounts here with its routes file unchanged.
//!
//! The transport retains what it routes, which is what gives it the crate's own position
//! vocabulary: a delivery reports its topic and its index in that topic's log as its offset, and
//! a subscription is seekable over the log through the same [`KafkaSeeker`](crate::KafkaSeeker)
//! and [`KafkaContext`](crate::context::KafkaContext) keys the real broker publishes. So a
//! service that reads `Ctx<Partition>`, replays with `Ctx<SeekHandle>` or opens at a
//! `start_at(..)` position mounts here unchanged and is tested with `TestApp`.
//!
//! Scope: topic-name routing; consumer groups, so competing consumers really compete (a record
//! reaches one member of each group, and every group reads its own copy); settlement as a read
//! position rather than a per-message frame, so `nack(true)` rewinds to the committed position
//! under [`Commit::Tracked`](crate::Commit::Tracked) and is advisory under
//! [`Commit::Auto`](crate::Commit::Auto); headers; the partition-key header; worker lanes (a
//! subscription's [`LaneKey`](crate::LaneKey) resolves here exactly as it does upstream, so
//! `workers(n, by_key)` lanes deliveries the same way); repositioning over the retained log; and
//! the client-visible half of a transaction (publishes held back until commit, discarded on
//! abort, one independent transaction per partition lane). Real partitions (every topic has
//! exactly one, so a group's records land on one member instead of spreading), positions that
//! outlive a subscription, the retry and dead-letter pipeline, record timestamps and rebalancing
//! are cluster behavior,
//! and so is every guarantee a transaction rests on: atomic `read_committed` visibility, zombie
//! fencing by transactional id, broker-held transaction timeouts, and the exactly-once coupling
//! of consumed offsets into the producer transaction. [`KafkaTestTransactionalPublisher`] names
//! those gaps one by one, and [`KafkaEosPublish`](crate::KafkaEosPublish) deliberately does not
//! pair here at all, so mounting an exactly-once route on the stand-in is a compile error rather
//! than a green test proving nothing. Exercise all of it against a real Kafka (see the crate's
//! integration tests and `KAFKA_TEST_URL`). Where a position cannot be resolved honestly here -
//! a timestamp, a partition other than zero - the seek reports an error instead of pretending.

mod broker;
mod publisher;
mod router;
pub(crate) mod seek;
mod subscriber;
mod transaction;

pub use broker::{ConnectedKafkaTestBroker, KafkaTestBroker};
pub use publisher::KafkaTestPublisher;
pub use subscriber::{KafkaTestMessage, KafkaTestSubscriber};
pub use transaction::{KafkaTestPartitions, KafkaTestTransactionalPublisher};
