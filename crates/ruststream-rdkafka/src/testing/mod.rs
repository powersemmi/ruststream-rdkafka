//! In-process test broker, behind the `testing` feature.
//!
//! [`KafkaTestBroker`] follows the same ladder as the real broker and implements the core
//! `TestableBroker` contract on its connected form, so the handlers and descriptors under test
//! are the production ones: a record routes synchronously by exact topic name to one member of
//! every consumer group reading it. The crate's real publish policies pair against it -
//! [`KafkaPublish`](crate::KafkaPublish),
//! [`KafkaTransactionalPublish`](crate::KafkaTransactionalPublish) and
//! [`KafkaPartitionedPublish`](crate::KafkaPartitionedPublish) - so an include site needs no
//! test-only variant, and a handler bounded `Out<impl TransactionalPublisher>` or
//! `Out<impl PartitionLanes>` mounts here with its routes file unchanged. Drive it with the
//! core's [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/index.html), whose
//! `publish` returns once every handler it triggered has settled, so a test needs no sleep.
//!
//! Never enable the feature in a production build.
//!
//! # Settlement is a read position
//!
//! `nack(true)` does not re-enqueue one message. It leaves the offset unsettled and resumes the
//! subscription from the committed position, so that record and everything after it are
//! delivered again - the at-least-once duplication a real rewind produces. `ack` and
//! `nack(false)` settle the offset, and the lowest unsettled offset is where a later rewind
//! lands. So the commit mode decides what a retry does, here as on a cluster:
//! [`Commit::Tracked`](crate::Commit::Tracked) rewinds,
//! [`Commit::Auto`](crate::Commit::Auto) (the descriptor default) is advisory and brings nothing
//! back, and [`Commit::Transactional`](crate::Commit::Transactional) rewinds like `Tracked`,
//! there being no pipeline here to defer the commit to. Name the mode on the descriptor whenever
//! a test turns on what a retry does.
//!
//! One deliberate exception: a subscription opened by name rather than by descriptor - the
//! `Subscribe` capability, which the bare `#[subscriber("orders")]` form and the core
//! conformance suite both use - settles like `Commit::Tracked`, because the core routing
//! contract requires `nack(true)` to redeliver and a bare name carries no mode to decide
//! otherwise.
//!
//! # Competing consumers
//!
//! [`KafkaTopic::group`](crate::KafkaTopic::group) carries its meaning here: a record reaches one
//! member of each consumer group, and every group reads its own copy, so replicas sharing a group
//! divide the work in a test the way they divide it on a cluster.
//! [`KafkaTestBroker::default_group`] mirrors the real broker's for subscriptions that name none;
//! set it when the service under test sets one, or two bare `#[subscriber("orders")]` handlers
//! are each alone in a group and both see every record.
//!
//! Which member gets a record is a partition assignment, not a rotation. Every topic here has
//! exactly one partition and Kafka hands a partition to exactly one member, so a group's records
//! all land on the same member - the truth about a one-partition topic, and the reason a test
//! must not assert that two workers each did half. When the owner drops its subscription the next
//! member takes over, which is the effect a rebalance has.
//!
//! # Positions, seeking and per-record settings
//!
//! The transport retains what it routes, which is what gives it this crate's position vocabulary:
//! a delivery reports its topic and its index in that topic's log as its offset, and a
//! subscription is seekable over that log through the same
//! [`KafkaSeeker`](crate::KafkaSeeker) and [`KafkaContext`](crate::context::KafkaContext) keys the
//! real broker publishes. A service that reads `Ctx<Partition>`, replays with `Ctx<SeekHandle>`
//! or opens at a `start_at(..)` position is therefore an ordinary `TestApp` test. A position this
//! transport cannot resolve honestly is refused rather than invented: a timestamp (it stamps
//! none), a partition other than zero, a topic the subscription does not read.
//!
//! A publisher folds a partition setting into the record, so the broker log no longer shows what
//! the call site asked for. The slot view answers that: `tb.out::<Marker>()` reads back the
//! options a publish through that slot carried, and asserts that a publish named none and left
//! the placement to the producer.
//!
//! # What it does not simulate
//!
//! The scope is the core routing contract - topic-name routing, consumer groups, settlement,
//! headers, the partition-key header, worker lanes (a subscription's
//! [`LaneKey`](crate::LaneKey) resolves here exactly as it does upstream) - plus the retained log
//! and the client-visible half of a transaction. Kafka itself is not simulated:
//!
//! - **Partitions.** Every topic has exactly one, numbered zero. `Ctx<Partition>` always reads
//!   `0`, a partition step changes where no record lands, and a seek elsewhere is refused. What a
//!   test cannot show is two keys landing on different partitions, several partitions running
//!   concurrently, or a group's work spreading across its members.
//! - **Committed positions do not outlive their subscription.** A later subscriber in the same
//!   group opens at the end of the log rather than resuming where its predecessor stopped, and
//!   [`StartOffset::Earliest`](crate::StartOffset::Earliest) is inert. Use the mount site's
//!   `start_at(KafkaPosition::earliest())`, and test resume-across-restart against a cluster.
//! - **Retries and dead-lettering do run here**, because the framework performs them rather than
//!   the consumer. What differs is the redelivery underneath: a `nack(true)` that no declaration
//!   covers hands the delivery straight back here, while a cluster returns to it only on the next
//!   fetch of the partition.
//! - **Transactions hold client-side only.** Publishes between `begin_transaction` and `commit`
//!   are held back and released together, an abort discards them, and the misuse contract holds.
//!   Everything the guarantee rests on is broker-side and absent:
//!   [`KafkaTestTransactionalPublisher`] names each gap - atomic `read_committed` visibility (a
//!   commit routes message by message, so a subscriber here can observe a prefix), zombie fencing
//!   by transactional id, broker-held transaction timeouts, and the startup work that pairing the
//!   real policy does.
//! - **Exactly-once is not approximated.** Coupling consumed offsets into the transaction needs a
//!   consumer group and its metadata, so [`KafkaEosPublish`](crate::KafkaEosPublish) does not pair
//!   against this broker at all: mounting such a route on it is a compile error rather than a
//!   green test proving nothing about exactly-once.
//! - **Manual assignment and pattern subscriptions** are refused with
//!   [`KafkaError::InvalidOptions`](crate::KafkaError::InvalidOptions) rather than approximated.
//!
//! All of it is covered against a live cluster by this crate's integration tests, which run under
//! `KAFKA_TEST_URL` (`just brokers-up`, then `just test-brokers`).

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
