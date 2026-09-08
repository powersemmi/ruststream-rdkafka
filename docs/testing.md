# Testing

The `testing` feature ships `KafkaTestBroker`, an in-process stand-in for Kafka: the same
handlers and descriptors, no cluster. It follows the same ladder as the real broker
(`new` -> `connect` -> `shutdown`) and the crate's real publish policies pair against its
connected form - `KafkaPublish`, `KafkaTransactionalPublish` and `KafkaPartitionedPublish` - so
include sites are identical for both brokers. Enable it as a dev-dependency only:

```toml
[dev-dependencies]
ruststream-rdkafka = { version = "0.7", features = ["testing"] }
```

Never enable this feature in production builds.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:handler"
```

`TestApp::publish` waits until the handlers settle before returning, so assertions read
finished state - no sleeps:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:testapp"
```

## Repositioning in-process

The transport keeps what it routes, so a subscription is seekable over that log and the crate's
own context keys work here unchanged: a delivery reports its topic and its index in that topic's
log as its offset, `Ctx<Position>` names where it sits, `Ctx<SeekHandle>` moves the subscription,
a batch body reads `KafkaBatchContext`, and `start_at(..)` opens a subscription at a chosen
position. A service that replays or skips is therefore tested with `TestApp` like any other
handler, and the replay settles inside `publish` before it returns - no sleep, no polling:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:seek"
```

What the transport does not have, it refuses instead of inventing: `KafkaPosition::timestamp(..)`
(it stamps no record timestamps), a partition other than `0` (it gives every topic one), and a
topic the subscription does not read all report `KafkaError::InvalidOptions`. Timestamp-resolved
seeks and multi-partition placement belong in the live suite.

## Transactions in-process

A handler that publishes inside a transaction mounts here with its routes file unchanged: the
handler names the capability, the include site names the production
`Publish::default().transactional_id(..)` policy, and the stand-in pairs it into an in-process
transactional publisher. `per_partition()` pairs too, so a handler taking
`Out<impl PartitionLanes>` gets one independent transaction per lane.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:transactions"
```

Publishes made between `begin_transaction` and `commit` are held back and released together, an
`abort` discards them, and the misuse contract holds - a second `begin_transaction` reports
`TransactionBusy`, a `commit` or `abort` with nothing open reports `NoTransaction`:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:transaction_asserts"
```

### What the in-process transaction does not reproduce

Everything a Kafka transaction's guarantee actually rests on is broker-side, and none of it is
reproducible in a channel. Each line names the assertion it makes unsound:

- **Atomic visibility.** A commit routes the buffer message by message, so a subscriber here can
  observe a prefix of it. Real readers at `read_committed` isolation see the whole transaction or
  none of it, and the stand-in has no isolation level to read at. Do not assert in-process that a
  reader never observes a partial commit.
- **Zombie fencing.** The transactional id is carried and reported but fences nothing: two
  publishers on the same id coexist here, where `init_transactions` would have fenced the older
  producer's epoch. Do not assert in-process that a replaced replica was fenced out.
- **Broker-held timeouts.** A transaction left open stays open until the process ends; Kafka
  aborts one that outlives its `transaction.timeout.ms`. `transaction_timeout` and
  `queue_timeout` are accepted and carry no behaviour, so a test cannot assert on either.
- **The work pairing does.** Pairing the real policy creates the producer and initializes its
  transactions, so a bad transactional id fails at startup; pairing here allocates a buffer and
  cannot fail. A startup-time misconfiguration is invisible in process.
- **Exactly-once.** Coupling consumed offsets into the transaction
  (`send_offsets_to_transaction`) needs a consumer group and its metadata, and the in-process
  transport has neither. `KafkaEosPublish` therefore does not pair against the test broker at
  all: mounting an exactly-once route on it is a **compile error**, deliberately, rather than a
  green test that proves nothing about exactly-once.

All of it is covered against a live cluster in `tests/integration_rdkafka.rs`:
`partition_scoped_transactions_run_independently`, `eos_pipeline_commits_offsets_with_records`,
`eos_aborted_window_replays_without_output_duplicates`,
`seeking_inside_an_eos_window_replays_without_committing_past_the_target` and
`eos_publishing_handler_replies_ride_the_window`.

## What the test broker does not simulate

The in-process broker implements the core routing contract - exact topic-name fanout,
settlement, headers, the partition-key header and worker lanes - plus the retained log above it
and the client-visible half of a transaction. It does not simulate Kafka itself: consumer groups,
real partitions, committed positions, rebalancing, retention and record timestamps are cluster
behavior, as is everything listed under transactions above. Concretely:

- **Consumer groups.** `KafkaTopic::group(..)` is accepted and carries no behaviour: the router
  fans a topic's messages out to *every* subscription of that topic, so each subscriber behaves
  as if it were alone in a group of its own. A competing-consumers service - several replicas of
  one handler under one group - is therefore not exercised here, and in process every replica
  sees every message where a real group would have split the partitions among them. Test that
  against a cluster.
- **Partitions.** Every topic has exactly one partition, numbered zero. `Ctx<Partition>` always
  reads `0`, a `PARTITION_HEADER` stamp does not change where a record lands, and a seek to any
  other partition is refused rather than invented. Worker lanes *are* reproduced: a
  subscription's `LaneKey` resolves here exactly as it does upstream, so under the default
  `LaneKey::Partition` a topic's records share one lane (one partition, one order, keyless
  records included) and under `LaneKey::RecordKey` they lane by key. What a test cannot show is
  two keys landing on different partitions, or several partitions running concurrently.
- **Commit modes and start offsets.** `commit(..)` carries no behaviour: settlement is
  per-delivery, with no committed position, so nothing survives a "restart" and there is no
  position for a later subscriber to resume from. `start(StartOffset::Earliest)` is likewise
  inert - use the mount site's `start_at(KafkaPosition::earliest())`, which really does open the
  subscription on the retained log.
- **Redelivery, retries and dead-lettering.** `nack(true)` re-enqueues that one delivery to the
  same subscription immediately. The real transport instead leaves the offset uncommitted and
  redelivers from the committed position on the next fetch, which replays everything settled
  after it too - so an in-process test cannot show what a real rewind drags back with it. The
  crate's retry pipeline is not reproduced at all: `retry(..)`, `dead_letter(..)` and
  `max_deliveries(..)` are built on the live consumer and are inert on the stand-in, so a
  republish onto a retry or dead-letter topic never happens here. `retry_after` is out of reach
  too, because it needs a build-time publisher (`KafkaBroker::retry_publisher`) that the
  in-process broker does not mint.
- **Manual assignment and patterns** are refused loudly rather than approximated
  (`KafkaError::InvalidOptions`), for the same reason.

Exercise the real semantics against a live cluster:

```text
just brokers-up
KAFKA_TEST_URL=127.0.0.1:9092 cargo test --workspace --all-features -- --test-threads=1
```

The crate's own suites follow the same split: `tests/testing_core.rs` holds the handler-level
scenarios on `TestApp` plus the transport's own contract, and `tests/integration_rdkafka.rs`
plus the conformance lifecycle run only when `KAFKA_TEST_URL` is set.
