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

## Competing consumers in-process

`KafkaTopic::group(..)` carries its meaning here: a record reaches **one** member of each consumer
group, and every group reads its own copy. A service whose replicas share a group therefore
divides the work in a test the way it divides it on a cluster, instead of every replica handling
every record.

Which member is a partition assignment, not a rotation. The transport gives every topic exactly
one partition, and Kafka hands a partition to exactly one member of a group, so a group's records
all land on the same member rather than spreading over them - the truth about a one-partition
topic, and the reason a test must not assert that two workers each did half. When the owner drops
its subscription the next member takes the topic over, which is the effect a rebalance has.

`KafkaTestBroker::default_group(..)` mirrors `KafkaBroker::default_group` for subscriptions that
name no group of their own. Set it when the service under test sets one: without it two bare
`#[subscriber("orders")]` handlers are each alone in a group and both see every record, which is
not what the same wiring does against Kafka.

## Settlement is a read position

`nack(true)` does not re-enqueue one message. It leaves the offset unsettled and resumes the
subscription from the committed position, so that record **and everything after it** are
delivered again - the at-least-once duplication a real rewind produces. `ack` and `nack(false)`
settle the offset, and the lowest unsettled offset is where a later rewind lands.

So the commit mode decides what a retry does, here as on a cluster:

- `Commit::Tracked` - the rewind above.
- `Commit::Auto` (the descriptor default) - **advisory**: librdkafka stores the position as a
  record is handed to the application, so `nack(true)` brings nothing back. A handler retrying
  under the default mode is retrying nothing, and the stand-in says so rather than obligingly
  redelivering.
- `Commit::Transactional` - rewinds like `Tracked`, there being no exactly-once pipeline here to
  defer the commit to.

One deliberate exception: a subscription opened **by name** rather than by descriptor (the
`Subscribe` capability, which the bare `#[subscriber("orders")]` form and the core conformance
suite both use) settles like `Commit::Tracked`. The core routing contract requires `nack(true)`
to redeliver and a bare name carries no commit mode to decide otherwise, so that one path keeps
the contract; a subscription that reaches the broker as a `KafkaTopic` gets its own mode. Name the
mode on the descriptor whenever a test turns on what a retry does.

## Byte-lane handlers

A handler reading the Confluent wire form is an ordinary handler here too: `IncomingFrame` and
`OutgoingFrame` carry their own bytes, so nothing about them needs a cluster, and on the Protobuf
side reading needs no registry either. See
[the Schema Registry page](schema-registry.md#testing-a-lane-handler) for the `TestApp` shape and
its manual-path counterpart.

## What the test broker does not simulate

The in-process broker implements the core routing contract - exact topic-name routing, consumer
groups, settlement, headers, the partition-key header and worker lanes - plus the retained log
above it and the client-visible half of a transaction. It does not simulate Kafka itself: real
partitions, positions that outlive a subscription, rebalancing, retention and record timestamps
are cluster behavior, as is everything listed under transactions above. Concretely:

- **Partitions.** Every topic has exactly one partition, numbered zero. `Ctx<Partition>` always
  reads `0`, a `PARTITION_HEADER` stamp does not change where a record lands, and a seek to any
  other partition is refused rather than invented. Worker lanes *are* reproduced: a
  subscription's `LaneKey` resolves here exactly as it does upstream, so under the default
  `LaneKey::Partition` a topic's records share one lane (one partition, one order, keyless
  records included) and under `LaneKey::RecordKey` they lane by key. What a test cannot show is
  two keys landing on different partitions, several partitions running concurrently, or a group's
  work spreading across its members.
- **Committed positions do not outlive their subscription.** The position lives on the
  subscription, so nothing survives a "restart": a later subscriber in the same group opens at the
  end of the log rather than resuming where its predecessor stopped, and
  `start(StartOffset::Earliest)` is inert. Use the mount site's
  `start_at(KafkaPosition::earliest())`, which really does open the subscription on the retained
  log, and test resume-across-restart against a cluster.
- **Retries and dead-lettering.** The crate's retry pipeline is not reproduced: `retry(..)`,
  `dead_letter(..)` and `max_deliveries(..)` are built on the live consumer and are inert on the
  stand-in, so a republish onto a retry or dead-letter topic never happens here. `retry_after` is
  out of reach too, because it needs a build-time publisher (`KafkaBroker::retry_publisher`) that
  the in-process broker does not mint. A test sees no retry rather than a wrong one.
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
