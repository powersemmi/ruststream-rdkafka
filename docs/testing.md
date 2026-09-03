# Testing

The `testing` feature ships `KafkaTestBroker`, an in-process stand-in for Kafka: the same
handlers and descriptors, no cluster. It follows the same ladder as the real broker
(`new` -> `connect` -> `shutdown`) and the real `KafkaPublish` policy pairs against its
connected form, so include sites are identical for both brokers. Enable it as a dev-dependency
only:

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
a page body reads `KafkaBatchContext`, and `start_at(..)` opens a subscription at a chosen
position. A service that replays or skips is therefore tested with `TestApp` like any other
handler, and the replay settles inside `publish` before it returns - no sleep, no polling:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:seek"
```

What the transport does not have, it refuses instead of inventing: `KafkaPosition::timestamp(..)`
(it stamps no record timestamps), a partition other than `0` (it gives every topic one), and a
topic the subscription does not read all report `KafkaError::InvalidOptions`. Timestamp-resolved
seeks and multi-partition placement belong in the live suite.

## What the test broker does not simulate

The in-process broker implements the core routing contract - exact topic-name fanout,
settlement, headers, the partition-key header - plus the retained log above it. It does not
simulate Kafka itself: consumer groups, real partitions, committed positions, rebalancing,
retention, record timestamps, and everything transactional (transactions and the exactly-once
pipeline) are cluster behavior. `nack(true)` redelivers immediately in-process, while the
real transport redelivers from the committed position on the next fetch.

Exercise the real semantics against a live cluster:

```text
just brokers-up
KAFKA_TEST_URL=127.0.0.1:9092 cargo test --workspace --all-features -- --test-threads=1
```

The crate's own suites follow the same split: `tests/testing_core.rs` holds the handler-level
scenarios on `TestApp` plus the transport's own contract, and `tests/integration_rdkafka.rs`
plus the conformance lifecycle run only when `KAFKA_TEST_URL` is set.
