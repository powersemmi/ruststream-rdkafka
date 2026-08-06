# Testing

The `testing` feature ships `KafkaTestBroker`, an in-process stand-in for Kafka: the same
handlers and descriptors, no cluster. It follows the same ladder as the real broker
(`new` -> `connect` -> `shutdown`) and the real `KafkaPublish` policy pairs against its
connected form, so include sites are identical for both brokers. Enable it as a dev-dependency
only:

```toml
[dev-dependencies]
ruststream-rdkafka = { version = "0.6", features = ["testing"] }
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

## What the test broker does not simulate

The in-process broker implements the core routing contract: exact topic-name fanout,
settlement, headers, and the partition-key header. It does not simulate Kafka itself: consumer
groups, partitions, committed positions, start offsets, rebalancing, and retention are
transport behavior. `nack(true)` redelivers immediately in-process, while the
real transport redelivers from the committed position on the next fetch.

Exercise the real semantics against a live cluster:

```text
just brokers-up
KAFKA_TEST_URL=127.0.0.1:9092 cargo test --workspace --all-features -- --test-threads=1
```

The crate's own suites follow the same split: `tests/testing_core.rs` drives the in-process
broker, and `tests/integration_rdkafka.rs` plus the conformance lifecycle run only when
`KAFKA_TEST_URL` is set.
