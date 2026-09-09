# Testing

The `testing` feature ships `KafkaTestBroker`, an in-process stand-in for Kafka: the same handlers
and descriptors, no cluster. It takes the same `KafkaPublish` policy the real broker does, so you
mount the handlers and their publishers exactly as in production. Add it to your dev-dependencies:

```toml
[dev-dependencies]
ruststream-rdkafka = { version = "0.7", features = ["testing"] }
```

Never enable it in a production build.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:handler"
```

`tb.broker::<KafkaTestBroker>().publish(topic, &value)` returns once every handler the publish
triggers has settled, so the assertions read finished state and the test needs no sleep:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:testapp"
```

## Repositioning in-process

The in-process transport retains every message it routes, so a subscription can be repositioned
over that log. A delivery's offset is its index in its topic's log.

You can read that position with `Ctx<Position>` and move the subscription with `Ctx<SeekHandle>`,
the same keys the handler uses against a cluster. In a batch handler the `SeekHandle` key works
over `KafkaBatchContext`. You choose where a subscription opens with `start_at(..)` when you
include the handler.

A service that replays or skips is therefore an ordinary `TestApp` test. In the example the log is
seeded before the app starts, so the subscription opened with `start_at(..)` replays it.
`tb.settle()` drives that replay to a standstill without publishing anything new, so the assertion
reads finished state:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:seek"
```

The seek returns `KafkaError::InvalidOptions` for a position this transport cannot resolve: a
timestamp (it stamps no record timestamps), a partition other than `0` (every topic here has
exactly one partition), and a topic the subscription does not read. Test a timestamp-resolved seek
or a multi-partition placement against a cluster.

## What the test broker does not simulate

The in-process broker routes by exact topic name, and it carries settlement, headers and the
partition-key header. The cluster's own behaviour is not reproduced: consumer groups, real
partitions, committed positions, rebalancing, retention, record timestamps and everything
transactional. `nack(true)` redelivers immediately here, while the real transport redelivers from
the committed position on the next fetch.

Exercise the real semantics against a live cluster:

```text
just brokers-up
KAFKA_TEST_URL=127.0.0.1:9092 cargo test --workspace --all-features -- --test-threads=1
```
