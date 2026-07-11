# Kafka broker

`ruststream-rdkafka` is the Apache Kafka broker for the
[RustStream](https://github.com/powersemmi/ruststream) messaging framework, backed by
[rdkafka](https://docs.rs/rdkafka) / librdkafka.

```toml
[dependencies]
ruststream = { version = "0.5", features = ["macros", "json"] }
ruststream-rdkafka = "0.5"
serde = { version = "1", features = ["derive"] }
```

A minimal service is one handler and one app function:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_quickstart.rs:handler"
```

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_quickstart.rs:app"
```

## The transport model

- A subscription is one consumer joining one consumer group on one topic.
  [`KafkaTopic`](topics.md) describes it; the bare-string `#[subscriber("orders")]` form uses
  the broker's `default_group` (Kafka cannot subscribe without a group).
- The outgoing message name is the destination topic, and the partition-key header becomes the
  record's native key, so Kafka itself keeps per-key ordering (see [Publishing](publishing.md)).
- Settlement follows Kafka's committed-position model: the `Commit` mode picks between
  librdkafka auto-commit (`Auto`, the default) and precise per-message acknowledgement over a
  contiguous watermark (`Tracked`). See [Topics and groups](topics.md).
- Configuration delegates to librdkafka: unset options mean librdkafka defaults, and raw
  `config(key, value)` passthroughs on the broker, the producer, and the descriptor reach every
  property this crate does not surface as a typed option.
- Lazy startup: `KafkaBroker::new` is synchronous and I/O-free, so a service composes with the
  synchronous `#[ruststream::app]` builder; the real network work happens in the idempotent
  async `Broker::connect`.

## Scaffold a service

```text
cargo generate --git https://github.com/powersemmi/ruststream-rdkafka templates/kafka-topic --name my-service
```

The starter wires one Kafka broker with a default consumer group, a tracked-commit subscriber
with a retry/dead-letter pipeline and a published reply, and the `#[ruststream::app]` entry
point (`run` / `asyncapi gen`).

## Guides

- [Topics and groups](topics.md) - descriptors, start offsets, commit modes, keyed lanes.
- [Publishing](publishing.md) - producing to topics, record keys, delivery guarantees.
- [Testing](testing.md) - the in-process test broker and the live-cluster suites.
