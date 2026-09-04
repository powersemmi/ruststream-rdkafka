# Kafka broker

`ruststream-rdkafka` is the Apache Kafka broker for the
[RustStream](https://github.com/powersemmi/ruststream) messaging framework, backed by
[rdkafka](https://docs.rs/rdkafka) / librdkafka.

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-rdkafka = "0.7"
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

## The lifecycle ladder

Each state of the connection is its own type, so out-of-order use does not compile:

```text
KafkaBroker::new(servers)          configuration only, synchronous, no I/O
  |
  | .connect().await?              creates the producer, probes the cluster
  v
ConnectedKafkaBroker               subscriptions and live publishers hang off this
  |
  | .shutdown().await?             flushes in-flight publishes
  v
ClosedKafkaBroker                  terminal witness: unflushed_records()
```

`KafkaBroker::new` recording configuration instead of connecting is what lets a service compose
with the synchronous `#[ruststream::app]` builder: the runtime calls `connect` once at startup,
opens every subscription against the connected form, and shuts it down at the end. Only the
owner of the handle gets the compile-time guarantee - handles that alias the connection
(publishers paired earlier, subscribers still open) report `KafkaError::Closed` after the
shutdown instead of succeeding against a dead connection.

Publishers follow the same split. `KafkaPublish` - and its `transactional_id`, `per_partition`,
and `KafkaEosPublish` transitions - is a **policy**: pure declaration, constructible anywhere,
with no publish surface of its own. The include site names the policy
(`b.include(handler).out(Reply, policy)` for the reply, `.out(marker, policy)` for an `Out<..>`
slot), and the runtime pairs it against the connected broker into the **live** publisher the
handler receives. A handler that only replies to its `publish("dest")` topic names nothing at
all: the broker's default policy is used.

## Capabilities

The framework's optional capability traits, and which of them this broker implements natively:

| Capability | Native | Detail |
|---|---|---|
| `Subscribe` | yes | A bare-string `#[subscriber("orders")]` resolves through the broker's [default consumer group](topics.md#consumer-groups). |
| `BatchSubscriber` | yes | A batch is one delivery plus everything librdkafka has already fetched, with no added waiting, cut off at the size the mount site names: [Batches](topics.md#batches). |
| `TransactionalPublisher` | yes | `KafkaTransactionalPublisher` drives the producer's transaction API, one open transaction per handle: [Transactions](publishing.md#transactions). |
| `OwnedTransactions` | no | A Kafka producer holds one broker-side transaction at a time, so a transaction cannot be an independently owned value; concurrent flows use [per-partition publishers](publishing.md#transaction-scopes-and-worker-pools) or an [exactly-once pipeline](publishing.md#exactly-once-pipelines). |
| `RequestReply` | no | The protocol has no reply correlation; request/reply on Kafka is an application-level reply topic plus a correlation header. |
| `Partitioned` | yes | The partition key of a delivery is the record's native Kafka key: [Keyed worker lanes](topics.md#keyed-worker-lanes). |
| `Seekable` + `Positioned` | yes | `KafkaSeeker` repositions the partitions this consumer holds, reached from a handler through the `SeekHandle` context key next to the delivery's own `Position`; the in-process test broker mints the same seeker over its retained log, so a service that replays is testable without a cluster: [Repositioning a subscription](topics.md#repositioning-a-subscription). |
| `DescribeServer` | yes | The broker reports its bootstrap servers under the `kafka` protocol for generated AsyncAPI documents. |

## Scaffold a service

```text
cargo generate --git https://github.com/powersemmi/ruststream-rdkafka templates/kafka-topic --name my-service
```

The starter wires one Kafka broker with a default consumer group, a tracked-commit subscriber
with a retry/dead-letter pipeline and a published reply, and the `#[ruststream::app]` entry
point (`run` / `asyncapi gen`).

## Guides

- [Topics and groups](topics.md) - descriptors, start offsets, commit modes, keyed lanes.
- [Publishing](publishing.md) - publish policies, record keys, transactions, delivery guarantees.
- [Schema Registry](schema-registry.md) - Confluent framing, Avro and Protobuf transcoding.
- [Testing](testing.md) - the in-process test broker and the live-cluster suites.
