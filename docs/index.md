# Kafka broker

`ruststream-rdkafka` runs a [RustStream](https://github.com/powersemmi/ruststream) service on
Apache Kafka, through [rdkafka](https://docs.rs/rdkafka) / librdkafka.

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

- A subscription is one consumer reading one topic. [`KafkaTopic`](topics.md) describes it, the
  consumer group included; the bare-string `#[subscriber("orders")]` form takes the group from
  the broker's `default_group`.
- The name an outgoing message declares is the destination topic. A reply type that declares
  `#[outgoing(name = "confirmations")]` is published to `confirmations`, and the subscriber
  writes the bare `publish` clause. A reply type that declares no name is published to the topic
  the mount site names, `publish("enriched-orders")`. A partition-key header becomes the record's
  native key, so Kafka itself keeps per-key ordering (see [Publishing](publishing.md)).
- Settlement follows Kafka's committed position rather than a per-message frame. `Commit::Auto`,
  the default, leaves that position to librdkafka's auto-commit; `Commit::Tracked` makes each
  `ack` a precise per-message acknowledgement over a contiguous watermark. See
  [Topics and groups](topics.md).
- Configuration delegates to librdkafka. An option you leave unset keeps the librdkafka default,
  and `config(key, value)` on the broker and on the descriptor, `producer_config(key, value)` for
  the producer, reach every property this crate does not surface as a typed option.

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

`KafkaBroker::new` only records configuration, so the service composes with the synchronous
`#[ruststream::app]` builder. The runtime calls `connect` once at startup, opens every
subscription against the connected broker, and shuts it down at the end.

The compile-time guarantee belongs to the owner of the handle. Handles that alias the connection
(publishers instantiated earlier, subscribers still open) return `KafkaError::Closed` after the
shutdown instead of succeeding against a dead connection.

Publishers follow the same split. `KafkaPublish` is the **policy** that constructs the **live**
`KafkaPublisher`; `transactional_id` turns it into the transactional policy, `per_partition`
turns that one into the per-partition policy, and `KafkaEosPublish` is the policy of an
exactly-once pipeline.

You name the policy when you register the handler (`b.include(handler).out(Reply, policy)` for
the reply, `.out(marker, policy)` for an `Out<..>` slot), and at startup the policy instantiates
the publisher on the connected broker. A handler that only replies names nothing, and the
broker's default policy constructs the publisher.

## Capabilities

The framework's optional capability traits, and which of them this broker implements natively:

| Capability | Native | Detail |
|---|---|---|
| `Subscribe` | yes | `#[subscriber("orders")]` subscribes by topic name alone, in the broker's [default consumer group](topics.md#consumer-groups). |
| `BatchSubscriber` | yes | Consume whole batches, one delivery plus everything librdkafka has already fetched, with no added waiting, up to the size the mount site names: [Batches](topics.md#batches). |
| `TransactionalPublisher` | yes | Publish inside Kafka transactions, one open transaction per handle: [Transactions](publishing.md#transactions). |
| `OwnedTransactions` | no | A Kafka producer holds one broker-side transaction at a time, so a transaction cannot be an independently owned value; concurrent flows take [per-partition publishers](publishing.md#transaction-scopes-and-worker-pools) or an [exactly-once pipeline](publishing.md#exactly-once-pipelines). |
| `RequestReply` | no | Kafka has no reply correlation; request/reply is a reply topic of your own plus a correlation header. |
| `Partitioned` | yes | Ordered worker lanes, keyed by the delivery's source partition or by the record key under `LaneKey::RecordKey`: [Keyed worker lanes](topics.md#keyed-worker-lanes). |
| `Seekable` + `Positioned` | yes | Reposition the partitions this consumer holds from a handler, through the `SeekHandle` context key next to the delivery's own `Position`: [Repositioning a subscription](topics.md#repositioning-a-subscription). |
| `DescribeServer` | yes | The generated AsyncAPI document lists the bootstrap servers under the `kafka` protocol. |

## Scaffold a service

```text
cargo generate --git https://github.com/powersemmi/ruststream-rdkafka templates/kafka-topic --name my-service
```

The starter wires one Kafka broker with a default consumer group, a tracked-commit subscriber
with a retry and dead-letter pipeline, and a published reply. Its `#[ruststream::app]` entry
point gives the binary the `run` and `asyncapi gen` commands.

## Guides

- [Topics and groups](topics.md) - descriptors, start offsets, commit modes, keyed lanes.
- [Publishing](publishing.md) - publish policies, record keys, transactions, delivery guarantees.
- [Schema Registry](schema-registry.md) - Confluent framing, Avro and Protobuf transcoding.
- [Testing](testing.md) - the in-process test broker and the live-cluster suites.
