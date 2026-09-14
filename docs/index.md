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

- A subscription is one consumer reading one topic. [`KafkaTopic`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing) describes it, the
  consumer group included; the bare-string `#[subscriber("orders")]` form takes the group from
  the broker's `default_group`.
- The name an outgoing message declares is the destination topic. A reply type that declares
  `#[outgoing(name = "confirmations")]` is published to `confirmations`, and the subscriber
  writes the bare `publish` clause. A reply type that declares no name is published to the topic
  the mount site names, `publish("enriched-orders")`. A partition-key header becomes the record's
  native key, so Kafka itself keeps per-key ordering (see [Publishing](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#publishing)).
- Settlement follows Kafka's committed position rather than a per-message frame. `Commit::Auto`,
  the default, leaves that position to librdkafka's auto-commit; `Commit::Tracked` makes each
  `ack` a precise per-message acknowledgement over a contiguous watermark. See
  [Topics and groups](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing).
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

You name the policy when you register the handler (`b.include(handler).out_reply(policy)` for
the reply, `.out(marker, policy)` for an `Out<..>` slot), and at startup the policy instantiates
the publisher on the connected broker. A handler that only replies names nothing, and the
broker's default policy constructs the publisher.

## Capabilities

The framework's optional capability traits, and which of them this broker implements natively:

| Capability | Native | Detail |
|---|---|---|
| `Subscribe` | yes | `#[subscriber("orders")]` subscribes by topic name alone, in the broker's [default consumer group](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing). |
| `BatchSubscriber` | yes | Consume whole batches, one delivery plus everything librdkafka has already fetched, with no added waiting, up to the size the mount site names: [Batches](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#batches). |
| `TransactionalPublisher` | yes | Publish inside Kafka transactions, one open transaction per handle: [Transactions](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#transactions). |
| `OwnedTransactions` | no | A Kafka producer holds one broker-side transaction at a time, so a transaction cannot be an independently owned value; concurrent flows take [per-partition publishers](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#transactions) or an [exactly-once pipeline](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#exactly-once-pipelines). |
| `RequestReply` | no | Kafka has no reply correlation; request/reply is a reply topic of your own plus a correlation header. |
| `Partitioned` | yes | Ordered worker lanes, keyed by the delivery's source partition or by the record key under `LaneKey::RecordKey`: [Keyed worker lanes](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing). |
| `Seekable` + `Positioned` | yes | Reposition the partitions this consumer holds from a handler, through the `SeekHandle` context key next to the delivery's own `Position`: [Repositioning a subscription](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#positions-and-seeking). |
| `DescribeServer` | yes | The generated AsyncAPI document lists the bootstrap servers under the `kafka` protocol, with the schema registry beside them: [The AsyncAPI document](#the-asyncapi-document). |

## The AsyncAPI document

`asyncapi gen` prints the document a service describes itself with, and this crate fills in
Kafka's own vocabulary - the specification calls it the `kafka` binding. One feature turns it on:

```toml
ruststream-rdkafka = { version = "0.7", features = ["asyncapi"] }
```

A server then reports the schema registry it is configured with, a channel reports the topic
behind it, and a `receive` operation reports the consumer group that reads it:

```json
--8<-- "docs/snippets/asyncapi-bindings.json"
```

The client id appears when the descriptor's raw passthrough names `client.id`. The group appears
only when the descriptor names it: the document is built before anything connects, from the
descriptor alone, which never sees the broker whose `default_group` it would otherwise inherit.
`protocolVersion` stays out, because Kafka negotiates its version per API key between the client
and the cluster, so no one number says what clients speak.

A `KafkaTopics` subscription reports its group and no topic, because the binding's `topic` names
one and such a subscription reads a set. A registry-backed publisher adds a message binding: the
schema id rides in the payload under the Confluent encoding, under the subject its naming
strategy found.

A channel the service publishes to reports its topic too. That topic is the destination the mount
site resolved: a registration's `publish("dest")` clause, the reply type's own
`#[outgoing(name)]`, the name of a slot entry, a declared dead-letter topic. A publish policy
carries producer settings and no destination, so it has no topic of its own to report.

The registry URL reaches the document with any userinfo stripped, for the reason the bootstrap
addresses do - the document is published and shared, and a password that reaches it has left the
service.

## Scaffold a service

```text
cargo generate --git https://github.com/powersemmi/ruststream-rdkafka templates/kafka-topic --name my-service
```

The starter wires one Kafka broker with a default consumer group, a tracked-commit subscriber
under a declared attempt cap and dead-letter topic, and a published reply. Its `#[ruststream::app]` entry
point gives the binary the `run` and `asyncapi gen` commands.

## Where the documentation is

The reference is on docs.rs, one section per task.
[Subscribing](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing) covers the descriptors, consumer groups, start offsets, commit
modes, keyed worker lanes, batches, retries and repositioning.
[Publishing](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#publishing) covers the policies, record keys and explicit partitions,
delivery guarantees, transactions and exactly-once pipelines.
[`schema_registry`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/schema_registry/index.html) covers the Confluent envelope, with
[`avro`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/avro/index.html) and [`protobuf`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/protobuf/index.html) beside it, and
[`testing`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/testing/index.html) covers the in-process broker.

Installation, the tutorial and the list of brokers are on the framework's own site:
<https://powersemmi.github.io/ruststream/>.
