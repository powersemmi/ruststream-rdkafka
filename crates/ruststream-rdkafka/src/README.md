Apache Kafka broker for the [RustStream](https://github.com/powersemmi/ruststream) messaging
framework, backed by [`rdkafka`](https://docs.rs/rdkafka) and librdkafka.

A subscription is one consumer in one consumer group, and a publish is one record on the topic
the outgoing message names. Kafka settles by committed position, so what `ack` and `nack` do is
decided by the subscription's [`Commit`] mode. A setting with no typed option here is a
librdkafka property, set through the `config` passthroughs.

# A service

A handler is an `async fn` over the decoded payload, mounted on a broker inside the
application object, and the attribute writes the `main`:

```
# #[cfg(feature = "json")]
# mod demo {
use ruststream_rdkafka::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "accepted");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
        |b| {
            b.include(handle);
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it. [`KafkaBroker::new`] only records the configuration; the runtime
connects the broker at startup and shuts it down when the service stops.

# Where things are

The [guides][guides] take a Kafka service from installation to production, in this order:
[getting started][g-start], [topics, consumer groups and ordering][g-groups],
[commits and delivery guarantees][g-commits], [retries and dead-letter topics][g-retries],
[replay and seeking][g-replay], [transactions and exactly-once][g-eos],
[Schema Registry][g-registry], [Protobuf][g-protobuf], [testing][g-testing] and
[connecting to a production cluster][g-production]. The [architecture][architecture] pages show
how the crate sits between librdkafka and the runtime, and the [benchmarks][benchmarks] what it
costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Broker

- [Kafka broker][c-broker]: [`KafkaBroker`], connected into [`ConnectedKafkaBroker`] and closed
  into [`ClosedKafkaBroker`].
- [Config][c-config]: [`KafkaBroker::config`] and [`KafkaBroker::producer_config`] pass any
  librdkafka property.
- [Security][c-security]: SASL and TLS through `config`, with the `ssl`, `ssl-vendored` and
  `zstd` features.
- [Startup][c-startup]: [`KafkaBroker::connect_timeout`] and [`KafkaBroker::assignment_timeout`].
- [Shutdown][c-shutdown]: [`KafkaBroker::flush_timeout`] and
  [`ClosedKafkaBroker::unflushed_records`].
- [Prelude][c-prelude]: [`prelude`], the one glob a Kafka service imports.
- [Errors][c-errors]: [`KafkaError`].

## Subscriptions

- [Subscriptions][c-subscriptions]: a bare topic name, read in the broker's
  [`default_group`](KafkaBroker::default_group).
- [KafkaTopic][c-kafka-topic]: [`KafkaTopic`], one topic in one consumer group.
- [KafkaTopics][c-kafka-topics]: [`KafkaTopics`], a list of topics through one consumer.
- [Patterns][c-patterns]: [`KafkaTopicPattern`], every topic a regex matches.
- [Schema checking][c-schema]: [`KafkaTopic::schema`] and its twins check a topic's registry
  subjects at startup, with [`schema::Schema`] naming the namespace and subjects.
- [KafkaPartitions][c-kafka-partitions]: [`KafkaPartitions`], named partitions with no group.
- [StartOffset][c-start-offset]: [`StartOffset`], where a group with no committed offset starts.
- [Assignment][c-assignment]: [`Assignment`], the group's rebalance strategy.
- [LaneKey][c-lane-key]: [`LaneKey`], what a `workers(n, by_key)` lane is keyed by.
- [Passthrough][c-passthrough]: [`KafkaTopic::config`] and its twins pass consumer properties.
- [Batches][c-batches]: a slice handler over [`KafkaSubscriber`] batches.

## Settlement

- [Commit][c-settlement]: [`Commit`], what an ack or a nack does on Kafka.
- [Auto][c-auto]: [`Commit::Auto`], librdkafka's timer commit.
- [Tracked][c-tracked]: [`Commit::Tracked`], precise at-least-once.
- [Retries][c-retries]: `max_attempts` and `retry_after` copies.
- [Dead letters][c-dead-letters]: `dead_letter`, the topic a spent delivery goes to.
- [ToSourceTopic][c-to-source-topic]: [`ToSourceTopic`], a retry copy back to its own topic.

## Context

- [Kafka context][c-context]: [`KafkaContext`](context::KafkaContext) and the keys in
  [`context::keys`]: topic, partition, offset, timestamp and key of a delivery.
- [KafkaBatchContext][c-batch-context]: [`KafkaBatchContext`](context::KafkaBatchContext), a
  batch handler's context.

## Positions

- [KafkaPosition][c-positions]: [`KafkaPosition`], a place in the log.
- [start_at][c-start-at]: opening a subscription at a position on every startup.
- [Seek][c-seek]: [`SeekHandle`](context::keys::SeekHandle) and [`KafkaSeeker`], moving a running
  subscription.

## Publishing

- [Publish][c-publishing]: [`KafkaPublish`], the plain publish policy, and its live
  [`KafkaPublisher`].
- [Keys][c-keys]: [`PARTITION_KEY_HEADER`] becomes the record's native key.
- [partition][c-partition]: [`KafkaOptions`] and [`KafkaPublishSteps::partition`] pin one record.
- [RoundRobin][c-round-robin]: [`RoundRobin`] spreads replies over partitions.
- [Durability][c-durability]: the producer's `acks` and idempotence through
  [`KafkaBroker::producer_config`].

## Transactions

- [Transactions][c-transactions]: [`KafkaTransactionalPublish`] and its live
  [`KafkaTransactionalPublisher`].
- [per_partition][c-per-partition]: [`KafkaTransactionalPublish::per_partition`],
  [`KafkaPartitionedPublish`] and [`PartitionLanes`].
- [Exactly once][c-eos]: [`KafkaEosPublish`] with [`Commit::Transactional`].
- [EosReplies][c-eos-replies]: [`EosReplies`] joins replies to the open window.
- [EosPipeline][c-eos-pipeline]: [`EosPipeline`] and [`SourceOffset`] publish into it by hand.

## Formats and tooling

- [Schema Registry][c-schema-registry]: [`schema_registry`], the Confluent client and wire
  format (feature `schema-registry`).
- [Avro][c-avro]: [`avro`], the Avro codec (feature `avro`).
- Schemas as code: [`schema`], the `Schema` derive that checks a message type against the
  schema migrations of its subjects when the crate compiles (feature `schema-check`).
- [Protobuf][c-protobuf]: [`protobuf`], `prost` messages under the Confluent envelope (feature
  `protobuf`).
- [AsyncAPI][c-asyncapi]: the Kafka bindings of the generated document (feature `asyncapi`).
- [Testing][c-testing]: the in-process Kafka cluster
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  the broker to (feature `testing`), and [Live][c-live] runs against a running cluster.

# Cargo features

`json` (default), `msgpack` and `cbor` forward the core's codecs; the default codec is what a
mount site with no `.codec(..)` step encodes with. `schema-registry` adds the Confluent client,
the wire-format envelope and the subscriber-side prefetch; `avro` and `protobuf` add those
formats on top of it. `schema-check` adds the `Schema` derive, which checks a type against
the migrations in `schemas/` when the crate compiles. `asyncapi` contributes the Kafka bindings to the generated document,
`testing` adds the in-process mode `TestApp::start` connects the broker through, and `ssl`,
`ssl-vendored` and `zstd` map onto rdkafka's own backends.

[guides]: https://powersemmi.github.io/ruststream-rdkafka/latest/
[g-start]: https://powersemmi.github.io/ruststream-rdkafka/latest/guides/getting-started/
[g-groups]: https://powersemmi.github.io/ruststream-rdkafka/latest/guides/groups-and-ordering/
[g-commits]: https://powersemmi.github.io/ruststream-rdkafka/latest/guides/commits/
[g-retries]: https://powersemmi.github.io/ruststream-rdkafka/latest/guides/retries/
[g-replay]: https://powersemmi.github.io/ruststream-rdkafka/latest/guides/replay/
[g-eos]: https://powersemmi.github.io/ruststream-rdkafka/latest/guides/exactly-once/
[g-registry]: https://powersemmi.github.io/ruststream-rdkafka/latest/guides/schema-registry/
[g-protobuf]: https://powersemmi.github.io/ruststream-rdkafka/latest/guides/protobuf/
[g-testing]: https://powersemmi.github.io/ruststream-rdkafka/latest/guides/testing/
[g-production]: https://powersemmi.github.io/ruststream-rdkafka/latest/guides/production/
[architecture]: https://powersemmi.github.io/ruststream-rdkafka/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-rdkafka/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/broker/
[c-config]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/broker/config/
[c-security]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/broker/security/
[c-startup]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/broker/startup/
[c-shutdown]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/broker/shutdown/
[c-prelude]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/broker/prelude/
[c-errors]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/broker/errors/
[c-subscriptions]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/subscriptions/
[c-kafka-topic]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/subscriptions/kafka-topic/
[c-kafka-topics]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/subscriptions/kafka-topics/
[c-patterns]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/subscriptions/patterns/
[c-schema]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/subscriptions/schema/
[c-kafka-partitions]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/subscriptions/kafka-partitions/
[c-start-offset]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/subscriptions/start-offset/
[c-assignment]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/subscriptions/assignment/
[c-lane-key]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/subscriptions/lane-key/
[c-passthrough]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/subscriptions/passthrough/
[c-batches]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/subscriptions/batches/
[c-settlement]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/settlement/
[c-auto]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/settlement/auto/
[c-tracked]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/settlement/tracked/
[c-retries]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/settlement/retries/
[c-dead-letters]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/settlement/dead-letters/
[c-to-source-topic]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/settlement/to-source-topic/
[c-context]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/context/
[c-batch-context]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/context/batch-context/
[c-positions]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/positions/
[c-start-at]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/positions/start-at/
[c-seek]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/positions/seek/
[c-publishing]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/publishing/
[c-keys]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/publishing/keys/
[c-partition]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/publishing/partition/
[c-round-robin]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/publishing/round-robin/
[c-durability]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/publishing/durability/
[c-transactions]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/transactions/
[c-per-partition]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/transactions/per-partition/
[c-eos]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/transactions/exactly-once/
[c-eos-replies]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/transactions/exactly-once/eos-replies/
[c-eos-pipeline]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/transactions/exactly-once/eos-pipeline/
[c-schema-registry]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/schema-registry/
[c-avro]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/avro/
[c-protobuf]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/protobuf/
[c-asyncapi]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/asyncapi/
[c-testing]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-rdkafka/latest/components/testing/live/
