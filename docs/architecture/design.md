---
title: Design
sidebar_position: 2
description: "Where ruststream-rdkafka sits: between the RustStream runtime and the rust-rdkafka client, one consumer per subscription, one shared producer."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the rust-rdkafka client and the Kafka cluster, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": "Runtime: dispatch, lanes, settlement",
  "crate": "ruststream-rdkafka",
  "broker": ["KafkaBroker", "configuration"],
  "descriptors": ["KafkaTopic,", "KafkaTopics,", "KafkaPartitions"],
  "subscriber": ["KafkaSubscriber", "yields KafkaMessage"],
  "publishers": ["KafkaPublisher", "and transactional", "publishers"],
  "tracker": "Commit tracker",
  "eos": "EosPipeline",
  "client": "rust-rdkafka and librdkafka",
  "consumer": ["StreamConsumer", "one per subscription"],
  "producer": ["FutureProducer", "one per connection"],
  "cluster": "Kafka cluster",
  "mounted": "mounted",
  "polls": "polls the stream"
};

# Design

Where `ruststream-rdkafka` sits: between the RustStream runtime and the rust-rdkafka client, one
consumer per subscription, one shared producer.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over
[rust-rdkafka](https://docs.rs/rdkafka/0.39.0/rdkafka/), which wraps librdkafka. The runtime above it
is the same for every broker. Everything Kafka-specific is in this crate: the [descriptors](../components/subscriptions/index.md), [what an
ack means](../components/settlement/index.md), [where a retry copy goes](../components/settlement/retries.md) and [how a transaction commits](../components/transactions/index.md).

Each subscription opens a librdkafka consumer of its own, so a subscription is one member of its
consumer group. The [publishers](../components/publishing/index.md) of a connection share one producer, which the first publish opens. A
transactional publisher opens a producer of its own, under its `transactional.id`.

A setting this crate does not type is a librdkafka property. The
[`config`](../components/broker/config.md) passthroughs reach every one of them, and a
property left unset keeps the librdkafka default.

The [commit tracker](settlement.md) turns acknowledgements into stored offsets. The
[`EosPipeline`](api:ruststream_rdkafka::EosPipeline) commits consumed offsets inside a producer
transaction. How the pieces start and stop is on [Lifecycle](lifecycle.md), and the way of one
record on [Path](path.md).
