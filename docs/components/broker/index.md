---
title: KafkaBroker
description: "KafkaBroker connects a service to a Kafka cluster; new only records the bootstrap servers, and the runtime connects it at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KafkaBroker

[`KafkaBroker`](api:ruststream_rdkafka::KafkaBroker) connects a service to a Kafka cluster; `new` only records the bootstrap servers, and the runtime connects it at startup.

[`KafkaBroker::new`](api:ruststream_rdkafka::KafkaBroker::new) takes the bootstrap servers, each a `host` or `host:port` the client starts from.
It does no I/O, so the whole service fits the synchronous `#[ruststream::app]` builder.
The settings that follow it are builder steps too: a [default consumer group](api:ruststream_rdkafka::KafkaBroker::default_group), librdkafka properties through [`config`](./config.md), and the [startup](./startup.md) and [shutdown](./shutdown.md) bounds.
A property the service leaves unset keeps the librdkafka default.

At startup the runtime connects the broker and receives a [`ConnectedKafkaBroker`](api:ruststream_rdkafka::ConnectedKafkaBroker).
The connect probes the cluster, so an unreachable cluster fails startup instead of the first publish.
Every subscription and every publisher of the service opens against that connected form.
At shutdown the runtime turns it into a [`ClosedKafkaBroker`](api:ruststream_rdkafka::ClosedKafkaBroker), after the producer has flushed what the handlers published.

`#[subscriber("orders")]` reads the topic `orders` in the broker's default group.
Kafka reads a topic only through a consumer group, so a subscription with no group of its own on a broker with no default group fails startup.
The descriptors that set a group, several topics or exact partitions per subscription are on the [Subscriptions](../subscriptions/index.md) pages.

## Example

The service reads the topic `orders` in the group `orders-svc`.
`cargo run --example kafka_quickstart --features testing -- run` starts it against a cluster on `localhost:9092`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_quickstart.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_quickstart.rs#app"
```

</TabItem>
</Tabs>
