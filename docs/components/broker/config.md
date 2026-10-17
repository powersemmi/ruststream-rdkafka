---
sidebar_position: 1
title: Config
description: "config and producer_config pass any librdkafka property to every client of the broker or to its producer only."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Config

[`config`](api:ruststream_rdkafka::KafkaBroker::config) and [`producer_config`](api:ruststream_rdkafka::KafkaBroker::producer_config) pass any librdkafka property to every client of the broker or to its producer only.

A property set with `config` reaches every consumer and the producer the broker creates.
A property that applies to one side only is ignored by the other, as librdkafka ignores it.
`producer_config` applies on top of `config`, to the producer alone: `acks`, `compression.type`, `message.max.bytes`, `linger.ms`.
A descriptor's own [`config`](../subscriptions/passthrough.md) adds consumer properties for one subscription.
The names and values are librdkafka's, listed in its `CONFIGURATION.md`.

librdkafka checks every property when the broker connects.
An unknown name or a value it refuses fails startup with [`KafkaError::Connect`](api:ruststream_rdkafka::KafkaError::Connect), so a typo never reaches the first publish.
The in-process cluster a [`TestApp`](../testing/index.md) test runs on reads the same properties.
It refuses what librdkafka refuses, and a record over the producer's `message.max.bytes` is refused there as on a cluster.

## Example

The broker names its clients `orders-svc` and compresses what it publishes.
A shipment over 64 KiB is refused, and the order that produced it is settled as a retry.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_broker_config.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_broker_config.rs#app"
```

</TabItem>
</Tabs>
