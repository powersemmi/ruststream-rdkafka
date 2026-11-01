---
sidebar_position: 9
title: Passthrough
description: "A descriptor's config passes any consumer property; one the commit mode owns is refused at startup, naming the property."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Passthrough

A descriptor's `config` passes any consumer property; one the [commit mode](../settlement/index.md) owns is refused at startup, naming the property.

[`KafkaTopic::config`](api:ruststream_rdkafka::KafkaTopic::config), and the same method on `KafkaTopics` and `KafkaPartitions`, sets one librdkafka consumer property by its name.
It reaches the properties the descriptor has no typed option for: fetch sizes, queue limits, timeouts.
The passthrough applies last, so it wins over the typed options and over the broker-wide configuration.
A property the commit mode owns is the exception.
Under [`Commit::Tracked`](../settlement/tracked.md) and `Commit::Transactional` the crate computes the committed position itself, through `enable.auto.offset.store` and `enable.auto.commit`.
A passthrough that sets either one would leave the position to librdkafka while every `ack` decided nothing.
So the subscription refuses to open, and the startup error names the subscription and the property.
`Commit::Auto` owns neither property and takes both from the passthrough.
A property for every consumer of the broker goes on [`KafkaBroker`](../broker/config.md) instead.

## Example

`fetch.queue.backoff.ms` shortens the pause librdkafka takes when a partition's local queue is full, from a second to 10 ms.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_subscriptions_passthrough.rs#config"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_subscriptions_passthrough.rs#config"
```

</TabItem>
</Tabs>
