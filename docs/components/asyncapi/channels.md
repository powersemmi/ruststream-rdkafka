---
sidebar_position: 2
title: Channels
description: "Each channel reports its topic: the descriptor's for a subscription, the resolved destination for a publish."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Channels

Each channel reports its topic: the descriptor's for a subscription, the resolved destination for a publish.

The channel binding has one field, `topic`.
A subscription by name, a [`KafkaTopic`](api:ruststream_rdkafka::KafkaTopic) and a [`KafkaPartitions`](api:ruststream_rdkafka::KafkaPartitions) reader each report the one topic they read.
A [`KafkaTopics`](api:ruststream_rdkafka::KafkaTopics) set or pattern reads several topics, so its channel carries no binding.

A channel the service publishes to reports the topic the mount site resolved.
That is a `reply("dest")` clause, a reply type's `#[outgoing(name)]`, the name of a slot entry, or a declared dead-letter topic.
A publish policy holds producer settings and no topic, so the policy itself adds nothing to the channel.

## Example

The channels `orders` and `ledger` report the topics their descriptors read, and `confirmations` the topic the reply clause names.
The channel of the `audit` and `audit.eu` set reports none.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_asyncapi.rs#channels"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_asyncapi.rs#channels"
```

</TabItem>
</Tabs>
