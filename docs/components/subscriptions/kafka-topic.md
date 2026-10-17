---
sidebar_position: 1
title: KafkaTopic
description: "KafkaTopic reads one topic in one consumer group, with its start offset, commit mode, assignment and lane key."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KafkaTopic

[`KafkaTopic`](api:ruststream_rdkafka::KafkaTopic) reads one topic in one consumer group, with its start offset, commit mode, assignment and lane key.

[`KafkaTopic::new`](api:ruststream_rdkafka::KafkaTopic::new) takes the topic, and every other setting is optional.
An unset setting keeps librdkafka's own default; the crate adds none of its own.
`group` names the consumer group and overrides the broker's `default_group`.
`start` is the [start offset](./start-offset.md) of a group that has committed nothing yet.
`commit` is the [commit mode](api:ruststream_rdkafka::Commit), which decides what an `ack` does.
`assignment` is the group's [rebalance strategy](./assignment.md), and `lane_key` is what [keyed lanes](./lane-key.md) are keyed by.
`config` [passes](./passthrough.md) any other consumer property to librdkafka.
The subscription's name is the topic: a test reads its deliveries under that name, and the AsyncAPI document reports that channel.
A publish to the topic reaches every group that reads it, so a delayed retry copy goes back to the topic with no setting at the mount site.
A topic name that Kafka refuses, or a name that starts with `^`, stops the service at startup; a pattern is [`KafkaTopics::pattern`](./patterns.md).

## Example

Every order of `orders` is confirmed by the group `orders-workers`, and the confirmation goes to the topic its type declares.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_topics.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_topics.rs#descriptor"
```

</TabItem>
</Tabs>
