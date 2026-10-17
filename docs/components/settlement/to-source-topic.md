---
sidebar_position: 5
title: ToSourceTopic
description: "ToSourceTopic sends each retry copy back to the topic its delivery arrived on, for subscriptions over several topics."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ToSourceTopic

[`ToSourceTopic`](api:ruststream_rdkafka::ToSourceTopic) sends each retry copy back to the topic its delivery arrived on, for subscriptions over several topics.

A [`KafkaTopic`](api:ruststream_rdkafka::KafkaTopic) subscription reads one topic, and its retry copies go there.
A [`KafkaTopics`](api:ruststream_rdkafka::KafkaTopics) list or pattern and a [`KafkaPartitions`](api:ruststream_rdkafka::KafkaPartitions) reader have no single topic, so the mount site names one.
`ToSourceTopic` is a transform of the [`Retry`](api:ruststream::runtime::Retry) slot: `.out_retry(Publish::default()).transform(ToSourceTopic)`.
It reads each delivery's topic from its [Kafka context](../context/index.md), so a copy of an `orders-us` record goes back to `orders-us`.
The context reaches the slot when the handler reads it: a `Ctx<Topic>` parameter, or a manual body typed over `KafkaContext`.
A batch handler carries no per-record topic, so a batch registration names a fixed topic instead.
`.out_retry(Publish::default()).to(topic)` sends every copy to that one topic; a pattern that matches it reads the copies back.
A registration over several topics that names neither does not start.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_settlement_source_topic.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_settlement_source_topic.rs#handler"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_settlement_source_topic.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_settlement_source_topic.rs#app"
```

</TabItem>
</Tabs>

## A fixed retry topic

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_settlement_source_topic.rs#pattern"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_settlement_source_topic.rs#pattern"
```

</TabItem>
</Tabs>
