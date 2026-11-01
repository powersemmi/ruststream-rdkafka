---
sidebar_position: 3
title: Patterns
description: "KafkaTopicPattern reads every topic a librdkafka regex starting with ^ matches, those created later included."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Patterns

[`KafkaTopicPattern`](api:ruststream_rdkafka::KafkaTopicPattern) reads every topic a librdkafka regex starting with `^` matches, those created later included.

The leading `^` is how librdkafka tells a pattern from a topic name.
A pattern without it stops the service at startup, because librdkafka would subscribe to it as a literal name.
[`KafkaTopicPattern::new`](api:ruststream_rdkafka::KafkaTopicPattern::new) takes the pattern, and [`and_pattern`](api:ruststream_rdkafka::KafkaTopicPattern::and_pattern) adds another one to the same subscription.
[`KafkaTopics::pattern`](api:ruststream_rdkafka::KafkaTopics::pattern) builds the same descriptor; the `#[subscriber]` attribute takes the type its constructor names, so the attribute starts from `KafkaTopicPattern::new`.
The group settings are the ones [`KafkaTopic`](./kafka-topic.md) has.
The consumer finds a topic created after startup on its next metadata refresh, and the group then assigns its partitions.
A new topic is read from where the [start offset](./start-offset.md) puts it.
The subscription's name is the pattern itself.
A delivery names the topic it came from through [`Ctx<Topic>`](../context/index.md), and a [retry copy](../settlement/retries.md) needs a destination named at the mount site, as on [`KafkaTopics`](./kafka-topics.md).
Sets of the matched topics opt into [schema checking](./schema.md) with `.schema(pattern, namespace)`.

## Example

Every topic under `audit.` is archived; `auditing` is not.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_multi_topic.rs#pattern"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_multi_topic.rs#pattern"
```

</TabItem>
</Tabs>
