---
sidebar_position: 2
title: KafkaTopics
description: "KafkaTopics reads a list of topics through one consumer and one group, and each delivery names its topic."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KafkaTopics

[`KafkaTopics`](api:ruststream_rdkafka::KafkaTopics) reads a list of topics through one consumer and one group, and each delivery names its topic.

[`KafkaTopics::new`](api:ruststream_rdkafka::KafkaTopics::new) takes the topics, and [`and_topic`](api:ruststream_rdkafka::KafkaTopics::and_topic) adds one more.
The group settings are the ones [`KafkaTopic`](./kafka-topic.md) has: `group`, `start`, `commit`, `assignment`, `lane_key` and `config`.
Every topic of the list shares the handler, so every topic carries the handler's payload type.
`Ctx<Topic>` binds the topic a delivery came from.
The subscription's name is the list joined with commas, `orders,cancellations` here.
No one publish reaches a list again, so a registration over it names where a retry copy goes: `.out_retry(policy).to(topic)`, or [`ToSourceTopic`](api:ruststream_rdkafka::ToSourceTopic) to send each copy back to the topic it came from.
A registration that names neither stops the service at startup.

## Example

Order events from two topics become one history, and each entry names the topic of its event.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_multi_topic.rs#multi"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_multi_topic.rs#multi"
```

</TabItem>
</Tabs>
