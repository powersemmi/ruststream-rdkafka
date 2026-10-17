---
title: KafkaPosition
description: "KafkaPosition names a place in the log: either end, an offset, an offset of one topic, or a timestamp."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KafkaPosition

[`KafkaPosition`](api:ruststream_rdkafka::KafkaPosition) names a place in the log: either end, an offset, an offset of one topic, or a timestamp.

Kafka keeps its log, so a subscription can be moved through it, at startup with [`start_at`](./start-at.md) or from a handler with a [seek](./seek.md).
`KafkaPosition::earliest()` is the oldest record each partition retains, and `KafkaPosition::latest()` is the end of each partition's log.
`KafkaPosition::timestamp(millis)` is, on each partition, the first record stamped at or after that time in epoch milliseconds; a partition with no such record resumes at its end.
These three apply to every partition the subscription reads.
`KafkaPosition::offset(partition, offset)` is one partition at an absolute offset, on every topic the subscription reads.
`KafkaPosition::topic_offset(topic, partition, offset)` names the topic too, which a subscription over [several topics](../subscriptions/kafka-topics.md) needs.
A delivery's own coordinates are a position as well: `Ctx<Position>` binds them, and a seek there replays that record.
A position on a partition or a topic the subscription does not read is refused with an error.

## Example

A new reporting group starts from the first reading stamped at or after the time its configuration names.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_positions_timestamp.rs#replay"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_positions_timestamp.rs#replay"
```

</TabItem>
</Tabs>
