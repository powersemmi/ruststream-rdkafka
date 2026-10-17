---
title: KafkaContext
description: "KafkaContext gives a handler the record's topic, partition, offset, timestamp and key, each read as a parameter through Ctx<K>."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KafkaContext

[`KafkaContext`](api:ruststream_rdkafka::context::KafkaContext) gives a handler the record's topic, partition, offset, timestamp and key, each read as a parameter through `Ctx<K>`.

The subscription builds it once per delivery, beside the payload and the headers.
A handler binds one field with [`Ctx<K>`](api:ruststream::runtime::Ctx), where `K` is a key of [`context::keys`](api:ruststream_rdkafka::context::keys):

| Key | Value | What it is |
| --- | --- | --- |
| [`Topic`](api:ruststream_rdkafka::context::keys::Topic) | `Str` | the topic the record was read from, which differs per record on a topic list or pattern |
| [`Partition`](api:ruststream_rdkafka::context::keys::Partition) | `i32` | the partition the record was read from |
| [`Offset`](api:ruststream_rdkafka::context::keys::Offset) | `i64` | the record's offset in its partition |
| [`TimestampMillis`](api:ruststream_rdkafka::context::keys::TimestampMillis) | `Option<i64>` | the record's timestamp in milliseconds since the epoch, when it has one |
| [`Key`](api:ruststream_rdkafka::context::keys::Key) | `Option<Bytes>` | the record key, when the record has one |

`Topic` and `Partition` are in the prelude; the other keys are imported from `context::keys`.
A body that reads several fields takes `ctx: &mut Context<'_, KafkaContext>` instead and calls `ctx.context(key)` per field, as the manual form does.
Reading the context also hands it to the retry position, where [ToSourceTopic](../settlement/to-source-topic.md) reads the topic.
The same context carries the [`Position`](../positions/index.md) and [`SeekHandle`](../positions/seek.md) keys, which move the subscription through the log.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_context_fields.rs#context"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_context_fields.rs#context"
```

</TabItem>
</Tabs>
