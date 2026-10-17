---
sidebar_position: 1
title: KafkaBatchContext
description: "A batch handler's context carries the subscription's seek handle; each record's own coordinates travel with the elements."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KafkaBatchContext

A batch handler's context, [`KafkaBatchContext`](api:ruststream_rdkafka::context::KafkaBatchContext), carries the subscription's seek handle; each record's own coordinates travel with the elements.

A batch spans many records, so it has no single topic, partition or offset.
The subscription builds the context once per batch, and it holds what every record of the batch shares: the [`SeekHandle`](api:ruststream_rdkafka::context::keys::SeekHandle) key, the same key a [`KafkaContext`](./index.md) carries.
A batch body takes `ctx: &mut Context<'_, KafkaBatchContext>` and reads the handle with `ctx.context(SeekHandle)`.
A seek from a batch body takes effect once the whole batch has settled, so the next batch opens at the new position.
A per-record key such as `Partition` or `Offset` is a compile error in a batch body, because the batch context does not carry it.
A batch registration has no per-record topic for [ToSourceTopic](../settlement/to-source-topic.md) either, so it names a fixed retry topic.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_context_batch.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_context_batch.rs#batch"
```

</TabItem>
</Tabs>
