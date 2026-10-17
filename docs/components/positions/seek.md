---
sidebar_position: 2
title: Seek
description: "A handler moves its running subscription through the SeekHandle key; the seek is local and a rebalance discards it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Seek

A handler moves its running subscription through the [`SeekHandle`](api:ruststream_rdkafka::context::keys::SeekHandle) key; the seek is local and a rebalance discards it.

`Ctx<SeekHandle>` binds a [`KafkaSeeker`](api:ruststream_rdkafka::KafkaSeeker) for the subscription the delivery came from.
Its `seek(position)` takes a [`KafkaPosition`](./index.md), and the next delivery comes from there.
`Ctx<Partition>` and [`Ctx<Position>`](api:ruststream_rdkafka::context::keys::Position) give the delivery's own coordinates to aim with.
A seek moves this consumer and no other member of the group.
It commits nothing, and the next rebalance discards it: whoever gets the partitions resumes from the group's committed offsets.
Under [`Commit::Tracked`](../settlement/tracked.md) a seek clears the tracked position of every partition it moves, so a later commit never passes records the seek replayed and nobody handled.
A seek after the service shut down returns an error.
A batch handler reaches the same key through its [`KafkaBatchContext`](../context/batch-context.md), and the move applies once the batch has settled.
A position that has to survive a restart belongs in [`start_at`](./start-at.md) instead.

## Example

A job that carries `resume_at` moves its partition past a run of jobs known to fail.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_seek.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_seek.rs#handler"
```

</TabItem>
</Tabs>
