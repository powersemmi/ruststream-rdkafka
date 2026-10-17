---
sidebar_position: 2
title: partition
description: "The partition step pins one record to a partition, ahead of its key and the partitioner."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# partition

The [`partition`](api:ruststream_rdkafka::KafkaPublishSteps::partition) step pins one record to a partition, ahead of its key and the partitioner.

It is a per-record setting of Kafka's publisher, [`KafkaOptions`](api:ruststream_rdkafka::KafkaOptions), written by a step of the publish builder.
A handler reaches the step through a slot bounded on those options: `Out<impl Publisher<Options = KafkaOptions>, Slot>`, with this crate's prelude imported.
The bound keeps the step off a publisher of another broker.
The record goes to exactly that partition; its key still travels with it.
A partition the topic does not have fails the publish, and the handler decides what follows.
A test reads the setting back from the slot with `tb.out::<Slot>().with_options(..)`.
The [in-process cluster](../testing/index.md) gives every topic one partition, so a pin to partition `0` lands and a pin to any other is refused, as a cluster refuses a partition its topic lacks.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_publishing_partition.rs#partition"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_publishing_partition.rs#partition"
```

</TabItem>
</Tabs>
