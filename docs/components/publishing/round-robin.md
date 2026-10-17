---
sidebar_position: 3
title: RoundRobin
description: "RoundRobin spreads replies over a fixed partition count, placing only records with no key and no partition."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RoundRobin

[`RoundRobin`](api:ruststream_rdkafka::RoundRobin) spreads replies over a fixed partition count, placing only records with no key and no partition.

librdkafka places a keyless record by its partitioner, which may keep a whole batch on one partition.
When every message takes about the same long time to process, that leaves one consumer busy and its peers idle.
`RoundRobin::partitions(n)` pins each record to the next partition of the cycle `0, 1, ..., n - 1`, one record each.
It is a transform of the publish position, written after the policy: `.out_reply(Publish::default()).transform(RoundRobin::partitions(n))`.
It writes the same setting as the [`partition`](./partition.md) step, so it mounts over Kafka's publisher only.
A record that carries a [key](./keys.md) or already names a partition keeps its placement.
The count is fixed at the mount site and matches the destination topic: a smaller one leaves the last partitions empty, a larger one makes publishes fail.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_distribution.rs#round_robin"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_distribution.rs#round_robin"
```

</TabItem>
</Tabs>
