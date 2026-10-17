---
sidebar_position: 7
title: LaneKey
description: "LaneKey decides what workers(n, by_key) keys a lane by: the source partition, or the record key."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# LaneKey

[`LaneKey`](api:ruststream_rdkafka::LaneKey) decides what `workers(n, by_key)` keys a lane by: the source partition, or the record key.

`workers(n, by_key)` runs `n` sequential lanes: deliveries with one lane key run in order on one lane, and different keys run at once.
On Kafka the descriptor's `lane_key` decides what that key is.
`LaneKey::Partition`, the default, keys a lane by the partition the record came from, Kafka's own unit of order.
Everything one partition delivers then runs in order, keyless records included, and partitions run side by side.
`LaneKey::RecordKey` keys a lane by the record's native key, which is finer than a partition.
One key's records stay in order, and records of different keys run at once even inside one partition.
A keyless record has no lane key under `RecordKey`; it rotates over the lanes and loses its partition order.
The publish side sets the record key from the [partition-key header](../publishing/keys.md), so a record published with a key reaches its lane by that key.
The manual form writes the lanes as `.workers_by_key(n)` on the mount chain.

## Example

Two tenants' orders, each order a second of work: lanes by record key run the tenants side by side, two seconds in all.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_keys.rs#consumer"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_keys.rs#consumer"
```

</TabItem>
</Tabs>

## Lanes by partition

The same four orders on a topic with one partition take four seconds: the partition is one lane.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_keys.rs#partition_lanes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_keys.rs#partition_lanes"
```

</TabItem>
</Tabs>
