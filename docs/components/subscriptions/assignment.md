---
sidebar_position: 6
title: Assignment
description: "Assignment picks the consumer group's rebalance strategy, eager or cooperative sticky."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Assignment

[`Assignment`](api:ruststream_rdkafka::Assignment) picks the consumer group's rebalance strategy, eager or cooperative sticky.

A group hands each partition of its topics to one member, and hands them out again when a member joins or leaves.
The setting is librdkafka's `partition.assignment.strategy`, written on the descriptor with `assignment`.
`Assignment::Range` and `Assignment::RoundRobin` are eager: every member gives up all its partitions at each rebalance, and the group stops reading until the new assignment lands.
`Assignment::CooperativeSticky` is incremental: only the partitions that change hands move, and the rest keep flowing during the rebalance.
Without the setting librdkafka uses `range,roundrobin`.
Every member of one group must use the same family: librdkafka refuses a cooperative member in an eager group, and the error surfaces on the subscription.
[`KafkaPartitions`](./kafka-partitions.md) joins no group and has no assignment.

## Example

A cooperative sticky group reads every shipment once, whichever member holds its partition.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_subscriptions_assignment.rs#assignment"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_subscriptions_assignment.rs#assignment"
```

</TabItem>
</Tabs>
