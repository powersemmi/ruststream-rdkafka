---
sidebar_position: 4
title: KafkaPartitions
description: "KafkaPartitions reads exactly the partitions it names, joins no group and never rebalances."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KafkaPartitions

[`KafkaPartitions`](api:ruststream_rdkafka::KafkaPartitions) reads exactly the partitions it names, joins no group and never rebalances.

[`KafkaPartitions::new`](api:ruststream_rdkafka::KafkaPartitions::new) takes the topic and its partition numbers.
It suits a reader pinned to a partition, an inspection or replay tool, and one consumer per partition.
The reader takes no part in the group protocol, so group members on the same topic neither see it nor take its partitions away.
A group stays optional and decides only where offsets go.
With `group` the reader commits into that group without joining it.
Without a group it commits nothing, so `start` must name `Earliest` or `Latest`: `Committed` has nothing to resume from and stops the service at startup.
`commit`, `lane_key` and `config` work as on [`KafkaTopic`](./kafka-topic.md); `Commit::Transactional` stops the service at startup, because an exactly-once pipeline commits through the group protocol.
Under the default [lane key](./lane-key.md) each named partition gets a lane of its own.
The subscription's name is the topic.
No publish addresses a subset of partitions, so a registration names where a retry copy goes, as the example does with `.out_retry(policy).to("orders.retry")`.

## Example

Partition 0 of `orders`, read from its first record by a service that names no group.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_subscriptions_partitions.rs#reader"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_subscriptions_partitions.rs#reader"
```

</TabItem>
</Tabs>
