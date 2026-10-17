---
sidebar_position: 1
title: per_partition
description: "per_partition gives each source partition its own transactional publisher, so a worker pool runs independent transactions."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# per_partition

[`per_partition`](api:ruststream_rdkafka::KafkaTransactionalPublish::per_partition) gives each source partition its own transactional publisher, so a worker pool runs independent transactions.

It turns the policy into a [`KafkaPartitionedPublish`](api:ruststream_rdkafka::KafkaPartitionedPublish), named `PartitionedPublish` in the prelude.
The transactional id becomes a base: the publisher of partition 3 is fenced by `"{base}-p3"`.
A handler bounds its slot `Out<impl PartitionLanes>` and reads its delivery's partition through `Ctx<Partition>`.
[`PartitionLanes::for_partition`](api:ruststream_rdkafka::PartitionLanes::for_partition) returns that partition's publisher, created and initialized on first use.
The live value is [`TransactionalPartitions`](api:ruststream_rdkafka::TransactionalPartitions), and its clones share the publishers.

The scope is the source partition because one producer runs one transaction at a time.
Under the default [`LaneKey::Partition`](../subscriptions/lane-key.md), a `workers(n, by_key)` pool processes each partition in order on one lane, so each lane has a transaction of its own.
The set of ids follows the topic's partitions, not the worker count: changing `n` keeps the ids and their fencing.
It does not combine with `LaneKey::RecordKey`, which spreads one partition over several lanes; two lanes would then meet on one producer and get `KafkaError::TransactionBusy`.
What a lane publishes leaves through the partition's own publisher, so a test reads it on the topic, not on the slot's record.

## Example

An invoice's lines are published in the transaction of the invoice's source partition.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_transactions_partitions.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_transactions_partitions.rs#handler"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_transactions_partitions.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_transactions_partitions.rs#app"
```

</TabItem>
</Tabs>
