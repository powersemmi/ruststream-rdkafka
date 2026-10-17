---
sidebar_position: 3
title: Startup
description: "Startup probes the cluster within connect_timeout and waits up to assignment_timeout for each group to assign its partitions."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Startup

Startup probes the cluster within [`connect_timeout`](api:ruststream_rdkafka::KafkaBroker::connect_timeout) and waits up to [`assignment_timeout`](api:ruststream_rdkafka::KafkaBroker::assignment_timeout) for each group to assign its partitions.

The connect fetches the cluster's metadata.
A cluster that does not answer within `connect_timeout`, 30 seconds by default, fails startup with [`KafkaError::Connect`](api:ruststream_rdkafka::KafkaError::Connect).
The connect also checks the producer's properties, and opens no producer: the first publish opens it, so a service that only consumes runs none.

A subscription through a consumer group is open once the group has assigned this member its partitions and the offset each one starts from is known.
From that moment everything published to the topic reaches it.
Startup waits for each group up to `assignment_timeout`, 60 seconds by default, and then fails with [`KafkaError::Subscribe`](api:ruststream_rdkafka::KafkaError::Subscribe), naming the subscription and its group.
A new group waits the coordinator's `group.initial.rebalance.delay.ms`, three seconds by default.
A group that still holds a member which left without a word waits until that member's `session.timeout.ms` expires, 45 seconds by default.
A subscription that names its partitions with [`KafkaPartitions`](api:ruststream_rdkafka::KafkaPartitions) joins no group and does not wait.

A subscription to a topic the cluster does not have yet opens without partitions.
When the topic appears, the group reads it from where `auto.offset.reset` puts it, the end of the log by default, so the record that created the topic is not read.

## Example

The probe gives up after five seconds, and each group gets twenty seconds to assign its partitions.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_broker_startup.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_broker_startup.rs#broker"
```

</TabItem>
</Tabs>
