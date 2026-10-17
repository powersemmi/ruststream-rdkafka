---
title: Commit
description: "Commit decides what an ack or a nack does on Kafka, which settles a consumer group by committed position, not per message."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Commit

[`Commit`](api:ruststream_rdkafka::Commit) decides what an ack or a nack does on Kafka, which settles a consumer group by committed position, not per message.

A consumer group stores one offset per partition: the position its next member starts reading at.
A handler settles one delivery at a time, so the subscription has to turn those outcomes into that one position.
The descriptor chooses how with `.commit(..)`: [`KafkaTopic`](../subscriptions/kafka-topic.md), [`KafkaTopics`](../subscriptions/kafka-topics.md) and [`KafkaPartitions`](../subscriptions/kafka-partitions.md) all take it.
[`Commit::Auto`](./auto.md) is the default: librdkafka stores the position as a record is handed over and commits it on a timer.
[`Commit::Tracked`](./tracked.md) moves the position only past deliveries the handlers have settled, so a crash brings back what was not finished.
`Commit::Transactional` hands the offsets to an [exactly-once pipeline](../transactions/exactly-once/index.md), which commits them in the producer's transaction.
Under `Tracked` an ack and a [`drop`](api:ruststream::runtime::HandlerOutcome::drop) both settle the offset, and nothing comes back.
A [`retry`](api:ruststream::runtime::HandlerOutcome::retry) hands the delivery back to its subscription instead.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_settlement_commit.rs#commit"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_settlement_commit.rs#commit"
```

</TabItem>
</Tabs>
