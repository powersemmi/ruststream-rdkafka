---
title: Exactly once
description: "KafkaEosPublish commits consumed offsets inside the producer's transaction, so positions move atomically with the records published."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Exactly once

[`KafkaEosPublish`](api:ruststream_rdkafka::KafkaEosPublish) commits consumed offsets inside the producer's transaction, so positions move atomically with the records published.

It is the consume-transform-produce shape of Kafka (KIP-447), named `EosPublish` in the prelude.
[`KafkaEosPublish::new`](api:ruststream_rdkafka::KafkaEosPublish::new) takes the pipeline id, which is also the producer's [transactional id](../index.md).
Each source subscription names the same id in [`Commit::Transactional`](api:ruststream_rdkafka::Commit::Transactional): its consumer stops committing on its own, and the pipeline commits its offsets.
One pipeline serves every lane of a `workers(n, by_key)` pool: publishes join the open transaction concurrently, and only the commit is exclusive.

The pipeline works in windows.
Every [`commit_interval`](api:ruststream_rdkafka::KafkaEosPublish::commit_interval) (100 ms by default) it waits until the window's deliveries have settled, adds their positions and the group's metadata to the transaction, and commits.
A record becomes visible at that commit, so end-to-end latency is at least the interval.
A failed publish, a failed commit or a participant that does not settle in time aborts the window.
The consumers then seek back to the last committed offsets, and the whole window is delivered and published again in a fresh transaction, so the output topic never holds a duplicate.
A `retry()` from a participant holds its window until the transaction deadline, [`transaction_timeout`](api:ruststream_rdkafka::KafkaEosPublish::transaction_timeout).
The `retry_after` copy does not apply to the pipeline's replies: a delayed copy would separate the record from its offset.
The window's commit runs on the runtime the broker connected on, so a handler on a dedicated thread may open a window.

The transactional commit needs consumer group membership.
So `Commit::Transactional` is a [`KafkaTopic`](../../subscriptions/kafka-topic.md) or [`KafkaTopics`](../../subscriptions/kafka-topics.md) mode; [`KafkaPartitions`](../../subscriptions/kafka-partitions.md), which joins no group, works with `Commit::Auto` and `Commit::Tracked`.
A replying handler reaches the window through [`EosReplies`](./eos-replies.md), and a handler that publishes by hand through [`EosPipeline`](./eos-pipeline.md).

## Example

An order is priced, and the priced order commits together with the offset of the order it answers.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_eos.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_eos.rs#handler"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_eos.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_eos.rs#app"
```

</TabItem>
</Tabs>
