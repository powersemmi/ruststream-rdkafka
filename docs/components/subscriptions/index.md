---
title: Subscriptions
description: "A topic name in #[subscriber] reads that topic in the broker's default group; a descriptor carries every other setting."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

A topic name in `#[subscriber]` reads that topic in the broker's default group; a descriptor carries every other setting.

`#[subscriber("orders")]` reads the topic `orders`.
Kafka reads a topic only through a consumer group, and a bare name names none, so the group comes from [`KafkaBroker::default_group`](api:ruststream_rdkafka::KafkaBroker::default_group).
A subscription with no group, on a broker with no default group, stops the service at startup with an error that names the subscription.
Two subscriptions of one name and one group are two members of that group: each record goes to one of them.
Every other setting of a subscription belongs to a descriptor: [`KafkaTopic`](./kafka-topic.md) for one topic, [`KafkaTopics`](./kafka-topics.md) for several topics or a [pattern](./patterns.md), and [`KafkaPartitions`](./kafka-partitions.md) for named partitions.
A descriptor goes where the name goes, as the attribute's first argument.
The manual form passes the name to [`subscriber(name, body)`](api:ruststream::runtime::subscriber), and a descriptor joins the chain through [`map_source`](api:ruststream::runtime::SubscriberSettings::map_source).
A subscription is open once its group has assigned it its partitions, so everything published after startup reaches it.
Startup waits for that assignment up to [`KafkaBroker::assignment_timeout`](api:ruststream_rdkafka::KafkaBroker::assignment_timeout), 60 seconds by default.

## Example

The topic `orders`, read in the group `orders-svc`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_subscriptions_name.rs#name"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_subscriptions_name.rs#name"
```

</TabItem>
</Tabs>
