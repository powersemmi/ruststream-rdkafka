---
sidebar_position: 5
title: Prelude
description: "One glob, ruststream_rdkafka::prelude, brings the core prelude, the descriptors, the policies under concept names and the context keys."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

One glob, [`ruststream_rdkafka::prelude`](api:ruststream_rdkafka::prelude), brings the core prelude, the descriptors, the policies under concept names and the context keys.

`use ruststream_rdkafka::prelude::*;` is the one import a Kafka service writes.
It carries the core's own prelude, so `#[subscriber]`, `RustStream`, `HandlerOutcome` and `Out` come with it.
It adds `KafkaBroker`, the descriptors `KafkaTopic`, `KafkaTopics` and `KafkaPartitions` with their settings (`Commit`, `StartOffset`, `Assignment`, `LaneKey`), the reply transforms `RoundRobin` and `ToSourceTopic`, and the context keys `Topic`, `Partition`, `Position`, `Source` and `SeekHandle`.

The publish policies come under the names every broker crate uses: `Publish` is [`KafkaPublish`](api:ruststream_rdkafka::KafkaPublish), `TransactionalPublish` is [`KafkaTransactionalPublish`](api:ruststream_rdkafka::KafkaTransactionalPublish), `PartitionedPublish` is [`KafkaPartitionedPublish`](api:ruststream_rdkafka::KafkaPartitionedPublish), and `EosPublish` is [`KafkaEosPublish`](api:ruststream_rdkafka::KafkaEosPublish).
So a mount site reads the same on every broker.
Two broker preludes in one file collide on these names; the prefixed types at the crate root tell them apart.

A handler body bounds its slots with capability traits, such as `Out<impl TransactionalPublisher>`, and names no broker type.
The one exception is a body that sets a per-record setting: `partition(..)` needs [`KafkaPublishSteps`](api:ruststream_rdkafka::KafkaPublishSteps) in scope and the slot bounded `Out<impl Publisher<Options = KafkaOptions>, Marker>`, both from this prelude.

## Example

The descriptor, its settings and the reply policy all come from the one glob.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_broker_prelude.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_broker_prelude.rs#app"
```

</TabItem>
</Tabs>
