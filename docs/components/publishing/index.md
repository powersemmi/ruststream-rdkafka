---
title: KafkaPublish
description: "KafkaPublish, Publish in the prelude, is the plain publish policy; every publish waits for the cluster's delivery report."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KafkaPublish

[`KafkaPublish`](api:ruststream_rdkafka::KafkaPublish), `Publish` in the prelude, is the plain publish policy; every publish waits for the cluster's delivery report.

A mount site names it wherever a registration publishes: `.out_reply(Publish::default())` for a reply, `.out(Slot, Publish::default())` for a slot, `.out_retry(Publish::default())` for retry copies.
The policy holds no connection; the runtime pairs it with the connected broker at startup into a [`KafkaPublisher`](api:ruststream_rdkafka::KafkaPublisher).
A registration that names no policy for its reply publishes through this one.
A publish returns once the cluster has acknowledged the record, as the producer's [durability](./durability.md) settings define it, or with the error it reported.
librdkafka queues records locally and sends them in batches; a full queue makes a publish wait for room, which slows the handler to the pace of the cluster.
[`queue_timeout`](api:ruststream_rdkafka::KafkaPublish::queue_timeout) bounds that wait: past it the publish fails with a queue-full error.
A reply goes to one topic and does not wait for an answer from the reader.
A synchronous exchange is a reply topic of the service's own plus a correlation header the requester sets and the responder copies to its reply.
Per record, a publish carries a [key](./keys.md) or a [partition](./partition.md); [`RoundRobin`](./round-robin.md) spreads replies over the partitions.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_publishing_publish.rs#publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_publishing_publish.rs#publish"
```

</TabItem>
</Tabs>
