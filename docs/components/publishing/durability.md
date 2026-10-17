---
sidebar_position: 4
title: Durability
description: "Durability is the producer's acks; an idempotent producer with Commit::Tracked gives at-least-once from input to output."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Durability

Durability is the producer's `acks`; an idempotent producer with [`Commit::Tracked`](../settlement/tracked.md) gives at-least-once from input to output.

[`producer_config`](../broker/config.md) sets a librdkafka producer property on the broker, for every publisher of the service.
`acks` says which replicas confirm a record before a publish returns: `all` waits for every in-sync replica.
`enable.idempotence` makes the producer number its sends, so the cluster writes a record once even when the producer sends it again.
librdkafka checks the values when the broker connects, so a wrong value stops the service at startup.
A handler that replies settles its input after the reply's publish returned.
Under `Commit::Tracked` the input's offset is committed only then, so a crash before it brings the input back and the reply is sent again.
A reader of the output can therefore see a reply twice; exactly once takes a [transactional pipeline](../transactions/exactly-once/index.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_publishing_durability.rs#durability"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_publishing_durability.rs#durability"
```

</TabItem>
</Tabs>
