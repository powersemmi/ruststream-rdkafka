---
sidebar_position: 3
title: Operations
description: "A receive operation reports the consumer group its descriptor names, and the client id its config sets."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Operations

A receive operation reports the consumer group its descriptor names, and the client id its [`config`](../subscriptions/passthrough.md) sets.

The operation binding carries `groupId` and `clientId`, each as a one-value schema.
`groupId` is the group the descriptor names, such as [`KafkaTopic::group`](api:ruststream_rdkafka::KafkaTopic::group).
The document is built before the broker connects, from the descriptor alone, so the broker's `default_group` does not appear.
`clientId` appears when the descriptor's `config` sets `client.id`.
An operation with neither carries no binding.

## Example

The operation `receive_orders` reports the group `orders-svc` and the client id `orders-reader`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_asyncapi.rs#operations"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_asyncapi.rs#operations"
```

</TabItem>
</Tabs>
