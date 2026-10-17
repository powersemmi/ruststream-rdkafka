---
sidebar_position: 2
title: Tracked
description: "Commit::Tracked commits below the lowest unsettled delivery, so a retry comes back and nothing is committed past it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Tracked

[`Commit::Tracked`](api:ruststream_rdkafka::Commit::Tracked) commits below the lowest unsettled delivery, so a retry comes back and nothing is committed past it.

The subscription stores a position only when a handler settles a delivery.
The stored position is the offset just below the lowest delivery still in flight, so handlers that finish out of order, on several `workers`, never commit past an unfinished one.
librdkafka's timer commits the stored position in the background and once more when the consumer closes.
An ack and a [`drop`](api:ruststream::runtime::HandlerOutcome::drop) settle the offset.
A [`retry`](api:ruststream::runtime::HandlerOutcome::retry) hands the delivery back to its subscription, which delivers it again before the records behind it.
Until it is settled, the group's committed position stays below it, so a crash or a rebalance brings it to the next member.
Offsets the consumer never receives, such as transaction markers and compacted records, do not hold the position back.
Tracked with an idempotent producer gives at-least-once from input to output, as [Durability](../publishing/durability.md) shows.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_settlement_commit.rs#tracked"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_settlement_commit.rs#tracked"
```

</TabItem>
</Tabs>
