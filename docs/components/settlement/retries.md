---
sidebar_position: 3
title: Retries
description: "retry_after asks for a later delivery: the runtime republishes a copy to the subscription with its delivery count in a header."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) asks for a later delivery: the runtime republishes a copy to the subscription with its delivery count in a header.

Kafka holds no record back and counts no deliveries, so the runtime does both.
It settles the delivery, waits out the delay and publishes a copy of the record, payload and headers, to the topic the subscription reads.
The copy carries [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) incremented, and the handler reads it like any other header.
The copy waits in the service's memory, so it is at-most-once over the delay: a process that exits before the timer fires loses it.
[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) caps the deliveries of one record at the mount site, the first one included.
Under a cap every retry, an immediate one too, travels as such a copy, because the count lives in the header.
The delivery that reaches the cap goes to the [dead-letter topic](./dead-letters.md).
The copies leave through the [`Retry`](api:ruststream::runtime::Retry) slot, which [`out_retry`](api:ruststream::runtime::Mounting::out_retry) replaces.
A subscription over several topics names where its copies go, as [ToSourceTopic](./to-source-topic.md) shows.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_retries.rs#retry_after"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_retries.rs#retry_after"
```

</TabItem>
</Tabs>
