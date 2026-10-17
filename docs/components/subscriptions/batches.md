---
sidebar_position: 9
title: Batches
description: "A slice handler reads one fetched delivery plus what librdkafka already holds, up to the size the mount site names."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler reads one fetched delivery plus what librdkafka already holds, up to the size the mount site names.

A handler over `&[T]` is a batch handler, and [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is its largest batch.
The size travels down to the consumer's poll, which never hands the body more than `n` records.
librdkafka fetches records ahead in the background, and a batch takes what it already holds without waiting for more.
So a batch is often smaller than `n`, and a quiet topic yields batches of one.
One outcome settles every record of the batch.
A `Vec<HandlerOutcome>` settles record `i` with outcome `i`.
Under `Commit::Tracked` a record that asks to come back holds the committed position in front of it until it settles; a process that stops first replays the acked records behind it too.
A `retry_after` in a batch becomes a copy published back to the topic after the delay, so the registration names its retry publisher with `.out_retry(policy)`.
How much librdkafka queues locally is a consumer property, set through the descriptor's [passthrough](./passthrough.md) (`queued.max.messages.kbytes`, `fetch.queue.backoff.ms`).
`workers(n)` keeps up to `n` batches in flight at once.
A batch body reads a [`KafkaBatchContext`](api:ruststream_rdkafka::context::KafkaBatchContext), which carries the seek handle and no per-record field.
In the manual form the body is an `impl Handle<[T]>`, where `Ok(())` acks the batch and `Err(outcomes)` settles record by record.

## Example

Three orders waiting in the consumer's queue arrive in one call.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_batches.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_batches.rs#handler"
```

</TabItem>
</Tabs>

## One outcome per record

Only the unsettled payment comes back, as a copy published after the delay.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_batches.rs#selective"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_batches.rs#selective"
```

</TabItem>
</Tabs>
