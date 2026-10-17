---
sidebar_position: 3
description: "Choose what ack and nack commit on Kafka: librdkafka's own commits, precise at-least-once with Commit::Tracked, and batches settled element by element."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Commits and delivery guarantees

This guide chooses what `ack` and `nack` commit on Kafka: librdkafka's own commits, precise at-least-once with [`Commit::Tracked`](../components/settlement/tracked.md), and batches settled element by element.

Kafka does not settle single messages.
A consumer group commits a position per partition, and everything below it counts as read.
A handler still settles each delivery, so the subscription's [`Commit`](../components/settlement/index.md) mode decides how those settlements become a position.

| Mode | `ack` | `drop` | `retry` | After a crash |
|---|---|---|---|---|
| [`Auto`](../components/settlement/auto.md) (default) | nothing | nothing | reports unsupported, the record does not come back | the tail of handled records is read again, or records stored but not handled are skipped |
| [`Tracked`](api:ruststream_rdkafka::Commit::Tracked) | settles the offset | settles the offset | the record comes back | every record not settled is read again |
| [`Transactional`](api:ruststream_rdkafka::Commit::Transactional) | settles the offset, committed with the pipeline's transaction | the same as `ack` | stalls the window until the transaction deadline | the open window is read again |

The example is a payouts service: payouts must not be lost, page views may be.
[Transactions and exactly-once](exactly-once.md) covers the third mode.

## At-least-once with `Commit::Tracked`

`pay_out` sends a payout through the bank and answers with a confirmation.
When the bank does not answer, the payout comes back and is tried again.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/commits.rs#tracked"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/commits.rs#tracked"
```

</TabItem>
</Tabs>

Under [`Commit::Tracked`](api:ruststream_rdkafka::Commit::Tracked) an `ack` moves the committed position up to the lowest delivery that is still open.
Acks from concurrent workers arrive out of order, and the position never passes a payout that is still in flight.
[`retry`](api:ruststream::runtime::HandlerOutcome::retry) hands the delivery back to its subscription, which delivers it again before anything it fetches next.
Until then its offset stays open, so the position does not move past it.
A process that stops first gets the payout back from Kafka when it starts again.

So a payout is never lost, and it may be handled twice: once before a crash and once after.
The bank call has to accept a repeated payout, for example by its id.
A `retry` without an attempt cap comes back forever; [Retries and dead-letter topics](retries.md) adds the cap.

## librdkafka's own commits

`count_view` counts page views in an analytics store.
A lost view does not matter here, so the subscription keeps the default mode.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/commits.rs#auto"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/commits.rs#auto"
```

</TabItem>
</Tabs>

Under [`Commit::Auto`](api:ruststream_rdkafka::Commit::Auto) librdkafka stores the position when it hands the record to the application and commits it on a timer.
`ack` and `drop` change nothing.
`retry` reports itself unsupported, and the view does not come back.
This mode costs the least, and a crash can lose handled records or skip records that were stored but not handled.

## Batches

`reconcile` takes statement lines in [batches](../components/subscriptions/batches.md) of up to 500 and settles each line on its own.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/commits.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/commits.rs#batch"
```

</TabItem>
</Tabs>

A batch is one delivery plus the records librdkafka has already fetched, with no extra waiting, so it is often smaller than the size the mount site names.
The returned list settles the batch element by element; in the manual form `Ok(())` acks every element and `Err(outcomes)` settles each one.
Under `Commit::Tracked` an element that asks for a retry comes back alone, and the committed position stops in front of it until it settles.
A process that stops first reads the acked elements behind it again: duplicates, not loss.

How much librdkafka keeps queued is a consumer property, set with [`KafkaTopic::config`](../components/subscriptions/passthrough.md).
When the queue is full, librdkafka pauses fetching for `fetch.queue.backoff.ms` (1000 ms by default), which slows large records behind a slow handler.
`.config("fetch.queue.backoff.ms", "10")` shortens the pause.

## The producer side

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/commits.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/commits.rs#app"
```

</TabItem>
</Tabs>

At-least-once from end to end needs the producer side too.
`enable.idempotence` makes librdkafka's own resends of a record land once; [`producer_config`](../components/broker/config.md) sets it and every other producer property, [`acks`](../components/publishing/durability.md) included.
Every publish waits for the cluster's delivery report, so a publish that returned `Ok` was accepted by Kafka.

A publish waits for space while librdkafka's local queue is full, which slows the handler down to what the cluster takes.
[`queue_timeout`](api:ruststream_rdkafka::KafkaPublish::queue_timeout) bounds that wait and turns it into an error.
At shutdown the broker flushes the records still in flight, for up to [`flush_timeout`](../components/broker/shutdown.md) (30 seconds by default).
[`ClosedKafkaBroker::unflushed_records`](api:ruststream_rdkafka::ClosedKafkaBroker::unflushed_records) counts the records that did not make it.

The tests in the example file run the app in process and replace the bank and the analytics store with clients that fail once.
The [in-process Kafka](../components/testing/index.md) commits as a cluster does, so the payout comes back under `Commit::Tracked` and the page view does not under `Commit::Auto`.
