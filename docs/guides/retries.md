---
sidebar_position: 4
description: "Retry a Kafka record now or later, cap the attempts and move what keeps failing to a dead-letter topic, on one topic or several."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries and dead-letter topics

This guide retries a Kafka record now or later, caps the attempts and moves what keeps failing to a dead-letter topic, on one topic or several.

Kafka does not hold a record back and does not count deliveries.
The framework does both: the mount site declares the cap and the dead-letter topic, and a retry travels as a copy of the record with a retry count in a header.

The example is a refunds service that sends refunds through a card processor.

## The subscriber

The processor refuses a refund in three ways, and each needs a different outcome.
`Busy` means its rate limit is used up: a retry right now would be refused again, so `retry_after` waits 30 seconds.
`Unavailable` means it did not answer: `retry` tries again at once.
`Declined` means the card account is closed, and another try gets the same answer, so the refund is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

[`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) settles the record and publishes a copy back to `refunds` once the delay has passed.
The copy carries the retry-count header ([`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)), raised by one.
The delayed copy is at-most-once: a process that stops before the delay ends loses it.
The copy is a new record at the end of the topic, so the records behind the refund do not wait for it.

[`retry`](api:ruststream::runtime::HandlerOutcome::retry) under a declared cap also becomes a copy, published at once.
Without a cap it hands the record back to the subscription under `Commit::Tracked`, as [Commits and delivery guarantees](commits.md) shows.

## The cap and the dead-letter topic

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) counts every delivery of one refund, the first included.
The fifth delivery is not retried: [`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) publishes it to `refunds.dlq`, payload and headers as they arrived.
A dropped refund settles and goes nowhere; the dead-letter topic holds only the refunds that ran out of attempts.
Retry and dead-letter topics are the service's infrastructure: create them like any other topic, and the framework only publishes to them.

## Reading the dead-letter topic

The dead-letter topic is an ordinary topic, and `review` reads it like any other.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

The retry-count header of a dead-lettered record holds the number of deliveries it went through, 5 here.
`review` reads it with `ctx.headers()` and logs the refund for a person to look at.

## Retries on several topics

A copy of a record is published to the topic the subscription reads.
[`KafkaTopic`](api:ruststream_rdkafka::KafkaTopic) reads one topic, so it needs nothing more.
[`KafkaTopics`](api:ruststream_rdkafka::KafkaTopics) and [`KafkaPartitions`](api:ruststream_rdkafka::KafkaPartitions) read a set that no single publish reaches, so their registration names where a copy goes, and the service refuses to start without it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#regional"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#regional"
```

</TabItem>
</Tabs>

The app mounts `process_regional_refund` with `.out_retry(Publish::default()).transform(ToSourceTopic)`.
[`ToSourceTopic`](api:ruststream_rdkafka::ToSourceTopic) sends each copy back to the topic its delivery came from, so a retried refund from `refunds.us` comes back on `refunds.us`.
It reads the topic from the Kafka context, so the handler takes a context key such as `Ctx<Topic>`; in the manual form its context type is `KafkaContext`.
`.out_retry(Publish::default()).to("refunds.retry")` sends every copy to one topic of your own instead; list that topic in the subscription so the copies come back.
The same rule holds for a subscription that never retries: every registration over a set of topics names a retry destination.

## Testing the delay

The tests in the example file run the app in process with a paused clock, so `tb.advance(BACKOFF)` lets the 30 seconds pass at once.
[Testing a Kafka service](testing.md) covers the harness.
