---
title: Settlement
sidebar_position: 6
description: "What ack, drop, retry and retry_after become on Kafka, which commits one offset per partition instead of acknowledging records."
---

import {Diagram} from '@site/src/components/Diagrams';

export const watermark = {
  "title": "Commit::Tracked on one partition: the stored position stops below the lowest delivery that has not settled",
  "text": "The diagram as text",
  "partition": "One partition, offsets as delivered",
  "settled": "settled",
  "unsettled": "running",
  "stored": ["Stored position: 11", "below the lowest", "running delivery"],
  "committed": ["Committed offset: 12", "the record the group", "resumes from"],
  "next": ["When 12 settles,", "the position moves", "to 14: 15 is still", "running"],
  "storedAt": "stored at",
  "commits": "auto-commit"
};

export const retries = {
  "title": "A retry on Kafka: the attempt cap decides between a copy to the topic, the dead-letter topic and a drop",
  "text": "The diagram as text",
  "record": ["A record", "x-ruststream-retry-count says which attempt it is"],
  "outcome": ["The handler returns", "retry_after(delay), or retry() under a declaration"],
  "cap": ["max_attempts: is this attempt below the cap?"],
  "copy": ["Below the cap:", "the original", "settles, a copy", "goes to the topic"],
  "deadLetter": ["At the cap:", "a copy goes to", "the dead-letter", "topic"],
  "dropped": ["At the cap,", "no dead letter:", "the record", "settles, no copy"],
  "arrives": ["The copy is a new record", "at the end of its partition"],
  "crash": ["The delayed copy is held in the process:", "a crash before the timer fires loses it"],
  "handled": "handled",
  "retries": "a retry",
  "below": "below",
  "atCap": "at the cap"
};

# Settlement

What `ack`, `drop`, `retry` and [`retry_after`](../components/settlement/retries.md) become on Kafka, which commits one offset per partition
instead of acknowledging records.

<Diagram name="watermark" words={watermark} />

Kafka keeps one committed offset per partition and group. The subscription's
[`Commit`](../components/settlement/index.md) mode decides how a handler's outcome reaches it.

Under [`Commit::Tracked`](../components/settlement/tracked.md), the crate turns librdkafka's
automatic offset store off and records every offset it delivers. A settlement stores the position
just below the lowest delivery still running, or the highest delivered offset once none is running.
Acks from parallel lanes can arrive in any order, and the stored position never passes unfinished
work. Offsets the consumer never receives, such as transaction markers or compacted records, do not
hold it back. librdkafka's timer commits the stored position as usual.

| Outcome | [`Commit::Auto`](../components/settlement/auto.md) (default) | `Commit::Tracked` | `Commit::Transactional` |
|---|---|---|---|
| `ack` | Nothing: librdkafka stored the offset when it handed the record over | The offset settles and the stored position moves up | The offset settles; the pipeline commits it with its next window |
| `drop` | Nothing, as for `ack` | The offset settles, as for `ack` | The offset settles, as for `ack` |
| `retry()` | Refused (`AckError::Unsupported`) and logged: nothing brings the record back | The record is delivered again ahead of what is fetched next, and the position waits for it | The open window cannot commit: it aborts and is read again |

A rebalance or a [seek](../components/positions/seek.md) that moves a partition discards its tracked state. A settlement of a delivery
read before the move changes nothing, because the partition's next reader starts from the committed
offset anyway.

Under [`Commit::Transactional`](api:ruststream_rdkafka::Commit::Transactional) the consumer commits
nothing itself. The [`EosPipeline`](api:ruststream_rdkafka::EosPipeline) of the same id adds the
settled positions to its producer transaction every `commit_interval` (100 milliseconds), so the
consumed offsets and the published records become visible together, as
[Kafka transactions](kafka/transactions.md) draws.

## Retries

<Diagram name="retries" words={retries} />

Kafka holds no record back and counts no deliveries, so the runtime does both. A `retry_after`
outcome, and a `retry()` on a registration that declares
[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) or a [dead-letter topic](../components/settlement/dead-letters.md),
take this path in every commit mode. The attempt count travels in the copy's
`x-ruststream-retry-count` header, and the first delivery counts as one.

The copy goes to the address the descriptor reports. A
[`KafkaTopic`](../components/subscriptions/kafka-topic.md) subscription reports its own topic. A
[`KafkaTopics`](../components/subscriptions/kafka-topics.md) or
[`KafkaPartitions`](../components/subscriptions/kafka-partitions.md) subscription reads a set no single
publish reaches, so its registration [names the destination](../components/settlement/to-source-topic.md), or the app refuses to start.

The copy keeps the record's key, so it lands on the key's partition again, behind the records
written since. Every consumer group reading the topic receives the copy, not only the one that
retried.
