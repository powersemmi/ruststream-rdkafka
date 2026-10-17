---
sidebar_position: 5
description: "Read a topic again from the start or from a moment, and move a running subscription from inside its handler."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Replay and seeking

This guide reads a topic again from the start or from a moment, and moves a running subscription from inside its handler.

Kafka keeps the log after a consumer has read it.
A service can read it again: to rebuild a read model, to backfill a new consumer, or to skip records it cannot use.
A subscription takes its [start position](../components/positions/start-at.md) at the mount site, and a handler moves its own subscription through the delivery's [context](../components/context/index.md).

## The read models

The service keeps two read models in memory: the balance of every account and the spend of every card.
Each posting carries the ledger's revision.
A correction of past postings is published as a new revision, and a read model that meets one rebuilds itself from the start of the log.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#projection"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#projection"
```

</TabItem>
</Tabs>

## Start from the beginning

The balances live in memory, so after a restart they are built again from the whole log.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#balance"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#balance"
```

</TabItem>
</Tabs>

`start_at(KafkaPosition::earliest())` opens the subscription at the start of the log on every start, whatever the group `balances` committed before.
[`StartOffset`](../components/subscriptions/start-offset.md) on the descriptor differs here: it applies only while the group has no committed offset.
The position lands on the partitions the group assigns to this consumer, before their first record.

[`KafkaPosition`](../components/positions/index.md) has five positions:

| Position | Where the subscription resumes |
|---|---|
| [`earliest()`](api:ruststream_rdkafka::KafkaPosition::earliest) | the oldest record Kafka keeps |
| [`latest()`](api:ruststream_rdkafka::KafkaPosition::latest) | the end of the log, so only new records are read |
| [`offset(partition, n)`](api:ruststream_rdkafka::KafkaPosition::offset) | offset `n` of one partition |
| [`topic_offset(topic, partition, n)`](api:ruststream_rdkafka::KafkaPosition::topic_offset) | offset `n` of one partition of one topic, for a subscription that reads several topics |
| [`timestamp(ms)`](api:ruststream_rdkafka::KafkaPosition::timestamp) | the first record stamped at or after `ms`, in every partition |

A start position that names none of the subscription's partitions fails the start.
Under [`Assignment::CooperativeSticky`](../components/subscriptions/assignment.md) the first assignment may be only part of the group's share, and partitions that arrive in a later increment are not moved.

## Seek from a handler

The same handler rebuilds the balances when a posting of a newer revision arrives.
[`Ctx<SeekHandle>`](../components/positions/seek.md) gives the handler the [`KafkaSeeker`](api:ruststream_rdkafka::KafkaSeeker) of its own subscription.
[`seek`](api:ruststream::Seeker::seek) moves the subscription to the start of the log, and the handler reads every posting again.
The posting that started the rebuild comes back as well, and this time the projection applies it.

A seek takes any of the positions above.
`Ctx<Position>` holds the delivery's own position: a seek there reads the record again, and everything after it.
`Ctx<Partition>` and `Ctx<Offset>` give the coordinates for an `offset(..)` position.

## Seek from a batch handler

Card postings arrive in volume, so the spend is built in [batches](../components/subscriptions/batches.md) of up to 500.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#batch"
```

</TabItem>
</Tabs>

A batch handler declares [`KafkaBatchContext`](../components/context/batch-context.md) instead of the per-delivery context.
It carries the same `SeekHandle` key and no position, because a batch spans many records.
The subscription moves once the whole batch has settled.

## Start from a moment

The payout review reads the payouts of the last seven days on every start.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#payouts"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#payouts"
```

</TabItem>
</Tabs>

The moment is known only when the service starts, so the mount site names the position instead of the attribute.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#service"
```

</TabItem>
</Tabs>

`KafkaPosition::timestamp` takes milliseconds since the epoch, the unit Kafka stamps records with.
Kafka resolves the moment in each partition to the first record stamped at or after it.
A partition with no such record resumes at its end.

## What a seek does to the group

A seek moves this consumer, not the group.
It moves the partitions this member holds and commits nothing for anyone.
Other members of the group keep reading where they are.
The next rebalance discards the seek: whoever gets a partition then resumes from the group's committed offset.
A `start_at` position applies again at every start.

## Seeking under Commit::Tracked

All three subscriptions commit with [`Commit::Tracked`](../components/settlement/tracked.md) ([Commits and delivery guarantees](commits.md)).
A seek clears the tracked position of every partition it moves.
A delivery pulled before the seek settles into nothing, so the ack of the posting that started the rebuild commits nothing.
A later commit therefore never passes a record that the seek replayed and no handler has handled yet.
