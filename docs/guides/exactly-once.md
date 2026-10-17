---
sidebar_position: 6
description: "Publish several records as one Kafka transaction, run transactions under a worker pool, and make a consume-transform-produce handler exactly-once."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Transactions and exactly-once

This guide publishes several records as one Kafka transaction, runs transactions under a worker pool, and makes a consume-transform-produce handler exactly-once.

A Kafka transaction makes a set of records visible together.
A reader at `read_committed` isolation, librdkafka's default, sees all of them once the transaction commits and none of them after an abort.
In RustStream a transaction is a property of the publish policy at the mount site, and the handler names only the capability it uses.

## All or nothing from one handler

`ship` turns a paid order into one shipment line per item.
An order with an empty line is dropped, and none of its lines may reach the warehouse.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/exactly_once.rs#fanout"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/exactly_once.rs#fanout"
```

</TabItem>
</Tabs>

The handler takes its slot as `Out<impl TransactionalPublisher, Shipments>`: it names the core's [`TransactionalPublisher`](api:ruststream::TransactionalPublisher) capability and nothing of Kafka.
Records published between [`begin_transaction`](api:ruststream::TransactionalPublisher::begin_transaction) and [`commit`](api:ruststream::TransactionalPublisher::commit) become visible together.
[`abort`](api:ruststream::TransactionalPublisher::abort) discards them, so the first line of the order with an empty line never reaches a reader.
Outside an open transaction the slot publishes like a plain one.
A second `begin_transaction` while one is open reports [`KafkaError::TransactionBusy`](api:ruststream_rdkafka::KafkaError::TransactionBusy), and a `commit` or `abort` with nothing open reports [`KafkaError::NoTransaction`](api:ruststream_rdkafka::KafkaError::NoTransaction).

The transaction covers the published lines and not the consumed order.
`Commit::Tracked` commits the order's offset after the handler acks it.
A crash between the two delivers the order again and ships its lines a second time.
The exactly-once pipeline below closes that gap.

## Transactions under a worker pool

One Kafka producer runs one transaction at a time, so the handlers of a worker pool cannot share one transactional publisher.
`invoice` runs four lanes and gives each source partition a transaction of its own.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/exactly_once.rs#lanes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/exactly_once.rs#lanes"
```

</TabItem>
</Tabs>

`workers(4, by_key)` keys the lanes by partition, so one partition is handled in order on one lane.
The slot is [`PartitionLanes`](api:ruststream_rdkafka::PartitionLanes): [`for_partition`](api:ruststream_rdkafka::PartitionLanes::for_partition) returns the transactional publisher of the delivery's partition, created on its first use.
Lanes that handle different partitions run their transactions at the same time.
The transactional ids follow the topic's partitions, not the number of workers, so changing `workers(n)` changes no id.

This scheme needs the default lane key.
[`LaneKey::RecordKey`](api:ruststream_rdkafka::LaneKey::RecordKey) spreads one partition across several lanes, which would then share one partition's publisher and collide on its single transaction.

## The exactly-once pipeline

`enrich` reads a placed order and answers with the totals the warehouse plans with.
Each answer is published exactly once, even when the service crashes and the order is read again.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/exactly_once.rs#eos"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/exactly_once.rs#eos"
```

</TabItem>
</Tabs>

The handler only returns its reply; it holds no transaction code.
[`Commit::Transactional("enrichment-1")`](api:ruststream_rdkafka::Commit::Transactional) stops the subscription from committing offsets on its own.
The pipeline of the same id commits them instead, inside the transaction that carries the replies.
So the consumed position and the published records move together: either both commit, or neither does.

## The mount site

The mount site names the policy behind each slot and reply.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/exactly_once.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/exactly_once.rs#service"
```

</TabItem>
</Tabs>

[`transactional_id`](api:ruststream_rdkafka::KafkaPublish::transactional_id) turns the plain policy into a transactional one.
One transactional id belongs to one live producer: a second producer that starts with the same id fences the first, and the first can no longer commit.
Give every running instance of the service its own id, for example from the pod's ordinal.

[`per_partition`](api:ruststream_rdkafka::KafkaTransactionalPublish::per_partition) makes the id a base: the publishers of `invoice` take the ids `invoicing-1-p0`, `invoicing-1-p1` and so on.

[`EosPublish::new("enrichment-1")`](api:ruststream_rdkafka::KafkaEosPublish::new) is the pipeline, and its id is the one the subscription's commit mode names.
[`EosReplies`](api:ruststream_rdkafka::EosReplies) carries each delivery's source position onto its reply, which is how the pipeline pairs the two.
Without it the first reply fails instead of leaving outside the transaction.

## The commit window

The pipeline collects replies into a window and commits it every [`commit_interval`](api:ruststream_rdkafka::KafkaEosPublish::commit_interval), 100 ms unless set.
At the end of a window it waits for every delivery that published into it to settle, adds their positions and the group's metadata to the transaction, and commits.
So a reply becomes visible at least one commit interval after the handler returned it.

A failed publish, a failed commit or a delivery that does not settle in time aborts the window.
The consumers then seek back to the last committed offsets, and the whole window is handled again in a fresh transaction.
The committed output still holds each reply once.

The pipeline limits what its handlers can return and how they read:

- A `retry()` from a handler holds its window open until the transaction timeout, 30 seconds unless [`transaction_timeout`](api:ruststream_rdkafka::KafkaEosPublish::transaction_timeout) sets another, and then the window replays.
- `retry_after` does not belong in a pipeline handler: its delayed copy is published outside the window and breaks the pairing of records and offsets.
- [`KafkaPartitions`](api:ruststream_rdkafka::KafkaPartitions) does not combine with `Commit::Transactional`, and such a subscription fails to start: the pipeline commits through the consumer group, and a reader of fixed partitions takes no part in one.
