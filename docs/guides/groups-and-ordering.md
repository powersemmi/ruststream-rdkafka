---
sidebar_position: 2
description: "Read a topic in consumer groups, keep the order of each key with parallel lanes, and place records on partitions by key, by number or in turn."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Topics, consumer groups and ordering

This guide reads a topic in consumer groups, keeps the order of each key with parallel lanes, and places records on partitions by key, by number or in turn.

The example is a bank's ledger service.
It posts transfers to account ledgers, screens them for fraud, keeps an index of its records and opens cases for disputed transfers.

## Order per account

Kafka keeps the order of records inside one partition.
Records with the same key land on the same partition, so the order of each key holds.
`post` reads transfers keyed by account and handles up to eight at once without breaking that order.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#post"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#post"
```

</TabItem>
</Tabs>

`workers(8, by_key)` runs eight lanes; deliveries with the same lane key run one after another on one lane.
[`LaneKey::RecordKey`](api:ruststream_rdkafka::LaneKey::RecordKey) keys the lanes by the record key, so two transfers of one account never run at the same time, even inside one partition.
The default, [`LaneKey::Partition`](api:ruststream_rdkafka::LaneKey::Partition), keys them by the source partition, which also covers records without a key.
In the manual form the same setting is [`workers_by_key`](api:ruststream::runtime::SubscriberSettings::workers_by_key).

The record key reaches the handler as the `kafka-partition-key` header ([`PARTITION_KEY_HEADER`](api:ruststream_rdkafka::PARTITION_KEY_HEADER)).
`AccountKey` is a header contract with one field under that name.
`Headers<AccountKey>` reads the key of the delivery, and `with_headers(&key)` writes it as the key of the ledger entry.
So the entry goes to the partition of its account in `bank.entries` too, and readers of the ledger see each account in order.
A transfer without a key fails the header contract and is settled by the decode policy, which drops it by default.

## Consumer groups

A consumer group reads each record once, whichever of its members gets the partition.
Every group reads its own copy.
`screen` reads the same transfers in a group of its own, so it sees every transfer that `post` sees.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#screen"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#screen"
```

</TabItem>
</Tabs>

[`KafkaTopic::group`](api:ruststream_rdkafka::KafkaTopic::group) names the group.
A subscription that names none joins the broker's [`default_group`](api:ruststream_rdkafka::KafkaBroker::default_group); `post` does.
Kafka reads a topic only inside a group, so a service with neither fails at startup.

[`StartOffset`](api:ruststream_rdkafka::StartOffset) says where a group starts when it has no committed offset for a partition.
That is the case on its first run and after retention deleted its offset.
`Earliest` starts at the oldest record, `Latest` at the end of the log, and the default `Committed` leaves the choice to librdkafka's `auto.offset.reset`, which is `latest`.

[`Assignment`](api:ruststream_rdkafka::Assignment) picks how the group shares partitions among its members.
`CooperativeSticky` moves only the partitions that change owner when a member joins or leaves, so the others keep running.

Startup waits until the group has assigned this member its partitions, up to [`assignment_timeout`](api:ruststream_rdkafka::KafkaBroker::assignment_timeout) (60 seconds by default).
After that, every record published to the topic reaches the subscription.
Give each subscription of a service its own group.
Two subscriptions of different topics in one group rebalance each other: when the second joins, the first loses its partitions for a moment.
A record published during that moment is read from where `auto.offset.reset` puts the first member back, which under `latest` is past the record.

## Several topics and a pattern

[`KafkaTopics`](api:ruststream_rdkafka::KafkaTopics) reads several topics through one consumer and one group.
[`KafkaTopics::pattern`](api:ruststream_rdkafka::KafkaTopics::pattern) reads every topic whose name matches a regular expression, which must start with `^`.
`archive` reads every topic of the bank and records where each record sits.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#archive"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#archive"
```

</TabItem>
</Tabs>

The handler reads the coordinates of its delivery from the context: [`Topic`](api:ruststream_rdkafka::context::keys::Topic), [`Partition`](api:ruststream_rdkafka::context::keys::Partition) and [`Offset`](api:ruststream_rdkafka::context::keys::Offset).
All matched topics share one payload type; `archive` decodes any JSON.
A topic created after startup joins the pattern on librdkafka's next metadata refresh.

[`KafkaPartitions`](api:ruststream_rdkafka::KafkaPartitions) reads the partitions it names and joins no group.
It never rebalances, which suits one process per partition or a tool that inspects one partition.

## Placing records on partitions

`open_case` turns a disputed transfer into a case on `bank.cases`.
That topic has three partitions: 0 and 1 share the regular cases evenly, and partition 2 holds the urgent ones.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#cases"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#cases"
```

</TabItem>
</Tabs>

[`partition(n)`](api:ruststream_rdkafka::KafkaPublishSteps::partition) pins one record to a partition.
It is a step of [`KafkaPublishSteps`](api:ruststream_rdkafka::KafkaPublishSteps), so the slot is bounded `Publisher<Options = KafkaOptions>` to offer it.
A pinned partition wins over the record key, and the record key wins over librdkafka's partitioner.
A partition the topic does not have fails the publish.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#app"
```

</TabItem>
</Tabs>

[`RoundRobin::partitions(2)`](api:ruststream_rdkafka::RoundRobin::partitions) places each case that names no partition on the next of partitions 0 and 1.
librdkafka places a record without a key on a random partition, which leaves some consumers busier than others; the cycle spreads the records evenly.
A record with a key or a pinned partition keeps its placement.
The count is fixed: it matches the partitions the cycle should use, never more than the topic has.

A subscription over a set of topics has no single topic to send a retry copy to, so the app names one with [`ToSourceTopic`](api:ruststream_rdkafka::ToSourceTopic); [Retries and dead-letter topics](retries.md) explains it.

The tests in the example file run the app in process.
The in-process Kafka gives every topic one partition, so the test of placement on several partitions runs against a cluster:

```bash
KAFKA_TEST_URL=localhost:9092 cargo test
```
