---
title: Transactions
description: "transactional_id makes the publish policy transactional: publishes between begin and commit become visible together, or not at all."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Transactions

[`transactional_id`](api:ruststream_rdkafka::KafkaPublish::transactional_id) makes the publish policy transactional: publishes between begin and commit become visible together, or not at all.

The policy it returns is [`KafkaTransactionalPublish`](api:ruststream_rdkafka::KafkaTransactionalPublish), named `TransactionalPublish` in the prelude.
It pairs into a [`KafkaTransactionalPublisher`](api:ruststream_rdkafka::KafkaTransactionalPublisher), which carries the core's [`TransactionalPublisher`](api:ruststream::TransactionalPublisher) capability.
A handler bounds its slot `Out<impl TransactionalPublisher, Marker>` and calls `begin_transaction`, then `commit` or `abort`.
The producer is created and its transactions initialized at startup, when the policy pairs, so the handler holds a fenced producer from its first delivery.
Readers at Kafka's default `read_committed` isolation see the records of a transaction at its commit; an abort discards them on the broker.
Outside an open transaction the publisher sends like a plain one.

The transactional id is the fencing unit: initializing a producer with an id fences the previous producer that held it.
So the id is stable and unique per running instance, for example one per replica.
[`transaction_timeout`](api:ruststream_rdkafka::KafkaTransactionalPublish::transaction_timeout) bounds how long `begin`, `commit` and `abort` may block (30 seconds by default).
It is the call deadline; Kafka's own `transaction.timeout.ms` is a producer property, set through [`producer_config`](api:ruststream_rdkafka::KafkaBroker::producer_config).
[`queue_timeout`](api:ruststream_rdkafka::KafkaTransactionalPublish::queue_timeout) bounds the wait for space in librdkafka's local queue.

One producer runs one transaction at a time.
A second `begin_transaction` while one is open returns [`KafkaError::TransactionBusy`](api:ruststream_rdkafka::KafkaError::TransactionBusy), and a `commit` or `abort` with nothing open returns [`KafkaError::NoTransaction`](api:ruststream_rdkafka::KafkaError::NoTransaction).
So the handlers of a `workers(n)` pool do not share one transactional publisher; [`per_partition`](./per-partition.md) gives each source partition a publisher of its own.
The test harness records what the handler sent through the slot, committed or not, while the topic shows only what was committed.

## Example

An order ships one record per item, in one transaction.
An item without a name aborts the transaction, which discards the shipments already sent.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_transactions.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_transactions.rs#handler"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_transactions.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_transactions.rs#app"
```

</TabItem>
</Tabs>
