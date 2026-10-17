---
title: Testing
description: "TestApp::start connects KafkaBroker to an in-process cluster with topics, groups, offsets and transactions."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`KafkaBroker`](../broker/index.md) to an in-process cluster with topics, groups, offsets and transactions.

A test hands the harness the app `main` runs, unchanged, and addresses the broker by its production type, `tb.broker::<KafkaBroker>()`.
The `testing` feature in `[dev-dependencies]` adds the in-process mode, and turns on the core's harness with it.

The cluster has no settings of its own.
It reads what librdkafka would read: the broker's [`config`](../broker/config.md) and `producer_config`, each descriptor's options, and librdkafka's defaults for the rest.
A property name or value librdkafka refuses fails `TestApp::start` as it fails the connect.

The cluster keeps Kafka's semantics.
A topic comes into being with one partition when a record is produced to it or a subscription names it.
Each consumer group reads a record once, and every group reads its own copy.
The subscription's [commit mode](../settlement/index.md) decides what is committed, and a [seek](../positions/seek.md) moves the partitions the subscription reads.
A `read_committed` reader, librdkafka's default, sees a [transaction's](../transactions/index.md) records when it commits and never after an abort.
The cluster refuses what a cluster refuses: a partition the topic lacks, an illegal topic name, a record over `message.max.bytes`.

A publish through the harness returns once every handler it woke has settled, a reply or a committed transaction included.
The assertions that follow read finished state.
What only a cluster has, such as a topic with several partitions or the timing of a real group join, is for a [live test](./live.md).

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-rdkafka = { version = "0.7", features = ["testing"] }
```

The first test publishes a payment and reads how the handler settled it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_testing.rs#testapp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_testing.rs#testapp"
```

</TabItem>
</Tabs>

A refund publishes its lines inside one transaction, and the handler commits or aborts it.
The topic shows the lines of the committed refund and none of the aborted one.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_testing.rs#transactions"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_testing.rs#transactions"
```

</TabItem>
</Tabs>
