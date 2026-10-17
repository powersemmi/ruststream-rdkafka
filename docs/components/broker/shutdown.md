---
sidebar_position: 4
title: Shutdown
description: "Shutdown flushes in-flight publishes within flush_timeout, and a publisher used afterwards reports KafkaError::Closed."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

Shutdown flushes in-flight publishes within [`flush_timeout`](api:ruststream_rdkafka::KafkaBroker::flush_timeout), and a publisher used afterwards reports [`KafkaError::Closed`](api:ruststream_rdkafka::KafkaError::Closed).

librdkafka sends records in the background, so a publish the cluster has not yet confirmed may still sit in the producer's queue.
At shutdown the runtime closes the connection first and then flushes that queue.
The flush waits up to `flush_timeout`, 30 seconds by default.
Records still queued after it fail the shutdown with [`KafkaError::Publish`](api:ruststream_rdkafka::KafkaError::Publish).
A service that never published has no producer and nothing to flush.

A publisher taken before the shutdown keeps pointing at the closed connection.
A publish through it, or a commit or an abort of its transaction, returns `KafkaError::Closed` with the topic or the transactional id, and never succeeds against the dead connection.
A tool that drives the broker by hand receives a [`ClosedKafkaBroker`](api:ruststream_rdkafka::ClosedKafkaBroker) from the shutdown; its [`unflushed_records`](api:ruststream_rdkafka::ClosedKafkaBroker::unflushed_records) is the number of records librdkafka still held.

## Example

The flush gets ten seconds, and the shipments the handler replied with reach the cluster before the service exits.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_broker_shutdown.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_broker_shutdown.rs#broker"
```

</TabItem>
</Tabs>
