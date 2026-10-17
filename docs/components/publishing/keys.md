---
sidebar_position: 1
title: Keys
description: "The partition-key header becomes the record's native key, so records sharing a key land on one partition, in order."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Keys

The partition-key header, [`PARTITION_KEY_HEADER`](api:ruststream_rdkafka::PARTITION_KEY_HEADER), becomes the record's native key, so records sharing a key land on one partition, in order.

The header is named `kafka-partition-key`.
On publish its value becomes the Kafka record key and does not travel as a header of its own.
Kafka's partitioner hashes the key, so every record of one key goes to one partition, and a consumer reads them in the order they were written.
A message type declares the header in its typed header contract, `#[outgoing(headers = ..)]`, with a field renamed to `kafka-partition-key`; a `HeaderMap` with that entry works the same.
On consume the header holds the record key again, and a handler reads the key as raw bytes through the [`Key`](../context/index.md) context key.
`workers(n, by_key)` with `LaneKey::RecordKey` keeps the same order inside one consumer: records of one key run on one lane.
A [`partition`](./partition.md) step places a record ahead of its key.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_publishing_keys.rs#keys"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_publishing_keys.rs#keys"
```

</TabItem>
</Tabs>
