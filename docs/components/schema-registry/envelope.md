---
sidebar_position: 9
title: Envelope
description: "parse_envelope and encode_envelope read and write the Confluent wire format: a zero byte, the schema id, the datum."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Envelope

[`parse_envelope`](api:ruststream_rdkafka::schema_registry::parse_envelope) and [`encode_envelope`](api:ruststream_rdkafka::schema_registry::encode_envelope) read and write the Confluent wire format: a zero byte, the schema id, the datum.

The schema id takes four bytes, big-endian.
`parse_envelope` splits a payload into the id and a view of the datum, with no copy.
It returns `None` for a payload that does not open with the zero byte or is shorter than five bytes, which reads as "not framed".
`encode_envelope` writes the five bytes and the datum into a new buffer.
Neither function reaches a [registry](./index.md).

A handler that works on the framed bytes takes them through the byte lanes: a `#[derive(Deserialized)]` view of the delivery in, a `#[derive(Serialized)]` buffer out.
No codec runs on either side, so the bytes travel as they are.
The example moves records between two registries and rewrites the id each record names.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_registry_envelope.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_registry_envelope.rs#app"
```

</TabItem>
</Tabs>
