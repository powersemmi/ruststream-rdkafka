---
sidebar_position: 7
title: SchemaFramed
description: "SchemaFramed wraps a codec such as JsonCodec in the Confluent envelope, so a registry-backed JSON topic is a codec choice."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SchemaFramed

[`SchemaFramed`](api:ruststream_rdkafka::schema_registry::SchemaFramed) wraps a codec such as `JsonCodec` in the Confluent [envelope](./envelope.md), so a registry-backed JSON topic is a codec choice.

A JSON document reads without its schema, so the envelope separates from the format: the inner codec writes and reads the document, and `SchemaFramed` puts the envelope on and takes it off.
Decoding strips the envelope and looks nothing up.
Encoding frames with the id of the subject that [`register::<T>(subject)`](api:ruststream_rdkafka::schema_registry::SchemaFramed::register) records for `T`, found by the name serde gives the value's type.
The JSON Schema of `T` is captured with it, for [`MissingSubject::AutoRegister`](./missing-subject.md).
The wrapper frames and does not validate: the registry enforces compatibility between versions, and a service that validates each message passes an inner codec that does.

`SchemaFramed::new` takes the [`SchemaPrefetch`](./schema-prefetch.md) that the broker carries, which resolves the registered subjects at connect.
The codec named with [`with_broker_codec`](api:ruststream::runtime::RustStream::with_broker_codec) decodes the deliveries of its scope, and a reply names it at its own position with `.out_reply(policy).codec(codec)`.
An Avro datum cannot be read without its writer schema, so Avro has a codec of its own, [`AvroCodec`](../avro/index.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_registry_framed.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_registry_framed.rs#app"
```

</TabItem>
</Tabs>
