---
description: "AvroCodec::registry reads each delivery with its writer schema and frames each value with the id of its subject."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Avro

[`AvroCodec::registry`](api:ruststream_rdkafka::avro::AvroCodec::registry) reads each delivery with its writer schema and frames each value with the id of its subject.

[`AvroCodec`](api:ruststream_rdkafka::avro::AvroCodec) is a codec, so a handler on it takes and returns ordinary serde structs.
A model derives `AvroSchema` from `apache-avro`, which the crate re-exports as [`avro::AvroSchema`](api:derive@ruststream_rdkafka::avro::AvroSchema).
The codec is built from a [`SchemaPrefetch`](../schema-registry/schema-prefetch.md), and the same prefetch is attached to the broker with [`KafkaBroker::schema_prefetch`](api:ruststream_rdkafka::KafkaBroker::schema_prefetch).
The codec itself is synchronous and reads only the prefetch's cache, so its encode and decode reach no network.

Decoding needs no registration.
The [envelope](../schema-registry/envelope.md) of each delivery names the writer schema, the prefetch resolves it before the decode, and the codec reads the datum with it.
One codec therefore decodes every record type that arrives on its topics.

[`register::<T>(subject)`](api:ruststream_rdkafka::avro::AvroCodec::register) is the publish side: values of `T` are framed with the id of `subject`.
The codec finds the subject by the name serde gives the value's type.
A value of a type that no one registered fails its publish, and the error names the type and lists the registered ones.
Two registered types with one serde name stop the app at startup.

The codec named with [`with_broker_codec`](api:ruststream::runtime::RustStream::with_broker_codec) decodes the deliveries of its scope.
A reply names its codec at its own position, `.out_reply(policy).codec(codec)`; a reply without one is encoded with the default codec.
The codec and the [transcode](./transcode.md) are alternatives on one broker: a broker with `schema_registry` hands every subscription plain JSON.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_avro_codec.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_avro_codec.rs#app"
```

</TabItem>
</Tabs>
