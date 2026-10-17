---
sidebar_position: 1
title: Local
description: "AvroCodec::local pins one schema and writes a bare datum, with no envelope, no registry and no I/O."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Local

[`AvroCodec::local`](api:ruststream_rdkafka::avro::AvroCodec::local) pins one schema and writes a bare datum, with no [envelope](../schema-registry/envelope.md), no [registry](../schema-registry/index.md) and no I/O.

The schema passed in is the one the wire carries: `T::get_schema()` when the model defines it, or a parsed `.avsc` file when the schema text is the source of truth.
Every message is written and read with that schema, so producers and consumers of the topic agree on it outside the service.
`local` returns an error when the schema refers to a name it does not define.
The codec registers nothing; [`register`](api:ruststream_rdkafka::avro::AvroCodec::register) belongs to the registry form.

[`avro::encode`](api:ruststream_rdkafka::avro::encode) and [`avro::decode`](api:ruststream_rdkafka::avro::decode) are the same conversion as free functions, for a tool that works on datum bytes outside an app.
Both use the type's own schema.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_avro_local.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_avro_local.rs#app"
```

</TabItem>
</Tabs>
