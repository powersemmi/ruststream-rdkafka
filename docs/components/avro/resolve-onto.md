---
sidebar_position: 2
title: resolve_onto
description: "resolve_onto reads every delivery onto a reader schema, so Avro fills the fields the writer never wrote from their defaults."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# resolve_onto

[`resolve_onto`](api:ruststream_rdkafka::avro::AvroCodec::resolve_onto) reads every delivery onto a reader schema, so Avro fills the fields the writer never wrote from their defaults.

A datum read with its writer schema yields what the writer wrote.
A consumer whose model has moved ahead of its producers names its own schema as the reader schema, and Avro's schema resolution fills each missing field from the default that schema declares.
The model declares a default with `#[avro(default = "..")]`, whose value is JSON.
A field with no default that the writer never wrote fails the decode.

The reader schema applies to decoding only; a publish still writes the schema the wire carries.
On a [local](./local.md) codec the pair is checked when the codec is built, so a reader schema that cannot resolve against the writer schema is an error at startup.
On a [registry](./index.md) codec the writer schemas arrive with the deliveries, so a pair that cannot resolve fails the decode that meets it.
A codec carrying a reader schema reads one type, so a handler that needs one mounts it in a [`Router`](api:ruststream::runtime::Router) with [`with_codec`](api:ruststream::runtime::Router::with_codec), and the scope's codec serves the other handlers.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_avro_resolve.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_avro_resolve.rs#app"
```

</TabItem>
</Tabs>
