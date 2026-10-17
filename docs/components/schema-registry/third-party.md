---
sidebar_position: 10
title: Third-party clients
description: "Another registry stack does the schema work and the byte lanes carry its framed bytes, so no type crosses between the two."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Third-party clients

Another registry stack does the schema work and the byte lanes carry its framed bytes, so no type crosses between the two.

A service that already uses a registry crate, such as [`schema_registry_converter`](https://crates.io/crates/schema_registry_converter), keeps it.
The handler takes the delivery as a `#[derive(Deserialized)]` view of its bytes and hands them to the converter, which resolves the schema the [envelope](./envelope.md) names through its own client and cache.
The reply is the framed buffer the converter returns, in a `#[derive(Serialized)]` type, and the lane publishes it as it is.
No codec runs on either side.

Such a crate pins its own `apache-avro` and `prost`, and those versions may differ from this crate's.
On this path only bytes pass between the two, so the versions never meet.
The converter is application state, built in `on_startup` and read with [`State`](api:ruststream::runtime::State).
The broker carries neither [`schema_registry`](./json-transcode.md) nor [`schema_prefetch`](./schema-prefetch.md): the schemas are the converter's business.
The same shape fits any client that produces bytes, a binding to a client in another language included.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_third_party_registry.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_third_party_registry.rs#app"
```

</TabItem>
</Tabs>
