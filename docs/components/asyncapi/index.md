---
title: Kafka bindings
description: "With asyncapi the generated document speaks Kafka: the server, the topic behind each channel, the group of each operation."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Kafka bindings

With the `asyncapi` feature the generated document speaks Kafka: the server, the topic behind each channel, the group of each operation.

The core builds the AsyncAPI document from the app, before anything connects.
This crate adds the specification's `kafka` binding, version `0.5.0`, to the parts it knows about.
A [server](./server.md) reports the cluster's coordinates and its schema registry.
A [channel](./channels.md) reports the topic behind it.
A receive [operation](./operations.md) reports the consumer group it reads in.

Everything comes from the service's own declarations: the broker, the descriptors and the destinations the mount sites resolved.
The document is published and shared, so no binding carries a credential.
The binary `#[ruststream::app]` writes prints the document with `asyncapi gen`.

## Example

The dependency line:

```toml
ruststream-rdkafka = { version = "0.7", features = ["asyncapi"] }
```

The service registers its broker under the label `kafka`, the key of its server in the document.
`cargo run --example kafka_asyncapi --features asyncapi,schema-registry -- asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_asyncapi.rs#app"
```

</TabItem>
</Tabs>
