---
sidebar_position: 1
title: Server
description: "The server reports bare host:port coordinates and the schema registry, never credentials and never a protocol version."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server reports bare `host:port` coordinates and the schema registry, never credentials and never a protocol version.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
Its `host` is the bootstrap servers, joined with commas.
A `PLAINTEXT://` or `SASL_SSL://` prefix and any user info an address carries stay out of it.
The `protocol` is `kafka`.
`protocolVersion` stays out: a Kafka client and the cluster agree on a version per request type, so no one number describes what clients speak.

A broker with a [`SchemaRegistry`](../schema-registry/index.md) adds the server binding.
`schemaRegistryUrl` is the registry's address without user info, and `schemaRegistryVendor` is `confluent`, the envelope this crate writes.
The [SASL password](../broker/security.md) and the registry's basic-auth credentials stay out of the document.

## Example

The broker reaches two brokers over SASL_SSL and a registry with basic auth.
Its server in the document is `kafka-1.internal:9093,kafka-2.internal:9093` with the registry `https://registry.internal:8081`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_asyncapi.rs#server"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_asyncapi.rs#server"
```

</TabItem>
</Tabs>
