---
sidebar_position: 5
title: RegistryClient
description: "RegistryClient is the transport the registry speaks through; HttpRegistryClient is the default, and with_client supplies another."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RegistryClient

[`RegistryClient`](api:ruststream_rdkafka::schema_registry::RegistryClient) is the transport the registry speaks through; [`HttpRegistryClient`](api:ruststream_rdkafka::schema_registry::HttpRegistryClient) is the default, and [`with_client`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::with_client) supplies another.

[`SchemaRegistry`](./index.md) is a facade over two parts: a `RegistryClient` that asks the registry, and a [`SchemaCache`](./cache.md) that remembers the answers.
The codecs, the prefetch and the transcode are written against the facade, so a client of your own reaches all of them.
It suits a different HTTP stack, a published client crate, a binding to a client in another language, or schemas shipped with the service.

A client answers four questions, each as a boxed future: a schema by id, a subject's latest version, registering a schema, and the id of a schema a subject already holds.
`latest` answers `None` for a subject the registry does not hold, which the transcode reads as a plain topic.
[`is_compatible`](api:ruststream_rdkafka::schema_registry::RegistryClient::is_compatible) has a default that answers `None`, "cannot tell", and the prefetch then skips its compatibility check.
The client is shared as `Arc<dyn RegistryClient>`, so each round trip costs one virtual call and one allocation beside the network, and only a lookup the cache does not answer makes one.
`basic_auth`, `bearer_token` and `request_timeout` configure the shipped HTTP client, and a supplied client carries its own credentials and deadlines.
Errors are [`KafkaError::SchemaRegistry`](api:ruststream_rdkafka::KafkaError::SchemaRegistry), with a message that names what was asked.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_registry_custom_client.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_registry_custom_client.rs#app"
```

</TabItem>
</Tabs>
