---
description: "SchemaRegistry is the Confluent Schema Registry client: it sends nothing until the first lookup, and its clones share one cache."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Schema Registry

[`SchemaRegistry`](api:ruststream_rdkafka::schema_registry::SchemaRegistry) is the Confluent Schema Registry client: it sends nothing until the first lookup, and its clones share one cache.

[`SchemaRegistry::new`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::new) records the base URL, so the app starts with no registry reachable.
[`basic_auth`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::basic_auth) and [`bearer_token`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::bearer_token) set the credentials of every request, and HTTPS goes through rustls.
[`request_timeout`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::request_timeout) bounds each request, ten seconds by default, so a registry that accepts the connection and goes silent fails the lookup with [`KafkaError::SchemaRegistryTimeout`](api:ruststream_rdkafka::KafkaError::SchemaRegistryTimeout).
The client is configured before it is handed out, because clones keep the settings they were made from.

Every clone reads and writes one cache, so a schema or a subject is fetched once per process.
[`schema_by_id`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::schema_by_id) answers from the cache or asks the registry once; [`cached_schema`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::cached_schema) and [`cached_subject`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::cached_subject) read the cache alone.
A [`RegisteredSchema`](api:ruststream_rdkafka::schema_registry::RegisteredSchema) carries the id, the [`SchemaType`](api:ruststream_rdkafka::schema_registry::SchemaType) (Avro, Protobuf or JSON) and the definition text.

A registry-backed payload carries the Confluent [envelope](./envelope.md): a zero byte, the schema id, then the datum.
A broker meets it in one of two ways.
The codecs, [`AvroCodec`](../avro/index.md) and [`SchemaFramed`](./schema-framed.md), hold the format where a serializer belongs and take their schemas through a [`SchemaPrefetch`](./schema-prefetch.md).
The [transcode](./json-transcode.md) converts at the broker's edges and leaves the handlers on plain JSON.
The example holds the client as application state, and a handler asks it which schema a record was written with.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_registry_client.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_registry_client.rs#app"
```

</TabItem>
</Tabs>
