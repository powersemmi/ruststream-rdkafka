---
sidebar_position: 1
title: Registration
description: "register, register_json and register_avro put a schema under a subject; warm resolves an existing subject and registers nothing."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Registration

[`register`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register), [`register_json`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register_json) and [`register_avro`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register_avro) put a schema under a subject; [`warm`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::warm) resolves an existing subject and registers nothing.

Registering is an explicit step of the producer that owns the subject, usually in `on_startup`.
The registry keeps the id of a schema it already holds, so registering the same schema again changes nothing.
A schema that the subject's compatibility level refuses fails the call, and the error carries the registry's account of what differs.
Each call caches the subject, so the [`SchemaFrame`](./json-transcode.md) layer finds it without a request.

`register_json::<T>` registers the JSON Schema that [`JsonSchema`](api:derive@ruststream_rdkafka::schema_registry::JsonSchema) (schemars, re-exported) derives for `T`.
`register_avro::<T>` registers the Avro schema that `AvroSchema` derives, in full: field defaults, aliases and docs stay, because a later reader resolves an older writer's datum with them.
`register` takes a [`SchemaType`](api:ruststream_rdkafka::schema_registry::SchemaType) and the definition text, which is how a Protobuf subject is registered from its `.proto` source.
The registry paths of this crate read Protobuf schemas whose imports come from the well-known types.

`warm` resolves a subject's latest version and caches it, and fails when the registry has no such subject.
It is the startup check of a subject that another team owns.
[`lookup_id`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::lookup_id) returns the id a subject already gave one exact schema, and [`is_compatible`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::is_compatible) asks whether the subject would accept a schema; neither registers anything.

A codec registers in its own sense: `AvroCodec::register` and `SchemaFramed::register` record the subject a type publishes under, and the [`SchemaPrefetch`](./schema-prefetch.md) resolves those subjects when the broker connects.
The registry itself is named twice in such an app, building the prefetch and attaching it, and never at a mount site.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_registry_register.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_registry_register.rs#app"
```

</TabItem>
</Tabs>
