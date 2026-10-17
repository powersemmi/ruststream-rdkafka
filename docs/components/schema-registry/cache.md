---
sidebar_position: 4
title: Cache
description: "SchemaCachePolicy bounds the id entries and ages the subject entries, since an id never changes and a subject's latest version moves."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Cache

[`SchemaCachePolicy`](api:ruststream_rdkafka::schema_registry::SchemaCachePolicy) bounds the id entries and ages the subject entries, since an id never changes and a subject's latest version moves.

The registry assigns an id per distinct schema, so an entry keyed by id is never stale; it takes a bound and no expiry.
A subject's latest version moves whenever someone registers one, so its entry takes an expiry and no bound.
[`Cached`](api:ruststream_rdkafka::schema_registry::SchemaCachePolicy::Cached) holds up to `capacity` ids, drops the oldest past it, and trusts a subject's version for `subject_ttl`.
The default keeps 1000 ids and never looks a subject up again, as Confluent's own clients do.
[`Disabled`](api:ruststream_rdkafka::schema_registry::SchemaCachePolicy::Disabled) remembers nothing: the transcode works under it, and the synchronous codecs, which read the cache and cannot await a miss, do not.

`subject_ttl` governs the async lookups: [`warm`](./registration.md) and the publish side of the [transcode](./json-transcode.md).
A codec resolves its subject once, at connect, and keeps it.
Ids are numbered per schema context, so one client stays inside one context; an app that reaches across contexts uses a client per context.

[`SchemaRegistry::cache_policy`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::cache_policy) sets the policy of the default cache, [`MemorySchemaCache`](api:ruststream_rdkafka::schema_registry::MemorySchemaCache).
[`with_cache`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::with_cache) puts a [`SchemaCache`](api:ruststream_rdkafka::schema_registry::SchemaCache) of your own in its place.
That trait is synchronous, because a codec reads it while it encodes or decodes; fetching belongs to the [`RegistryClient`](./registry-client.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_registry_cache.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_registry_cache.rs#app"
```

</TabItem>
</Tabs>
