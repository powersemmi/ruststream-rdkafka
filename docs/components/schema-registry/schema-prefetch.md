---
sidebar_position: 2
title: SchemaPrefetch
description: "SchemaPrefetch resolves every registered subject at connect, which keeps the codecs' encode and decode free of I/O."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SchemaPrefetch

[`SchemaPrefetch`](api:ruststream_rdkafka::schema_registry::SchemaPrefetch) resolves every registered subject at connect, which keeps the codecs' encode and decode free of I/O.

A codec is synchronous, and a registry lookup is not.
The prefetch is the async half: every codec built from it, [`AvroCodec::registry`](../avro/index.md) or [`SchemaFramed::new`](./schema-framed.md), records its registered subjects on it.
[`KafkaBroker::schema_prefetch`](api:ruststream_rdkafka::KafkaBroker::schema_prefetch) attaches it, and the broker's connect resolves each subject before the app takes a delivery.
On the consume path the prefetch resolves the writer schema that an arriving envelope names before the codec decodes the delivery, and leaves the payload as it arrived.
A lookup that fails there is logged, and the decode that follows settles the delivery by the subscription's failure policy.

The registry appears twice in an app: once building the prefetch, once attaching it.
A mount site never names it.
Every codec comes from the one prefetch, and what varies per mount is the subject.
A [`Router`](api:ruststream::runtime::Router) inside a broker scope overrides the scope's codec for the handlers it carries, by the usual codec cascade.

[`check_compatibility`](api:ruststream_rdkafka::schema_registry::SchemaPrefetch::check_compatibility) is on by default.
At connect each registered type's schema is checked the way registering it would be, against the versions the subject's compatibility level names: the latest one, or every version under a `_TRANSITIVE` level.
A model that has drifted from its subject stops the app, and the error carries the registry's own account of the difference.
`check_compatibility(false)` turns the check off, for a registry whose level is `NONE`.
A subject the registry does not hold is settled by [`MissingSubject`](./missing-subject.md).

The prefetch and the [transcode](./json-transcode.md) are alternatives on one broker: the prefetch reads the envelope, and the transcode replaces it with JSON.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_registry_prefetch.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_registry_prefetch.rs#app"
```

</TabItem>
</Tabs>
