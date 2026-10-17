---
sidebar_position: 3
title: MissingSubject
description: "MissingSubject decides what a codec does when the registry no longer holds its subject: refuse, register the type's schema, or publish unframed."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MissingSubject

[`MissingSubject`](api:ruststream_rdkafka::schema_registry::MissingSubject) decides what a codec does when the registry no longer holds its subject: refuse, register the type's schema, or publish unframed.

The policy is set on the [prefetch](./schema-prefetch.md) with [`on_missing_subject`](api:ruststream_rdkafka::schema_registry::SchemaPrefetch::on_missing_subject) before codecs are built from it, and it runs when the broker connects.
[`Refuse`](api:ruststream_rdkafka::schema_registry::MissingSubject::Refuse), the default, stops the app with an error that names the subject: a registry is a shared contract, and creating a subject in it belongs to whoever owns the topic.
[`AutoRegister`](api:ruststream_rdkafka::schema_registry::MissingSubject::AutoRegister) registers the schema the type carries under the subject and logs a warning; it suits a producer that owns its subjects.
[`PublishUnframed`](api:ruststream_rdkafka::schema_registry::MissingSubject::PublishUnframed) writes the bare datum and logs a warning, for a topic that must outlive its registry; a registry-backed consumer cannot read those records.

The two deletions of a subject differ.
A soft delete hides the subject while its ids still resolve, so consumers keep reading and only the producer meets the policy.
A permanent delete removes the ids too, and a record that names one of them no longer decodes anywhere.
Registering the schema again afterwards mints a new id.

The [transcode](./json-transcode.md) reads a missing subject as a plain topic and publishes JSON there.
Naming a codec declares its topic registry-backed, so the policy belongs to the codecs.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_registry_missing.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_registry_missing.rs#app"
```

</TabItem>
</Tabs>
