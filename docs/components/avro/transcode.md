---
sidebar_position: 3
title: Avro transcode
description: "On a broker with schema_registry an Avro datum arrives as JSON, and SchemaFrame writes Avro where the reply's subject holds an Avro schema."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Avro transcode

On a broker with [`schema_registry`](api:ruststream_rdkafka::KafkaBroker::schema_registry) an Avro datum arrives as JSON, and [`SchemaFrame`](api:ruststream_rdkafka::schema_registry::SchemaFrame) writes Avro where the reply's subject holds an Avro schema.

The handler and its models stay on the default JSON codec and carry no Avro type.
On the way in, the subscription reads the [envelope](../schema-registry/envelope.md), resolves the writer schema by its id and rewrites the payload as a JSON document.
On the way out, the layer resolves the subject of the destination topic, `{topic}-value`, and writes the JSON payload as an Avro datum of that subject's schema, inside the envelope.
The reply's model derives `AvroSchema` only to register its subject at startup with [`register_avro`](../schema-registry/registration.md).
The flow is the same as the [JSON transcode](../schema-registry/json-transcode.md); the subject's schema type decides the format on the wire.

The transcode is the compatibility path for a service that keeps plain serde models on a registry-backed topic.
Each message takes a hop through JSON.
A datum is read with its writer schema alone, so a field the writer never wrote has no default to take; the [codec](./index.md) with [`resolve_onto`](./resolve-onto.md) is the path that resolves schemas.
An Avro type that JSON has no form for does not pass through the transcode.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_avro.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_avro.rs#app"
```

</TabItem>
</Tabs>
