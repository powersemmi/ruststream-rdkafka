---
sidebar_position: 8
title: JSON transcode
description: "schema_registry on the broker and the SchemaFrame layer convert framed payloads to and from plain JSON at the broker's edges."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# JSON transcode

[`schema_registry`](api:ruststream_rdkafka::KafkaBroker::schema_registry) on the broker and the [`SchemaFrame`](api:ruststream_rdkafka::schema_registry::SchemaFrame) layer convert framed payloads to and from plain JSON at the broker's edges.

The handler and its models stay on the default JSON codec and never see the envelope.
On the way in, every subscription of the broker reads the envelope of a framed delivery, resolves the schema its id names and hands the handler a plain JSON document.
A payload with no envelope passes through untouched.
A lookup that fails leaves the delivery as it arrived, with a warning, and the handler's decode settles it.
On the way out, the layer, added once with `RustStream::publish_layer`, resolves the subject of each publish's destination topic and frames the payload in that subject's format: a JSON subject keeps the document under the envelope, and an [Avro](../avro/transcode.md) or Protobuf subject is converted to its own format.
The subject is `{topic}-value` unless [`SubjectStrategy`](./subject-strategy.md) or a pin names another.

The layer resolves a subject on its first publish to the topic and remembers the answer.
A topic whose subject the registry does not hold publishes plain JSON, so registry-backed and plain topics share one app.
A registry that cannot be reached, or a payload that does not fit the schema, fails the publish, and the publishing handler asks for a redelivery.
The registry comes to the app as one [`SchemaRegistry`](./index.md) value; the broker and the layer share it, and with it one cache.
A producer owns its subjects, so the example registers the reply's subject at startup with [`register_json`](./registration.md).

The transcode takes a hop through JSON per message.
The codecs ([`SchemaFramed`](./schema-framed.md), [`AvroCodec`](../avro/index.md)) are the other path, and a broker uses one of the two.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_schema_registry.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_schema_registry.rs#app"
```

</TabItem>
</Tabs>
