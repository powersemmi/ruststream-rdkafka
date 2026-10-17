---
sidebar_position: 2
title: ProtobufFrame
description: "ProtobufFrame, added with publish_layer, frames every Protobuf publish of the app that leaves through a slot."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ProtobufFrame

[`ProtobufFrame`](api:ruststream_rdkafka::protobuf::ProtobufFrame), added with `publish_layer`, frames every Protobuf publish of the app that leaves through a slot.

[`ProtobufFrame::new`](api:ruststream_rdkafka::protobuf::ProtobufFrame::new) takes the [registry](../schema-registry/index.md), and the app adds the layer once, so no mount site names the framing.
A publish is framed when its destination's subject holds a Protobuf schema.
Every other publish passes as it is: a topic with no subject, and a subject holding Avro or JSON.
So one app carries Protobuf, Avro and JSON topics with the layer installed.
A payload that already carries an [envelope](../schema-registry/envelope.md) also passes, so a message is never framed twice.
[`subject_strategy`](../schema-registry/subject-strategy.md), [`subject`](api:ruststream_rdkafka::protobuf::ProtobufFrame::subject) and [`message`](api:ruststream_rdkafka::protobuf::ProtobufFrame::message) choose the subject and the message as they do on [`framed`](./framed.md).

The layer sees what leaves through an `Out` slot and through a publisher the runtime pairs.
A reply returned from a `reply(..)` handler goes straight to its paired publisher, so it is framed by naming [`framed`](./framed.md) at its mount site.
The layer is an alternative to the transcode's [`SchemaFrame`](./transcode.md): this one expects the payload to be the Protobuf datum already, that one expects JSON.

## Example

Every order writes an audit record through a slot, and the layer frames it with the id of `order-audit-value`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_protobuf_frame.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_protobuf_frame.rs#handler"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_protobuf_frame.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_protobuf_frame.rs#app"
```

</TabItem>
</Tabs>
