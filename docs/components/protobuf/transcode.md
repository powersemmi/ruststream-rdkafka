---
sidebar_position: 3
title: Protobuf transcode
description: "On a broker with schema_registry, framed Protobuf arrives as JSON and leaves framed, with no generated code in the service."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Protobuf transcode

On a broker with [`schema_registry`](api:ruststream_rdkafka::KafkaBroker::schema_registry), framed Protobuf arrives as JSON and leaves framed, with no generated code in the service.

Every subscription of that broker reads a delivery's schema id, fetches the schema and compiles its `.proto` source into a descriptor.
It decodes the datum into a dynamic message and hands the handler JSON, so the handler is a plain serde one on the default codec.
On the way out, the [`SchemaFrame`](api:ruststream_rdkafka::schema_registry::SchemaFrame) publish layer turns the JSON into the subject's Protobuf message and frames it.
Field names in the serde types match the `.proto` messages.
The message written is the schema's first; [`SchemaFrame::message`](api:ruststream_rdkafka::schema_registry::SchemaFrame::message) pins another.

This path is for a service that does not carry generated types.
It costs a JSON hop and a dynamic message per delivery, and nothing decodes while the registry is unreachable.
A service with generated types reads them on the byte lanes with [`decode_confluent`](./index.md) instead.
The two do not mix on one broker: a broker with `schema_registry` transcodes every subscription, so a generated type there would be handed JSON.

## Example

The `.proto` source of the reply's subject and the serde types.

{/* single-form: the schema and the message types are plain declarations, the same in both forms */}
```rust reference="examples/kafka_protobuf_transcode.rs#types"
```

The handler is an ordinary JSON one.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_protobuf_transcode.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_protobuf_transcode.rs#handler"
```

</TabItem>
</Tabs>

The broker transcodes the deliveries, the layer frames the replies, and startup registers the reply's subject.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_protobuf_transcode.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_protobuf_transcode.rs#app"
```

</TabItem>
</Tabs>
