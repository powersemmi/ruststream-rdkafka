---
title: Protobuf
description: "A prost message rides the byte lanes as itself, and decode_confluent steps over the Confluent envelope with no registry."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Protobuf

A `prost` message rides the byte lanes as itself, and [`decode_confluent`](api:ruststream_rdkafka::protobuf::decode_confluent) steps over the Confluent envelope with no registry.

The `protobuf` feature adds it, with the [`protobuf`](api:ruststream_rdkafka::protobuf) module.
A generated message is not a serde type, so it carries its own bytes: an incoming type derives `Deserialized` and names `#[wire(decode = decode_confluent)]`, an outgoing one derives `Serialized` and names `#[wire(encode = prost::Message::encode)]`.
The handler then takes and returns generated messages, and its signature says nothing about the wire.

A registry-backed producer writes the Confluent envelope in front of the message: a zero byte, the four-byte schema id, and the index path of the message within its schema.
`decode_confluent` parses all three and reads the rest into the reader's own type.
No lookup is needed, because Protobuf fields are addressed by tag: an unknown field is kept as unknown, and a missing one takes its default.
A payload with no envelope is read as a bare message, so a topic where only some producers frame still decodes.
What the reader does not check is which schema version a delivery claims.

Writing the envelope needs the schema id of the destination's subject, which a value cannot look up.
The publish side adds it where the destination is known: [`framed`](./framed.md) at a mount site, or [`ProtobufFrame`](./protobuf-frame.md) for the whole app.
A publish without either sends the bare message.
A subject is registered from `.proto` text through [`SchemaRegistry::register`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register) with `SchemaType::Protobuf`.
The registry paths compile that text with the well-known types available, so a schema imports from them and from nothing else.

## Example

The generated types, with their lanes.

{/* single-form: message types are plain derives, the same in both forms */}
```rust reference="examples/kafka_protobuf_plain.rs#types"
```

The handler reads a framed or a bare order and returns the confirmation, which leaves as a bare message.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_protobuf_plain.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_protobuf_plain.rs#handler"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_protobuf_plain.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_protobuf_plain.rs#app"
```

</TabItem>
</Tabs>
