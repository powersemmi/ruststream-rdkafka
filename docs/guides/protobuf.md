---
sidebar_position: 8
description: "Handle prost-generated Protobuf messages that carry the Confluent envelope: read framed records without a registry and publish framed replies."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Protobuf with the Confluent envelope

This guide handles prost-generated Protobuf messages that carry the Confluent envelope: it reads framed records without a registry and publishes framed replies.

A registry-backed Protobuf record starts with the Confluent envelope: a zero byte, the four-byte schema id, and a message-index path that names which message of the schema follows.
A generated message is not a serde type, so it does not go through a codec.
It encodes and decodes itself, and the broker puts the envelope on and takes it off.

```toml title="Cargo.toml"
[dependencies]
ruststream-rdkafka = { version = "0.7", features = ["protobuf"] }
prost = "0.14"
```

Use the `prost` version the crate depends on.

## Generated types in a handler

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/protobuf.rs#messages"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/protobuf.rs#messages"
```

</TabItem>
</Tabs>

`Parcel` and `Label` are what `prost-build` generates from this schema, plus the derives of RustStream:

```protobuf title="shipping.proto"
syntax = "proto3";
package shipping;

message Parcel {
  int64 id = 1;
  string address = 2;
}

message Label {
  int64 parcel = 1;
  string address = 2;
}
```

In a build script, prost-build's `message_attribute` adds the derives to the generated structs.
`#[derive(Deserialized)]` lets a handler take the message, and `#[derive(Serialized)]` lets it return one.
`#[wire(..)]` names the function that reads or writes the bytes.
In the manual form, the impls take the place of the derives.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/protobuf.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/protobuf.rs#handler"
```

</TabItem>
</Tabs>

`print_label` takes the generated `Parcel` and returns the generated `Label`.
A parcel with no address is dropped, and nothing is published for it.

## Reading framed records

[`decode_confluent`](api:ruststream_rdkafka::protobuf::decode_confluent) is the decode function of `Parcel`.
It steps over the envelope and the message-index path, then `prost` reads the message.
It needs no registry: the generated type already knows its fields.
A record with no envelope is read as a bare message, so a topic where some producers frame and others do not still decodes.

## Publishing framed records

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/protobuf.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/protobuf.rs#service"
```

</TabItem>
</Tabs>

`Label` writes bare Protobuf, and the publish policy puts the envelope in front of it.
[`KafkaPublish::framed`](api:ruststream_rdkafka::KafkaPublish::framed) is that policy, named for the reply with `out_reply`.
On the first publish to a topic, it looks up the topic's subject (`labels-value`) and caches its schema id.
The index path defaults to the schema's first message.
[`message`](api:ruststream_rdkafka::KafkaFramedPublish::message) names another message for a topic, and [`subject`](api:ruststream_rdkafka::KafkaFramedPublish::subject) another subject.

The subject is resolved on the first publish, not at startup, because the topic is only known then.
When the registry does not hold the subject, or holds another format there, the publish fails and the delivery settles as a retry.
With the default commit mode a retry is not redelivered; [Commits and delivery guarantees](commits.md) covers the modes.

[`ProtobufFrame`](api:ruststream_rdkafka::protobuf::ProtobufFrame) frames for a whole app, added with `RustStream::publish_layer`.
It frames what leaves through `Out` slots and publishers the mount sites do not name, when the destination subject holds a Protobuf schema, and passes the rest through as it is.
A reply goes straight to its own publisher and does not pass through the layer, so a replying handler names `KafkaPublish::framed`.
A payload that already carries an envelope is left alone, so the layer and the policy combine.

## Testing in process

Reading needs no registry, so `print_label` reads framed records under `TestApp::start` with no registry and no cluster.
The example's first test publishes a framed parcel with no address and checks that it is dropped and that no label is published.
A framed publish needs the subject's schema id, so the tests of the label run against a registry.
[Testing a Kafka service](testing.md) covers the harness.

## Limits

A Protobuf schema cannot be derived from a generated type.
So the `.proto` source is registered separately, with [`SchemaRegistry::register`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register) and `SchemaType::Protobuf`, or with the registry's own tools.
The schema registered for a framed topic may import only the well-known types (`google/protobuf/*.proto`); other schema references are not resolved.
