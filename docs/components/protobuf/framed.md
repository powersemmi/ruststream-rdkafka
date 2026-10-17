---
sidebar_position: 1
title: framed
description: "KafkaPublish::framed puts the Confluent envelope on every message its policy publishes, with the destination subject's schema id."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# framed

[`KafkaPublish::framed`](api:ruststream_rdkafka::KafkaPublish::framed) puts the Confluent envelope on every message its policy publishes, with the destination subject's schema id.

It returns a [`KafkaFramedPublish`](api:ruststream_rdkafka::protobuf::KafkaFramedPublish), a policy like any other, so it goes where a policy goes: `.out_reply(..)` or an `Out` slot.
Its live form is a [`KafkaFramedPublisher`](api:ruststream_rdkafka::protobuf::KafkaFramedPublisher).
The subject comes from the destination topic, `{topic}-value` by default; [`subject_strategy`](api:ruststream_rdkafka::protobuf::KafkaFramedPublish::subject_strategy) and [`subject`](api:ruststream_rdkafka::protobuf::KafkaFramedPublish::subject) choose another.
The index path names the schema's first message, which Confluent writes as one zero byte.
[`message`](api:ruststream_rdkafka::protobuf::KafkaFramedPublish::message) pins another message of the schema by its full name, for a topic whose type is not declared first.
[`KafkaFramedPublish::over`](api:ruststream_rdkafka::protobuf::KafkaFramedPublish::over) wraps a configured `KafkaPublish` instead of the default one.

The subject is resolved on the first publish to each destination and cached.
That is the first moment the destination is known: it comes from the reply clause or the slot publish, not from the policy.
So a subject the registry does not hold, or one holding another format, fails that publish with the subject and the format, and the handler's failure policy settles the delivery.
A payload that already carries an envelope is left as it is, so the policy and [`ProtobufFrame`](./protobuf-frame.md) together frame a message once.

## Example

The receipt declares no destination, so the mount site's `reply("order-receipts")` names it, and the policy frames it with the id of `order-receipts-value`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_protobuf_framed.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_protobuf_framed.rs#handler"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_protobuf_framed.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_protobuf_framed.rs#app"
```

</TabItem>
</Tabs>
