---
sidebar_position: 5
title: Schema checking
description: "schema(namespace) on a descriptor checks at startup that the topic's registry subjects exist and declare records of that namespace."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Schema checking

`schema(namespace)` on a descriptor checks at startup that the topic's registry subjects exist and declare records of that namespace.

A topic opts in one by one; a topic without `schema` is checked against no registry and costs nothing.
The namespace is the one its records carry: `com.acme.orders` for `com.acme.orders.Order`.
The subjects follow `TopicNameStrategy`: `orders-value` for the payload of `orders`, `orders-key` for its key.
[`Schema::namespace`](api:ruststream_rdkafka::schema::Schema::namespace) names other subjects with `.value(..)` and `.key(..)`; a plain string is the namespace with the default subjects.
The check reads the registry of [`KafkaBroker::schema_prefetch`](api:ruststream_rdkafka::KafkaBroker::schema_prefetch), once per topic, while the subscription opens.
A broker with no registry stops the service at startup when a topic opts in.

What stops the service, with the topic, the subject, where its name came from, and the fix in the error:
- the value subject is missing from the registry;
- the newest schema of the value subject declares no record of the namespace;
- a key subject named with `.key(..)` is missing;
- a key subject that exists declares records, none of them of the namespace.

A key subject the topic's name gives may be missing: a topic with plain string keys registers none.
An Avro subject declares its record, or each record of a union; a Protobuf subject declares its top-level messages in its package.
A JSON Schema names no record, so a JSON subject is checked for presence only.

Each form opts in with the arguments it needs:
- [`KafkaTopic::schema(namespace)`](api:ruststream_rdkafka::KafkaTopic::schema) for its one topic;
- [`KafkaTopics::schema(topic, namespace)`](api:ruststream_rdkafka::KafkaTopics::schema) for one topic of the list, addressed by name; a name the list does not hold stops the service and names the list;
- [`KafkaTopicPattern::schema(pattern, namespace)`](api:ruststream_rdkafka::KafkaTopicPattern::schema) for a set of the matched topics.

The sets of a pattern narrow in the order they are written: a topic takes the first set whose pattern it matches.
Each topic of a set has its own default subjects (`orders.eu-value`), and an override names one subject for the whole set.
A matched topic that no set takes is not checked.
The topics the cluster holds when the subscription opens are checked at startup.
A topic created later is checked when the group first assigns it to the member, and a mismatch then is logged.

## Example

`orders` is checked against `orders-value`; of the two listed topics, `returns` names its own value subject.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_subscriptions_schema.rs#schema"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_subscriptions_schema.rs#schema"
```

</TabItem>
</Tabs>
