---
sidebar_position: 6
title: SubjectStrategy
description: "SubjectStrategy names a subject from the topic, the record name or both, and SchemaFrame::subject pins the subject of one topic."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SubjectStrategy

[`SubjectStrategy`](api:ruststream_rdkafka::schema_registry::SubjectStrategy) names a subject from the topic, the record name or both, and [`SchemaFrame::subject`](api:ruststream_rdkafka::schema_registry::SchemaFrame::subject) pins the subject of one topic.

The strategies are Confluent's three.
[`TopicName`](api:ruststream_rdkafka::schema_registry::SubjectStrategy::TopicName), the default, is `{topic}-value`: one value schema per topic.
[`RecordName`](api:ruststream_rdkafka::schema_registry::SubjectStrategy::RecordName) is the record's fully qualified name: one subject per record type, shared by every topic that carries it.
[`TopicRecordName`](api:ruststream_rdkafka::schema_registry::SubjectStrategy::TopicRecordName) is `{topic}-{record}`: several record types on one topic, each with its own subject.
[`subject(topic, record)`](api:ruststream_rdkafka::schema_registry::SubjectStrategy::subject) builds the name.

The [`SchemaFrame`](./json-transcode.md) layer derives a topic's subject with its strategy, `TopicName` unless [`subject_strategy`](api:ruststream_rdkafka::schema_registry::SchemaFrame::subject_strategy) names another.
The publish path knows the topic and not the record name, so a topic under a record strategy gets its subject pinned with `subject(topic, subject)`, which overrides the strategy for that topic.
The example pins the subject that `RecordName` gives the record.
The Protobuf [publish policy](../protobuf/framed.md) and its [layer](../protobuf/protobuf-frame.md) take the same three settings.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_registry_subject.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_registry_subject.rs#app"
```

</TabItem>
</Tabs>
