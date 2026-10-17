---
title: Log
sidebar_position: 0
description: "A Kafka topic is a set of partitions, each an ordered log of records numbered by offset, and every consumer group reads all of it."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A producer appends to the partitions of one topic, and two consumer groups each read the whole topic",
  "text": "The diagram as text",
  "producer": ["Producer", "the key picks the partition"],
  "topic": "Topic orders",
  "p0": "P0",
  "p1": "P1",
  "p2": "P2",
  "billing": "Group billing",
  "billing1": ["Member 1: P0, P1"],
  "billing2": ["Member 2: P2"],
  "audit": "Group audit",
  "audit1": ["Member 1:", "P0, P1, P2"],
  "offsets": ["Each group commits its own offset per partition:", "the record it resumes from"],
  "appends": "appends",
  "reads": "reads every record"
};

# Log

A Kafka topic is a set of partitions, each an ordered log of records numbered by offset, and every
consumer group reads all of it.

<Diagram name="log" words={words} />

A producer appends a record to one partition of the topic. A record with a key goes to the
partition its key hashes to, so the records of one key keep their order. A record without a key
goes wherever the producer's partitioner puts it.

Each partition numbers its records from zero, and its offsets grow apart from the other
partitions'. Order holds inside a partition and nowhere else. A record stays in the log until
retention deletes it, whether anyone read it or not.

A consumer group reads every record of the topic once. The group hands each partition to one of
its members, so the members share the work. A second group reads its own copy of every record,
and the two never affect each other.

What a group has read is one number per partition: its committed offset, the record it resumes
from. Kafka stores no per-record acknowledgement.

The model is Apache Kafka's own
([concepts and terms](https://kafka.apache.org/documentation/#intro_concepts_and_terms)). How a
group shares its partitions is on [Groups](groups.md), and how the client reads and commits on
[Consumer](consumer.md).
