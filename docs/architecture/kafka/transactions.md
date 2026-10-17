---
title: Kafka transactions
sidebar_position: 3
description: "A Kafka transaction makes records written to several partitions, and the consumed offsets, visible together or not at all."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A consume-transform-produce transaction: records and consumed offsets commit together, or an abort discards both",
  "text": "The diagram as text",
  "begin": ["Begin", "on the transactional producer"],
  "produce": ["Produce", "records to any partitions"],
  "offsets": ["Send the consumed offsets", "with the group's metadata"],
  "commit": ["Commit", "the transaction"],
  "visible": ["A read_committed reader", "sees the records, and the", "offsets count as committed"],
  "abort": ["Abort:", "records discarded,", "offsets unmoved,", "the input is", "read again"],
  "fencing": ["One transactional id belongs to one live producer:", "initializing it again fences the earlier one"],
  "markers": "markers written",
  "onError": "on error"
};

# Kafka transactions

A Kafka transaction makes records written to several partitions, and the consumed offsets, visible
together or not at all.

<Diagram name="transactions" words={words} />

A [transactional producer](../../components/transactions/index.md) is named by its `transactional.id`. It runs one transaction at a time.
Records produced inside it land in the log at once, and the commit writes markers that make them
visible.

A reader at the `read_committed` isolation level, librdkafka's default, sees a transaction's
records only after the commit and never after an abort. A reader at `read_uncommitted` sees them as
they are written.

The consumed offsets can join the transaction. Then the input positions move exactly when the
output becomes visible, which is the [consume-transform-produce](../../components/transactions/exactly-once/index.md) shape of
[KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics).
An abort leaves the offsets where they were, so the input is read again into a new transaction.

A transaction open longer than `transaction.timeout.ms` (one minute) is aborted by the
coordinator. A producer started with an id already in use fences the earlier one: that producer's
next call fails.

The defaults and the fencing rule are librdkafka's
([transactional producer](https://github.com/confluentinc/librdkafka/blob/v2.12.1/INTRODUCTION.md#transactional-producer),
[configuration](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md)).
