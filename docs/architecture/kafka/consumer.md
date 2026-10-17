---
title: Consumer
sidebar_position: 2
description: "The librdkafka consumer fetches ahead on threads of its own, hands records out one poll at a time, and commits a stored offset on a timer."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The librdkafka consumer: fetch threads, the local queue, the application's poll, the offset store and the commit timer",
  "text": "The diagram as text",
  "cluster": ["Kafka cluster", "partition leaders, group coordinator"],
  "threads": "librdkafka threads",
  "fetch": ["Fetch", "assigned partitions"],
  "queue": ["Local queue", "fetched ahead"],
  "commit": ["Auto-commit", "on a timer,", "every 5 s"],
  "app": "Application",
  "poll": ["Poll", "the next record"],
  "process": ["Process", "the record"],
  "store": ["Offset store", "in memory,", "a position", "per partition"],
  "records": "records",
  "byDefault": "on hand-over",
  "later": "or later",
  "read": "read",
  "offsetCommit": "commit"
};

# Consumer

The librdkafka consumer fetches ahead on threads of its own, hands records out one poll at a time,
and commits a stored offset on a timer.

<Diagram name="consumer" words={words} />

librdkafka fetches the records of the assigned partitions into a local queue before the
application asks for them. `queued.max.messages.kbytes` (64 MiB) bounds that queue. When it is
full, fetching for a partition pauses for `fetch.queue.backoff.ms` (one second).

A poll takes the next record off the queue. rust-rdkafka's
[`StreamConsumer`](https://docs.rs/rdkafka/0.39.0/rdkafka/consumer/stream_consumer/struct.StreamConsumer.html)
does that through `recv`, an async call that is safe to cancel.

The offset store is an in-memory position per partition. By default
(`enable.auto.offset.store`) a poll stores the offset of the record it hands out, before the
application has processed it. The dashed arrow is the other way: the application turns the
automatic store off and stores an offset once the record is processed.

A timer commits the stored positions every `auto.commit.interval.ms` (five seconds) while
`enable.auto.commit` is on, and once more when the consumer closes. A crash loses what was
processed since the last commit, and it is read again. Under the default store, a record stored
on hand-over and not yet processed is skipped after a crash.

The behaviour and the defaults are librdkafka's
([offset management](https://github.com/confluentinc/librdkafka/blob/v2.12.1/INTRODUCTION.md#offset-management),
[configuration](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md)).
