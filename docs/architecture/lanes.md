---
title: Lanes
sidebar_position: 5
description: "One subscription reads all its partitions through one consumer; workers(n, by_key) spreads the records over lanes keyed by partition."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One subscription's dispatch: three partitions, one consumer, one stream and two lanes keyed by partition",
  "text": "The diagram as text",
  "p0": "Partition 0",
  "p1": "Partition 1",
  "p2": "Partition 2",
  "consumer": ["The subscription's consumer", "every partition the group assigned it"],
  "stream": ["KafkaSubscriber stream", "one record at a time"],
  "keyed": ["workers(2, by_key)", "lane = hash of the partition"],
  "lane0": ["Lane 1", "partitions 0 and 2,", "each in order"],
  "lane1": ["Lane 2", "partition 1, in order"],
  "recordKey": ["Under LaneKey::RecordKey a lane is keyed by the record key:", "one partition spreads over lanes, each key in order"],
  "fetched": "fetched records",
  "oneAtATime": "one at a time"
};

# Lanes

One subscription reads all its partitions through one consumer; `workers(n, by_key)` spreads the
records over lanes keyed by partition.

<Diagram name="lanes" words={words} />

A subscription's stream yields one record at a time, whichever partition it came from. Without
`workers`, one loop runs the handler for each record in turn, so every partition keeps its order
and nothing runs side by side.

`workers(n, by_key)` runs `n` lanes. A record goes to the lane its key hashes to, and a lane runs
its records in order. The key is the record's partition by default
([`LaneKey::Partition`](api:ruststream_rdkafka::LaneKey::Partition)), so a partition never spreads
over two lanes and Kafka's order is kept. Lanes fewer than partitions serve several partitions
each.

[`LaneKey::RecordKey`](api:ruststream_rdkafka::LaneKey::RecordKey) keys a lane by the record key
instead. Records of one key keep their order, and one partition's keys run side by side. A record
without a key has no lane key, and such records rotate over the lanes.

Acks from parallel lanes arrive out of order. Under `Commit::Tracked` the committed offset still
never passes a record that has not settled, as [Settlement](settlement.md) draws.

A batch handler takes one record and whatever librdkafka has already fetched, up to the size the
mount site names, so a batch is often smaller than that size.
