---
title: Path
sidebar_position: 4
description: "The way of one Kafka record through a RustStream service: the fetch queue, the delivery, the handler, the reply and the commit."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of one Kafka record that is answered, from librdkafka's fetch queue to its committed offset",
  "text": "The diagram as text",
  "queue": ["librdkafka fetch queue", "records fetched ahead"],
  "take": ["KafkaMessage", "the record stays in the fetch buffer"],
  "dispatch": ["Dispatch", "one loop, or a lane per key"],
  "decode": ["Decode", "read in place from the buffer"],
  "body": ["Handler", "async fn under #[subscriber]"],
  "encode": ["Reply", "encoded into the loop's buffer"],
  "publisher": ["KafkaPublisher", "awaits the delivery report"],
  "settle": ["Settlement", "the Commit mode decides"],
  "commit": ["Commit", "of the stored offset"]
};

# Path

The way of one Kafka record through a RustStream service: the fetch queue, the delivery, the
handler, the reply and the commit.

<Diagram name="path" words={words} />

A record librdkafka has already fetched is taken without waiting. The
[`KafkaMessage`](api:ruststream_rdkafka::KafkaMessage) holds it where librdkafka put it, together
with a reference count on the consumer that owns that buffer. The body, the key and the headers are
read from the fetch buffer and not copied, and the headers are converted only when something asks
for them. The consumer stays open for as long as a delivery holds a record of it.

The one record that is copied is the one a waiting stream resolves itself: when the queue was
empty, the record the wait returns is copied out.

The handler's reply is encoded into a buffer the dispatch loop reuses. The publisher lends those
bytes to librdkafka, which copies them into its own queue, and the publish returns once the cluster
has acknowledged the record. A reply that fails to publish settles the delivery with a retry.

The delivery settles after the reply. What that does to the committed offset depends on the
subscription's [`Commit`](api:ruststream_rdkafka::Commit) mode, which [Settlement](settlement.md)
draws.
