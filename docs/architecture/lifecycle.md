---
title: Lifecycle
sidebar_position: 3
description: "KafkaBroker::new records configuration, connect probes the cluster, startup opens a consumer per subscription, and shutdown flushes."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's three states as types, and what each transition does on Kafka",
  "text": "The diagram as text",
  "states": "Broker",
  "kafka": "On Kafka",
  "unconnected": ["KafkaBroker::new", "(servers)"],
  "connected": ["ConnectedKafkaBroker"],
  "closed": ["ClosedKafkaBroker", "unflushed_records()"],
  "config": ["Configuration only,", "no I/O"],
  "probe": ["Metadata probe", "within connect_timeout"],
  "subscriptions": ["Each subscription:", "a consumer, joined and", "assigned its partitions"],
  "producer": ["The first publish", "opens the producer"],
  "flush": ["Flush in-flight", "records within", "flush_timeout"],
  "afterShutdown": ["A publisher that outlives the connection", "answers KafkaError::Closed, never a silent success"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# Lifecycle

[`KafkaBroker::new`](../components/broker/index.md) records configuration, connect
probes the cluster, startup opens a consumer per subscription, and shutdown flushes.

<Diagram name="lifecycle" words={words} />

`new` does no I/O, so the service's builder stays synchronous. The runtime connects the broker when
the app starts. Connect fetches the cluster's metadata through a short-lived client built from the
producer settings, within [`connect_timeout`](../components/broker/startup.md)
(30 seconds). An unreachable cluster or a producer property librdkafka refuses fails the start.

Each subscription's [descriptor](../components/subscriptions/index.md) resolves once, against the connected broker. A group subscription
opens its consumer and waits until the group has assigned it partitions and the start of each is
known, up to [`assignment_timeout`](api:ruststream_rdkafka::KafkaBroker::assignment_timeout) (one
minute). Until the runtime starts reading, the consumer keeps answering the group, so a rebalance
does not wait for it. A subscription that names no group, under a broker with no
[`default_group`](api:ruststream_rdkafka::KafkaBroker::default_group), fails the start.

A service that only consumes runs no producer. The first publish opens the shared one.

Shutdown marks the connection closed, then flushes what the producer still holds within
[`flush_timeout`](../components/broker/shutdown.md) (30 seconds).
[`ClosedKafkaBroker`](api:ruststream_rdkafka::ClosedKafkaBroker) reports the records the flush left
behind. A consumer closes when its subscription is dropped: it leaves the group, and unless its
subscription is transactional it commits its stored position on the way.
