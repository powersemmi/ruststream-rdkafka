---
title: Groups
sidebar_position: 1
description: "A consumer group hands each partition to one member, and a member joining or leaving moves partitions in a rebalance."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A rebalance: a member joins, the coordinator moves a partition to it, and it resumes from the committed offset",
  "text": "The diagram as text",
  "firstLane": "Member 1",
  "coordinatorLane": "Coordinator",
  "secondLane": "Member 2",
  "reading": ["Reads", "P0, P1, P2"],
  "joins": ["Joins", "the group"],
  "rebalance": ["Rebalance:", "a new", "assignment"],
  "revoked": ["P2 is", "revoked"],
  "keeps": ["Reads", "P0, P1"],
  "resumes": ["Reads P2 from", "the committed", "offset"],
  "session": ["A member that stops heartbeating for session.timeout.ms", "leaves the group, and its partitions move the same way"],
  "join": "join",
  "revoke": "revoke",
  "assign": "assign"
};

# Groups

A consumer group hands each partition to one member, and a member joining or leaving moves
partitions in a rebalance.

<Diagram name="groups" words={words} />

The group coordinator is a broker of the cluster. When a member joins or leaves, it starts a
rebalance and the group agrees on a new assignment. A partition taken from one member is revoked
there and assigned to another, which resumes from the group's committed offset. Work the first
member did after its last commit is read again by the second. A partition the group has never
committed starts where [`auto.offset.reset`](../../components/subscriptions/start-offset.md) says: the end of the log by default.

A member that stops sending heartbeats for `session.timeout.ms` (45 seconds in librdkafka) is
removed from the group. A member that does not take a new record for `max.poll.interval.ms`
(5 minutes) leaves the group too.

[`partition.assignment.strategy`](../../components/subscriptions/assignment.md) decides how a rebalance moves partitions. The eager strategies,
`range` and `roundrobin` (librdkafka's default is both, `range` first), revoke every partition and
assign them anew. `cooperative-sticky` moves only the partitions that change owner, and the rest
keep flowing during the rebalance
([KIP-429](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol)).
The two families cannot mix inside one group.

A consumer can also [take partitions by name](../../components/subscriptions/kafka-partitions.md) instead of joining the group protocol. It then never
rebalances, and nothing moves its partitions.

The defaults are those of librdkafka
([configuration](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md),
[consumer groups](https://github.com/confluentinc/librdkafka/blob/v2.12.1/INTRODUCTION.md#consumer-groups)).
