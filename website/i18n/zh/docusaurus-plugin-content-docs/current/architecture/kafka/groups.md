---
title: 消费者组
sidebar_position: 1
description: "消费者组把每个分区交给一个成员；成员加入或离开时，分区通过再均衡转移。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一次再均衡：成员加入，协调者把一个分区转给它，它从已提交的偏移量继续读取",
  "text": "以文字阅读本图",
  "firstLane": "成员 1",
  "coordinatorLane": "协调者",
  "secondLane": "成员 2",
  "reading": ["读取", "P0、P1、P2"],
  "joins": ["加入", "组"],
  "rebalance": ["再均衡：", "新的", "分配"],
  "revoked": ["P2 被", "撤销"],
  "keeps": ["读取", "P0、P1"],
  "resumes": ["从已提交的", "偏移量读取", "P2"],
  "session": ["停止心跳超过 session.timeout.ms 的成员", "离开组，它的分区以同样方式转移"],
  "join": "加入",
  "revoke": "撤销",
  "assign": "分配"
};

# 消费者组

消费者组把每个分区交给一个成员；成员加入或离开时，分区通过再均衡转移。

<Diagram name="groups" words={words} />

组协调者是集群中的一个 Broker。成员加入或离开时，它发起再均衡，组就新的分配达成一致。从一个成员拿走的分区在那里被撤销，并分配给另一个成员，后者从组已提交的偏移量继续读取。前一个成员在最后一次提交之后做的工作，会被第二个成员再读一遍。组从未提交过的分区从 [`auto.offset.reset`](../../components/subscriptions/start-offset.md) 指定的位置开始：默认是日志末尾。

停止发送心跳超过 `session.timeout.ms`（librdkafka 中为 45 秒）的成员会被移出组。超过 `max.poll.interval.ms`（5 分钟）没有取新记录的成员也会离开组。

[`partition.assignment.strategy`](../../components/subscriptions/assignment.md) 决定再均衡如何转移分区。急切（eager）策略 `range` 和 `roundrobin`（librdkafka 的默认值是两者都用，`range` 在前）会撤销全部分区再重新分配。`cooperative-sticky` 只转移更换所有者的分区，其余分区在再均衡期间照常流动（[KIP-429](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol)）。两类策略不能在同一个组内混用。

消费者也可以[按名称直接获取分区](../../components/subscriptions/kafka-partitions.md)，而不加入组协议。这样它永远不会再均衡，也没有任何东西会转移它的分区。

默认值来自 librdkafka（[配置](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md)、[消费者组](https://github.com/confluentinc/librdkafka/blob/v2.12.1/INTRODUCTION.md#consumer-groups)）。
