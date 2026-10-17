---
title: 工作分区
sidebar_position: 5
description: "一个订阅通过一个消费者读取它的全部分区；workers(n, by_key) 把记录分到按分区划分的工作分区上。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一个订阅的分发：三个分区、一个消费者、一个流，以及按分区划分的两个工作分区",
  "text": "以文字阅读本图",
  "p0": "分区 0",
  "p1": "分区 1",
  "p2": "分区 2",
  "consumer": ["订阅的消费者", "组分配给它的全部分区"],
  "stream": ["KafkaSubscriber 流", "一次一条记录"],
  "keyed": ["workers(2, by_key)", "工作分区 = 分区的哈希"],
  "lane0": ["工作分区 1", "分区 0 和 2，", "各自有序"],
  "lane1": ["工作分区 2", "分区 1，有序"],
  "recordKey": ["在 LaneKey::RecordKey 下，工作分区按记录键划分：", "一个分区分散到多个工作分区，每个键内部有序"],
  "fetched": "已拉取的记录",
  "oneAtATime": "一次一条"
};

# 工作分区

一个订阅通过一个消费者读取它的全部分区；`workers(n, by_key)` 把记录分到按分区划分的工作分区上。

<Diagram name="lanes" words={words} />

订阅的流一次产出一条记录，不管它来自哪个分区。不用 `workers` 时，一个循环依次为每条记录运行处理器，因此每个分区都保持顺序，也没有任何东西并行运行。

`workers(n, by_key)` 运行 `n` 个工作分区。记录进入其键的哈希所对应的工作分区，每个工作分区按顺序运行自己的记录。键默认是记录所在的分区（[`LaneKey::Partition`](../components/subscriptions/lane-key.md)），因此一个分区绝不会分散到两个工作分区上，Kafka 的顺序得以保持。工作分区少于分区时，每个工作分区服务多个分区。

[`LaneKey::RecordKey`](api:ruststream_rdkafka::LaneKey::RecordKey) 改为按记录键划分工作分区。同一个键的记录保持顺序，一个分区里的不同键并行运行。没有键的记录也就没有分区键，这样的记录轮流分配到各个工作分区。

并行的工作分区发来的 ack 是乱序到达的。在 [`Commit::Tracked`](../components/settlement/tracked.md) 下，已提交的偏移量仍然绝不会越过尚未结算的记录，[结算](settlement.md)一页用图说明了这一点。

[批处理器](../components/subscriptions/batches.md)接收一条记录，以及 librdkafka 已经拉取的其余记录，最多到挂载处指定的大小，因此一个批往往小于这个大小。
