---
title: 结算
sidebar_position: 6
description: "ack、drop、retry 和 retry_after 在 Kafka 上变成什么；Kafka 为每个分区提交一个偏移量，而不是逐条确认记录。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const watermark = {
  "title": "单个分区上的 Commit::Tracked：已存储的位置停在尚未结算的最低投递之下",
  "text": "以文字阅读本图",
  "partition": "一个分区，偏移量按投递顺序",
  "settled": "已结算",
  "unsettled": "运行中",
  "stored": ["已存储的位置：11", "低于运行中的", "最低投递"],
  "committed": ["已提交的偏移量：12", "组从这条记录", "继续读取"],
  "next": ["12 结算后，", "位置移到 14：", "15 仍在", "运行"],
  "storedAt": "存储于",
  "commits": "自动提交"
};

export const retries = {
  "title": "Kafka 上的一次重试：尝试上限决定把副本发到主题、发到死信主题，还是丢弃",
  "text": "以文字阅读本图",
  "record": ["一条记录", "x-ruststream-retry-count 说明这是第几次尝试"],
  "outcome": ["处理器返回", "retry_after(delay)，或在有声明时返回 retry()"],
  "cap": ["max_attempts：这次尝试是否低于上限？"],
  "copy": ["低于上限：", "原记录", "结算，副本", "发往主题"],
  "deadLetter": ["到达上限：", "副本发往", "死信", "主题"],
  "dropped": ["到达上限，", "没有死信：", "记录结算，", "不发副本"],
  "arrives": ["副本是一条新记录，", "位于其分区的末尾"],
  "crash": ["延迟的副本保存在进程中：", "计时器触发前崩溃就会丢失它"],
  "handled": "已处理",
  "retries": "重试",
  "below": "低于上限",
  "atCap": "到达上限"
};

# 结算

`ack`、`drop`、`retry` 和 [`retry_after`](../components/settlement/retries.md) 在 Kafka 上变成什么。Kafka 为每个分区提交一个偏移量，而不是逐条确认记录。

<Diagram name="watermark" words={watermark} />

Kafka 为每个分区和组保存一个已提交的偏移量。订阅的 [`Commit`](../components/settlement/index.md) 模式决定处理器的结果如何到达那里。

在 [`Commit::Tracked`](../components/settlement/tracked.md) 下，本 crate 关闭 librdkafka 的自动偏移量存储，并记录它投递的每个偏移量。每次结算都把位置存储在仍在运行的最低投递之下，没有运行中的投递时，则存储已投递的最高偏移量。并行工作分区发来的 ack 可以按任意顺序到达，已存储的位置绝不会越过未完成的工作。消费者永远收不到的偏移量，比如事务标记或被压缩掉的记录，不会阻挡它。librdkafka 的计时器照常提交已存储的位置。

| 结果 | [`Commit::Auto`](../components/settlement/auto.md)（默认） | `Commit::Tracked` | `Commit::Transactional` |
|---|---|---|---|
| `ack` | 什么也不做：librdkafka 在交出记录时就已存储偏移量 | 该偏移量结算，已存储的位置上移 | 该偏移量结算；流水线在下一个窗口提交它 |
| `drop` | 什么也不做，与 `ack` 相同 | 该偏移量结算，与 `ack` 相同 | 该偏移量结算，与 `ack` 相同 |
| `retry()` | 被拒绝（`AckError::Unsupported`）并记录日志：没有任何东西能把记录送回来 | 记录在接下来拉取的内容之前再次投递，位置等待它 | 当前打开的窗口无法提交：它中止，其中的记录被重新读取 |

再均衡或 [seek](../components/positions/seek.md) 移动了某个分区时，该分区的跟踪状态会被丢弃。对移动之前读到的投递进行结算不会改变任何东西，因为该分区的下一个读取者无论如何都从已提交的偏移量开始。

在 [`Commit::Transactional`](api:ruststream_rdkafka::Commit::Transactional) 下，消费者自己不提交任何东西。同一 id 的 [`EosPipeline`](api:ruststream_rdkafka::EosPipeline) 每隔 `commit_interval`（100 毫秒）把已结算的位置加入它的生产者事务，于是已消费的偏移量和已发布的记录同时变得可见，[Kafka 事务](kafka/transactions.md)一页用图说明了这一点。

## 重试 {/* #retries */}

<Diagram name="retries" words={retries} />

Kafka 既不扣留记录，也不统计投递次数，所以这两件事都由运行时来做。`retry_after` 结果，以及声明了 [`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) 或[死信主题](../components/settlement/dead-letters.md)的注册上的 `retry()`，在每种提交模式下都走这条路径。尝试次数随副本的 `x-ruststream-retry-count` 消息头传递，第一次投递算作一次。

副本发往描述符报告的地址。[`KafkaTopic`](../components/subscriptions/kafka-topic.md) 订阅报告它自己的主题。[`KafkaTopics`](../components/subscriptions/kafka-topics.md) 或 [`KafkaPartitions`](../components/subscriptions/kafka-partitions.md) 订阅读取的是一个单次发布到达不了的集合，所以它的注册要[指定目的地](../components/settlement/to-source-topic.md)，否则应用拒绝启动。

副本保留记录的键，因此它再次落到该键的分区上，排在此后写入的记录之后。读取该主题的每个消费者组都会收到副本，而不只是发起重试的那个组。
