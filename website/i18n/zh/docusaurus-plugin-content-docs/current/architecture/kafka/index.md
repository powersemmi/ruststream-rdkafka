---
title: 日志
sidebar_position: 0
description: "Kafka 主题是一组分区，每个分区都是按偏移量编号的有序记录日志，每个消费者组都读取它的全部内容。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "生产者向一个主题的各个分区追加记录，两个消费者组各自读取整个主题",
  "text": "以文字阅读本图",
  "producer": ["生产者", "键决定分区"],
  "topic": "主题 orders",
  "p0": "P0",
  "p1": "P1",
  "p2": "P2",
  "billing": "组 billing",
  "billing1": ["成员 1：P0、P1"],
  "billing2": ["成员 2：P2"],
  "audit": "组 audit",
  "audit1": ["成员 1：", "P0、P1、P2"],
  "offsets": ["每个组按分区提交自己的偏移量：", "也就是它继续读取的那条记录"],
  "appends": "追加",
  "reads": "读取每条记录"
};

# 日志

Kafka 主题是一组分区，每个分区都是按偏移量编号的有序记录日志，每个消费者组都读取它的全部内容。

<Diagram name="log" words={words} />

生产者把一条记录追加到主题的一个分区。带键的记录进入其键的哈希所对应的分区，因此同一个键的记录保持顺序。不带键的记录由生产者的分区器决定去向。

每个分区从零开始给记录编号，它的偏移量与其他分区的偏移量各自增长。顺序只在分区内部成立，在其他任何地方都不成立。记录一直留在日志里，直到保留策略删除它，不管有没有人读过。

消费者组把主题的每条记录读一次。组把每个分区交给它的一个成员，于是成员们分担工作。第二个组对每条记录读取属于自己的一份，两个组互不影响。

组读到了哪里，就是每个分区一个数字：它已提交的偏移量，也就是它继续读取的那条记录。Kafka 不保存逐条记录的确认。

这一模型就是 Apache Kafka 自己的模型（[概念与术语](https://kafka.apache.org/documentation/#intro_concepts_and_terms)）。组如何分担分区，见[消费者组](groups.md)；客户端如何读取和提交，见[消费者](consumer.md)。
