---
title: Kafka 事务
sidebar_position: 3
description: "Kafka 事务让写入多个分区的记录和已消费的偏移量一起变得可见，或者全都不可见。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "消费-转换-生产事务：记录和已消费的偏移量一起提交，或者中止时一并丢弃",
  "text": "以文字阅读本图",
  "begin": ["开始", "在事务生产者上"],
  "produce": ["生产", "记录到任意分区"],
  "offsets": ["发送已消费的偏移量", "连同组的元数据"],
  "commit": ["提交", "事务"],
  "visible": ["read_committed 读取者", "看到这些记录，", "偏移量算作已提交"],
  "abort": ["中止：", "记录被丢弃，", "偏移量不动，", "输入被", "再读一遍"],
  "fencing": ["一个事务 id 属于一个存活的生产者：", "再次初始化它会隔离先前那个"],
  "markers": "写入标记",
  "onError": "出错时"
};

# Kafka 事务

Kafka 事务让写入多个分区的记录和已消费的偏移量一起变得可见，或者全都不可见。

<Diagram name="transactions" words={words} />

[事务生产者](../../components/transactions/index.md)以它的 `transactional.id` 命名。它一次运行一个事务。事务中生产的记录会立即落入日志，提交时写入的标记使它们变得可见。

处于 `read_committed` 隔离级别（librdkafka 的默认值）的读取者，只在提交之后才看到事务的记录，中止之后则永远看不到。处于 `read_uncommitted` 的读取者在记录写入时就能看到它们。

已消费的偏移量可以加入事务。这样，输入的位置恰好在输出变得可见时移动，这就是 [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics) 的[消费-转换-生产](../../components/transactions/exactly-once/index.md)模式。中止会让偏移量留在原处，于是输入被再读一遍，进入新的事务。

打开时间超过 `transaction.timeout.ms`（一分钟）的事务会被协调者中止。用一个已在使用的 id 启动的生产者会隔离（fence）先前的那个：先前那个生产者的下一次调用会失败。

默认值和隔离规则来自 librdkafka（[事务生产者](https://github.com/confluentinc/librdkafka/blob/v2.12.1/INTRODUCTION.md#transactional-producer)、[配置](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md)）。
