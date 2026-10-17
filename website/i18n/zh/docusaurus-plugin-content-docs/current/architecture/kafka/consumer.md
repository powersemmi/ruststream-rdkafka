---
title: 消费者
sidebar_position: 2
description: "librdkafka 消费者在自己的线程上预先拉取，每次轮询交出一条记录，并按计时器提交已存储的偏移量。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "librdkafka 消费者：拉取线程、本地队列、应用的轮询、偏移量存储和提交计时器",
  "text": "以文字阅读本图",
  "cluster": ["Kafka 集群", "分区 leader、组协调者"],
  "threads": "librdkafka 线程",
  "fetch": ["拉取", "已分配的分区"],
  "queue": ["本地队列", "预先拉取"],
  "commit": ["自动提交", "按计时器，", "每 5 秒"],
  "app": "应用",
  "poll": ["轮询", "下一条记录"],
  "process": ["处理", "这条记录"],
  "store": ["偏移量存储", "在内存中，", "每个分区", "一个位置"],
  "records": "记录",
  "byDefault": "交出时",
  "later": "或稍后",
  "read": "读取",
  "offsetCommit": "提交"
};

# 消费者

librdkafka 消费者在自己的线程上预先拉取，每次轮询交出一条记录，并按计时器提交已存储的偏移量。

<Diagram name="consumer" words={words} />

librdkafka 在应用请求之前，就把已分配分区的记录拉取到本地队列中。`queued.max.messages.kbytes`（64 MiB）限制这个队列的大小。队列满时，对某个分区的拉取暂停 `fetch.queue.backoff.ms`（一秒）。

一次轮询从队列中取出下一条记录。rust-rdkafka 的 [`StreamConsumer`](https://docs.rs/rdkafka/0.39.0/rdkafka/consumer/stream_consumer/struct.StreamConsumer.html) 通过 `recv` 完成这一步，这是一个取消安全的异步调用。

偏移量存储是每个分区在内存中的一个位置。默认情况下（`enable.auto.offset.store`），轮询在应用处理记录之前，就存储它交出的那条记录的偏移量。虚线箭头是另一种方式：应用[关闭自动存储](../../components/settlement/tracked.md)，在记录处理完之后再存储偏移量。

`enable.auto.commit` 开启时，计时器每隔 `auto.commit.interval.ms`（五秒）提交一次已存储的位置，消费者关闭时再提交一次。崩溃会丢失上次提交之后已处理的进度，这部分记录会被再读一遍。在默认的存储方式下，交出时已存储、尚未处理的记录在崩溃后会被跳过。

行为和默认值来自 librdkafka（[偏移量管理](https://github.com/confluentinc/librdkafka/blob/v2.12.1/INTRODUCTION.md#offset-management)、[配置](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md)）。
