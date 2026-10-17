---
title: 生命周期
sidebar_position: 3
description: "KafkaBroker::new 只记录配置，connect 探测集群，启动时为每个订阅打开一个消费者，关闭时刷出数据。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Broker 的三个状态作为类型，以及每次转换在 Kafka 上做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "kafka": "在 Kafka 上",
  "unconnected": ["KafkaBroker::new", "(servers)"],
  "connected": ["ConnectedKafkaBroker"],
  "closed": ["ClosedKafkaBroker", "unflushed_records()"],
  "config": ["只记录配置，", "无 I/O"],
  "probe": ["元数据探测，", "在 connect_timeout 内"],
  "subscriptions": ["每个订阅：", "一个消费者，入组", "并分到它的分区"],
  "producer": ["第一次发布", "打开生产者"],
  "flush": ["在 flush_timeout", "内刷出在途", "的记录"],
  "afterShutdown": ["比连接活得更久的发布者", "返回 KafkaError::Closed，绝不默默成功"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# 生命周期

[`KafkaBroker::new`](../components/broker/index.md) 只记录配置，connect 探测集群，启动时为每个订阅打开一个消费者，关闭时刷出数据。

<Diagram name="lifecycle" words={words} />

`new` 不做任何 I/O，所以服务的构建函数保持同步。运行时在应用启动时连接 Broker。connect 用一个由生产者设置构建的短命客户端获取集群的元数据，时限为 [`connect_timeout`](../components/broker/startup.md)（30 秒）。集群不可达，或者 librdkafka 拒绝某个生产者属性，启动就会失败。

每个订阅的[描述符](../components/subscriptions/index.md)针对已连接的 Broker 解析一次。加入消费者组的订阅打开它的消费者，并等待组给它分配分区、每个分区的起点都已知，最长等待 [`assignment_timeout`](api:ruststream_rdkafka::KafkaBroker::assignment_timeout)（一分钟）。在运行时开始读取之前，消费者一直响应组，因此再均衡不会等它。订阅没有指定组，而 Broker 又没有 [`default_group`](api:ruststream_rdkafka::KafkaBroker::default_group) 时，启动就会失败。

只消费的服务不运行生产者。第一次发布时才打开共用的那个生产者。

关闭先把连接标为已关闭，然后在 [`flush_timeout`](../components/broker/shutdown.md)（30 秒）内刷出生产者仍持有的数据。[`ClosedKafkaBroker`](api:ruststream_rdkafka::ClosedKafkaBroker) 报告刷出后遗留的记录。订阅被丢弃时，它的消费者随之关闭：消费者离开组，并且除非订阅是事务性的，还会在离开时提交已存储的位置。
