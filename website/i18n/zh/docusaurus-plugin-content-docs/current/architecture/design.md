---
title: 设计
sidebar_position: 2
description: "ruststream-rdkafka 的位置：在 RustStream 运行时与 rust-rdkafka 客户端之间，每个订阅一个消费者，共用一个生产者。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下依次是服务、RustStream 运行时、本 crate、rust-rdkafka 客户端和 Kafka 集群",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "订阅者处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": "运行时：分发、工作分区、结算",
  "crate": "ruststream-rdkafka",
  "broker": ["KafkaBroker", "配置"],
  "descriptors": ["KafkaTopic,", "KafkaTopics,", "KafkaPartitions"],
  "subscriber": ["KafkaSubscriber", "产出 KafkaMessage"],
  "publishers": ["KafkaPublisher", "和事务", "发布者"],
  "tracker": "提交跟踪器",
  "eos": "EosPipeline",
  "client": "rust-rdkafka 与 librdkafka",
  "consumer": ["StreamConsumer", "每个订阅一个"],
  "producer": ["FutureProducer", "每个连接一个"],
  "cluster": "Kafka 集群",
  "mounted": "挂载到",
  "polls": "轮询流"
};

# 设计

`ruststream-rdkafka` 所处的位置：在 RustStream 运行时与 rust-rdkafka 客户端之间，每个订阅一个消费者，所有发布者共用一个生产者。

<Diagram name="design" words={words} />

本 crate 在 [rust-rdkafka](https://docs.rs/rdkafka/0.39.0/rdkafka/) 之上实现核心的 Broker trait，rust-rdkafka 封装了 librdkafka。它上面的运行时对每个 Broker 都相同。Kafka 特有的一切都在本 crate 里：[描述符](../components/subscriptions/index.md)、[ack 的含义](../components/settlement/index.md)、[重试副本发往何处](../components/settlement/retries.md)，以及[事务如何提交](../components/transactions/index.md)。

每个订阅打开一个属于自己的 librdkafka 消费者，因此一个订阅就是它所在消费者组的一个成员。同一连接上的[发布者](../components/publishing/index.md)共用一个生产者，第一次发布时打开它。事务发布者在自己的 `transactional.id` 下打开一个属于自己的生产者。

本 crate 没有为之提供类型的设置，就是一个 librdkafka 属性。[`config`](../components/broker/config.md) 系列透传方法可以设置其中每一个，未设置的属性保持 librdkafka 的默认值。

[提交跟踪器](settlement.md)把 ack 变成已存储的偏移量。[`EosPipeline`](api:ruststream_rdkafka::EosPipeline) 在生产者事务中提交已消费的偏移量。各部分如何启动和停止，见[生命周期](lifecycle.md)；一条记录的路径，见[路径](path.md)。
