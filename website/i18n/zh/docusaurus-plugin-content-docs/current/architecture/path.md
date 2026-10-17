---
title: 路径
sidebar_position: 4
description: "一条 Kafka 记录在 RustStream 服务中的路径：拉取队列、投递、处理器、应答和提交。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条需要应答的 Kafka 记录的路径：从 librdkafka 的拉取队列到它已提交的偏移量",
  "text": "以文字阅读本图",
  "queue": ["librdkafka 拉取队列", "预先拉取的记录"],
  "take": ["KafkaMessage", "记录留在拉取缓冲区"],
  "dispatch": ["分发", "一个循环，或每个键一个工作分区"],
  "decode": ["解码", "直接从缓冲区读取"],
  "body": ["订阅者的处理器", "#[subscriber] 下的 async fn"],
  "encode": ["应答", "编码进循环的缓冲区"],
  "publisher": ["KafkaPublisher", "等待投递报告"],
  "settle": ["结算", "由 Commit 模式决定"],
  "commit": ["提交", "已存储的偏移量"]
};

# 路径

一条 Kafka 记录在 RustStream 服务中的路径：拉取队列、投递、处理器、应答和提交。

<Diagram name="path" words={words} />

librdkafka 已经拉取的记录无需等待即可取出。[`KafkaMessage`](api:ruststream_rdkafka::KafkaMessage) 把记录留在 librdkafka 放置它的位置，同时持有对拥有该缓冲区的消费者的一个引用计数。消息体、键和消息头都从拉取缓冲区读取，不做复制，消息头只在有人请求时才转换。只要还有投递持有某个消费者的记录，该消费者就保持打开。

唯一被复制的记录，是等待中的流自己取到的那一条：队列为空时，等待所返回的记录会被复制出来。

处理器的应答编码进分发循环复用的缓冲区。[发布者](../components/publishing/index.md)把这些字节借给 librdkafka，librdkafka 把它们复制进自己的队列，集群确认这条记录之后，发布才返回。应答发布失败时，这次投递以重试的方式结算。

投递在应答之后结算。这对已提交的偏移量有什么影响，取决于订阅的 [`Commit`](../components/settlement/index.md) 模式，[结算](settlement.md)一页用图说明了这一点。
