---
title: 事务与精确一次
sidebar_position: 6
description: "把多条记录作为一个 Kafka 事务发布，在工作者池下运行事务，并让消费-转换-生产处理器做到精确一次。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 事务与精确一次

本指南把多条记录作为一个 [Kafka 事务](../components/transactions/index.md)发布，在工作者池下运行事务，并让消费-转换-生产处理器做到精确一次。

Kafka 事务让一组记录同时变得可见。
`read_committed` 隔离级别（librdkafka 的默认值）下的读者，在事务提交后看到全部记录，在事务中止后一条也看不到。
在 RustStream 中，事务是挂载点上发布策略的属性，处理器只说明它用到的能力。

## 在一个处理器中全有或全无 {/* #all-or-nothing-from-one-handler */}

`ship` 把一个已付款的订单变成每件商品一行的发货单。
带有空订单行的订单会被丢弃，它的任何一行都不能到达仓库。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/exactly_once.rs#fanout"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/exactly_once.rs#fanout"
```

</TabItem>
</Tabs>

处理器以 `Out<impl TransactionalPublisher, Shipments>` 接收它的槽位：它只说出核心的 [`TransactionalPublisher`](api:ruststream::TransactionalPublisher) 能力，不涉及任何 Kafka 的东西。
在 [`begin_transaction`](api:ruststream::TransactionalPublisher::begin_transaction) 和 [`commit`](api:ruststream::TransactionalPublisher::commit) 之间发布的记录同时变得可见。
[`abort`](api:ruststream::TransactionalPublisher::abort) 丢弃这些记录，所以带空订单行的订单，其第一行永远不会到达读者。
在打开的事务之外，这个槽位像普通槽位一样发布。
已有事务打开时再次调用 `begin_transaction` 会报告 [`KafkaError::TransactionBusy`](api:ruststream_rdkafka::KafkaError::TransactionBusy)，没有打开的事务时调用 `commit` 或 `abort` 会报告 [`KafkaError::NoTransaction`](api:ruststream_rdkafka::KafkaError::NoTransaction)。

事务覆盖发布的订单行，不覆盖消费的订单。
[`Commit::Tracked`](../components/settlement/tracked.md) 在处理器 ack 订单之后提交它的偏移量。
如果在两者之间崩溃，订单会再次投递，它的订单行会被第二次发货。
下面的[精确一次流水线](../components/transactions/exactly-once/index.md)消除了这个缺口。

## 工作者池下的事务 {/* #transactions-under-a-worker-pool */}

一个 Kafka 生产者一次只运行一个事务，所以工作者池中的处理器不能共用一个事务性发布者。
`invoice` 运行四个工作分区，并给每个来源分区一个独立的事务。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/exactly_once.rs#lanes"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/exactly_once.rs#lanes"
```

</TabItem>
</Tabs>

`workers(4, by_key)` 按分区划分工作分区，所以一个分区在一个工作分区上按顺序处理。
槽位是 [`PartitionLanes`](api:ruststream_rdkafka::PartitionLanes)：[`for_partition`](api:ruststream_rdkafka::PartitionLanes::for_partition) 返回投递所在分区的事务性发布者，它在第一次使用时创建。
处理不同分区的工作分区同时运行各自的事务。
事务 id 跟随主题的分区，而不是工作者的数量，所以修改 `workers(n)` 不会改变任何 id。

这个方案需要默认的[分区键](../components/subscriptions/lane-key.md)。
[`LaneKey::RecordKey`](api:ruststream_rdkafka::LaneKey::RecordKey) 会把一个分区分散到多个工作分区上，它们就会共用这个分区的发布者，并在它唯一的事务上冲突。

## 精确一次流水线 {/* #the-exactly-once-pipeline */}

`enrich` 读取一个已下的订单，并以仓库用于规划的汇总作为回复。
每个回复都恰好发布一次，即使服务崩溃、订单被再读一遍也是如此。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/exactly_once.rs#eos"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/exactly_once.rs#eos"
```

</TabItem>
</Tabs>

处理器只返回它的回复，不包含任何事务代码。
[`Commit::Transactional("enrichment-1")`](api:ruststream_rdkafka::Commit::Transactional) 让订阅不再自己提交偏移量。
改由同一 id 的流水线在承载回复的事务中提交它们。
于是已消费的位置和已发布的记录一起移动：要么都提交，要么都不提交。

## 挂载点 {/* #the-mount-site */}

挂载点指定每个槽位和回复背后的发布策略。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/exactly_once.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/exactly_once.rs#service"
```

</TabItem>
</Tabs>

[`transactional_id`](api:ruststream_rdkafka::KafkaPublish::transactional_id) 把普通策略变成事务性策略。
一个事务 id 属于一个存活的生产者：以同一 id 启动的第二个生产者会隔离第一个，第一个从此无法提交。
给服务的每个运行实例一个独立的 id，比如取自 pod 的序号。

[`per_partition`](../components/transactions/per-partition.md) 让这个 id 成为基础名：`invoice` 的发布者使用 `invoicing-1-p0`、`invoicing-1-p1` 等 id。

[`EosPublish::new("enrichment-1")`](api:ruststream_rdkafka::KafkaEosPublish::new) 就是流水线，它的 id 正是订阅的提交模式所指定的那个。
[`EosReplies`](../components/transactions/exactly-once/eos-replies.md) 把每次投递的来源位置带到它的回复上，流水线靠它把两者配对。
没有它，第一个回复会失败，而不是在事务之外发出。

## 提交窗口 {/* #the-commit-window */}

流水线把回复收集到一个窗口中，每隔 [`commit_interval`](api:ruststream_rdkafka::KafkaEosPublish::commit_interval) 提交一次，未设置时为 100 毫秒。
在窗口结束时，它等待向这个窗口发布过的每次投递完成结算，把它们的位置和组的元数据加入事务，然后提交。
因此，回复最早也要在处理器返回它之后一个提交间隔才变得可见。

发布失败、提交失败，或者某次投递没有按时结算，都会中止这个窗口。
随后消费者 seek 回最后一次提交的偏移量，整个窗口在新的事务中再处理一遍。
已提交的输出中，每个回复仍然只出现一次。

流水线限制了它的处理器能返回什么、以及如何读取：

- 处理器返回的 `retry()` 会让它的窗口一直打开，直到事务超时（未用 [`transaction_timeout`](api:ruststream_rdkafka::KafkaEosPublish::transaction_timeout) 另行设置时为 30 秒），然后窗口重放。
- [`retry_after`](../components/settlement/retries.md) 不适用于流水线中的处理器：它延迟的副本在窗口之外发布，破坏了记录与偏移量的配对。
- [`KafkaPartitions`](../components/subscriptions/kafka-partitions.md) 不能与 `Commit::Transactional` 组合，这样的订阅无法启动：流水线通过消费者组提交，而读取固定分区的读者不参与任何组。
