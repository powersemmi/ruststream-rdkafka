---
title: 重放与 seek
sidebar_position: 5
description: "从头或从某个时刻重新读取主题，并在处理器内部移动正在运行的订阅。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重放与 seek

本指南从头或从某个时刻重新读取主题，并在处理器内部移动正在运行的订阅。

消费者读完日志之后，Kafka 仍然保留它。
服务可以再读一遍：重建读模型，为新的消费者回填数据，或者跳过它无法使用的记录。
订阅在挂载点上取得[起始位置](../components/positions/start-at.md)，处理器则通过投递的[上下文](../components/context/index.md)移动自己的订阅。

## 读模型 {/* #the-read-models */}

服务在内存中维护两个读模型：每个账户的余额和每张卡的消费额。
每条记账都带有账簿的修订号。
对过去记账的更正作为新的修订发布，读模型遇到新修订时，会从日志开头重建自己。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#projection"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#projection"
```

</TabItem>
</Tabs>

## 从头开始 {/* #start-from-the-beginning */}

余额保存在内存中，所以重启之后要从整个日志重新构建。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#balance"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#balance"
```

</TabItem>
</Tabs>

`start_at(KafkaPosition::earliest())` 在每次启动时都从日志开头打开订阅，不管组 `balances` 之前提交过什么。
描述符上的 [`StartOffset`](../components/subscriptions/start-offset.md) 与此不同：它只在组没有已提交的偏移量时生效。
这个位置落在组分配给该消费者的分区上，位于它们的第一条记录之前。

[`KafkaPosition`](../components/positions/index.md) 有五种位置：

| 位置 | 订阅从哪里继续 |
|---|---|
| [`earliest()`](api:ruststream_rdkafka::KafkaPosition::earliest) | Kafka 保留的最旧记录 |
| [`latest()`](api:ruststream_rdkafka::KafkaPosition::latest) | 日志末尾，因此只读取新记录 |
| [`offset(partition, n)`](api:ruststream_rdkafka::KafkaPosition::offset) | 某个分区的偏移量 `n` |
| [`topic_offset(topic, partition, n)`](api:ruststream_rdkafka::KafkaPosition::topic_offset) | 某个主题某个分区的偏移量 `n`，用于读取多个主题的订阅 |
| [`timestamp(ms)`](api:ruststream_rdkafka::KafkaPosition::timestamp) | 每个分区中时间戳不早于 `ms` 的第一条记录 |

起始位置如果没有指定订阅的任何一个分区，启动就会失败。
在 [`Assignment::CooperativeSticky`](../components/subscriptions/assignment.md) 下，第一次分配可能只是组份额的一部分，之后增量到达的分区不会被移动。

## 在处理器中 seek {/* #seek-from-a-handler */}

同一个处理器在收到更新修订的记账时重建余额。
[`Ctx<SeekHandle>`](../components/positions/seek.md) 把处理器所在订阅的 [`KafkaSeeker`](api:ruststream_rdkafka::KafkaSeeker) 交给它。
[`seek`](api:ruststream::Seeker::seek) 把订阅移到日志开头，处理器再读一遍每条记账。
触发重建的那条记账也会回来，这一次投影会应用它。

seek 可以使用上面任何一种位置。
`Ctx<Position>` 保存投递自身的位置：seek 到那里，会重新读取这条记录及其后的一切。
`Ctx<Partition>` 和 `Ctx<Offset>` 为 `offset(..)` 位置提供坐标。

## 在批处理器中 seek {/* #seek-from-a-batch-handler */}

卡的记账数量很大，所以消费额以最多 500 条的[批](../components/subscriptions/batches.md)构建。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#batch"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#batch"
```

</TabItem>
</Tabs>

批处理器声明 [`KafkaBatchContext`](../components/context/batch-context.md)，而不是单次投递的上下文。
它带有同样的 `SeekHandle` 键，但没有位置，因为一批跨越许多记录。
整批结算之后，订阅才会移动。

## 从某个时刻开始 {/* #start-from-a-moment */}

付款审查在每次启动时读取最近七天的付款。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#payouts"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#payouts"
```

</TabItem>
</Tabs>

这个时刻要到服务启动时才知道，所以由挂载点而不是属性指定位置。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#service"
```

</TabItem>
</Tabs>

`KafkaPosition::timestamp` 接收自纪元起的毫秒数，这正是 Kafka 给记录打时间戳所用的单位。
Kafka 在每个分区中把这个时刻解析为时间戳不早于它的第一条记录。
没有这种记录的分区从末尾继续。

## seek 对组的影响 {/* #what-a-seek-does-to-the-group */}

seek 移动的是这个消费者，而不是整个组。
它移动该成员持有的分区，不替任何人提交任何东西。
组的其他成员继续在原处读取。
下一次再均衡会抛弃这次 seek：届时分到某个分区的成员，从组已提交的偏移量继续。
`start_at` 位置在每次启动时都会重新生效。

## `Commit::Tracked` 下的 seek {/* #seeking-under-committracked */}

三个订阅都以 [`Commit::Tracked`](../components/settlement/tracked.md) 提交（[提交与投递保证](commits.md)）。
seek 会清除它移动的每个分区的跟踪位置。
在 seek 之前拉取的投递，结算后不产生任何效果，所以触发重建的那条记账的 ack 不提交任何东西。
因此，之后的提交绝不会越过一条被 seek 重放、但还没有处理器处理过的记录。
