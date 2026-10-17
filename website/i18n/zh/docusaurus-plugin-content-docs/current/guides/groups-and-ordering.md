---
title: 主题、消费者组与顺序
sidebar_position: 2
description: "在消费者组中读取主题，用并行的工作分区保持每个键的顺序，并按键、按编号或轮流把记录放到分区上。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 主题、消费者组与顺序

本指南在消费者组中读取主题，用并行的工作分区保持每个键的顺序，并按键、按编号或轮流把记录放到分区上。

示例是一家银行的账簿服务。
它把转账记入账户账簿，筛查欺诈，为自己的记录维护索引，并为有争议的转账立案。

## 每个账户内的顺序 {/* #order-per-account */}

Kafka 在一个分区内保持记录的顺序。
键相同的记录落到同一个分区，所以每个键的顺序得以保持。
`post` 读取以账户为键的转账，最多同时处理八条，而且不打乱这个顺序。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#post"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#post"
```

</TabItem>
</Tabs>

`workers(8, by_key)` 运行八个工作分区；[分区键](../components/subscriptions/lane-key.md)相同的投递在同一个工作分区上依次运行。
[`LaneKey::RecordKey`](api:ruststream_rdkafka::LaneKey::RecordKey) 以记录键作为分区键，所以同一账户的两笔转账绝不会同时运行，即使它们在同一个分区内。
默认的 [`LaneKey::Partition`](api:ruststream_rdkafka::LaneKey::Partition) 以来源分区作为分区键，这也覆盖了没有键的记录。
在手写形式中，同一设置是 [`workers_by_key`](api:ruststream::runtime::SubscriberSettings::workers_by_key)。

记录键以 `kafka-partition-key` 消息头（[`PARTITION_KEY_HEADER`](../components/publishing/keys.md)）到达处理器。
`AccountKey` 是一个消息头契约，只有一个以该名字命名的字段。
`Headers<AccountKey>` 读取投递的键，`with_headers(&key)` 把它写成账簿条目的键。
于是条目在 `bank.entries` 中也落到其账户的分区上，账簿的读者按顺序看到每个账户。
没有键的转账不满足消息头契约，由解码策略结算，默认会丢弃它。

## 消费者组 {/* #consumer-groups */}

一个消费者组对每条记录只读取一次，不论该分区分配给了组里的哪个成员。
每个组都读取自己的一份。
`screen` 在自己的组中读取同样的转账，所以 `post` 看到的每笔转账它都能看到。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#screen"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#screen"
```

</TabItem>
</Tabs>

[`KafkaTopic::group`](../components/subscriptions/kafka-topic.md) 指定组。
没有指定组的订阅加入 Broker 的 [`default_group`](api:ruststream_rdkafka::KafkaBroker::default_group)；`post` 就是这样。
Kafka 只在组内读取主题，所以两者都没有的服务会在启动时失败。

[`StartOffset`](../components/subscriptions/start-offset.md) 决定组在某个分区上没有已提交的偏移量时从哪里开始。
组第一次运行时，以及保留策略删除了它的偏移量之后，都属于这种情况。
`Earliest` 从最旧的记录开始，`Latest` 从日志末尾开始，默认的 `Committed` 把选择交给 librdkafka 的 `auto.offset.reset`，其值为 `latest`。

[`Assignment`](../components/subscriptions/assignment.md) 选择组在成员之间分配分区的方式。
有成员加入或离开时，`CooperativeSticky` 只移动换了主人的分区，其余分区继续运行。

启动会等到组把分区分配给这个成员，最多等待 [`assignment_timeout`](../components/broker/startup.md)（默认 60 秒）。
此后，发布到该主题的每条记录都会到达订阅。
给服务的每个订阅一个独立的组。
同一个组里读取不同主题的两个订阅会互相触发再均衡：第二个加入时，第一个会短暂失去它的分区。
在这段时间里发布的记录，要从 `auto.offset.reset` 让第一个成员重新开始的位置读取，而在 `latest` 下，这个位置已经越过了该记录。

## 多个主题与模式 {/* #several-topics-and-a-pattern */}

[`KafkaTopics`](../components/subscriptions/kafka-topics.md) 通过一个消费者和一个组读取多个主题。
[`KafkaTopics::pattern`](../components/subscriptions/patterns.md) 读取名字匹配某个正则表达式的所有主题，该表达式必须以 `^` 开头。
`archive` 读取银行的所有主题，并记下每条记录所在的位置。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#archive"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#archive"
```

</TabItem>
</Tabs>

处理器从[上下文](../components/context/index.md)读取投递的坐标：[`Topic`](api:ruststream_rdkafka::context::keys::Topic)、[`Partition`](api:ruststream_rdkafka::context::keys::Partition) 和 [`Offset`](api:ruststream_rdkafka::context::keys::Offset)。
所有匹配的主题共用一个载荷类型；`archive` 解码任意 JSON。
启动后创建的主题，会在 librdkafka 下一次刷新元数据时加入模式。

[`KafkaPartitions`](../components/subscriptions/kafka-partitions.md) 读取它指定的分区，不加入任何组。
它从不再均衡，适合每个分区一个进程的部署，或检查单个分区的工具。

## 把记录放到分区上 {/* #placing-records-on-partitions */}

`open_case` 把一笔有争议的转账变成 `bank.cases` 上的一个案件。
该主题有三个分区：0 和 1 平分普通案件，分区 2 存放紧急案件。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#cases"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#cases"
```

</TabItem>
</Tabs>

[`partition(n)`](../components/publishing/partition.md) 把一条记录固定到某个分区。
它是 [`KafkaPublishSteps`](api:ruststream_rdkafka::KafkaPublishSteps) 的一个步骤，所以槽位要以 `Publisher<Options = KafkaOptions>` 为约束才能提供它。
固定的分区优先于记录键，记录键优先于 librdkafka 的分区器。
主题中不存在的分区会让发布失败。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#app"
```

</TabItem>
</Tabs>

[`RoundRobin::partitions(2)`](../components/publishing/round-robin.md) 把每个没有指定分区的案件依次放到分区 0 和 1 中的下一个。
librdkafka 把没有键的记录放到随机分区上，这会让一些消费者比另一些更忙；轮转则把记录均匀地分开。
带键或固定了分区的记录保持原来的位置。
数量是固定的：它等于轮转应使用的分区数，绝不超过主题拥有的分区数。

读取一组主题的订阅没有唯一的主题可以接收重试副本，所以应用用 [`ToSourceTopic`](../components/settlement/to-source-topic.md) 指定一个；[重试与死信主题](retries.md)讲解了这一点。

示例文件中的测试在进程内运行应用。
[进程内 Kafka](../components/testing/index.md) 给每个主题一个分区，所以检查多分区放置的测试要连接集群运行：

```bash
KAFKA_TEST_URL=localhost:9092 cargo test
```
