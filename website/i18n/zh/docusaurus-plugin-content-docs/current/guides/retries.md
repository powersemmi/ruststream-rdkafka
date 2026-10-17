---
title: 重试与死信主题
sidebar_position: 4
description: "立即或稍后重试一条 Kafka 记录，限定尝试次数，并把一直失败的记录转入死信主题，适用于单个主题或多个主题。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重试与死信主题

本指南立即或稍后重试一条 Kafka 记录，限定尝试次数，并把一直失败的记录转入[死信主题](../components/settlement/dead-letters.md)，适用于单个主题或多个主题。

Kafka 既不扣留记录，也不统计投递次数。
这两件事都由框架来做：挂载点声明上限和死信主题，重试以记录副本的形式传递，消息头里带着重试次数。

示例是一个退款服务，它通过发卡处理商发出退款。

## 订阅者 {/* #the-subscriber */}

处理商有三种拒绝退款的方式，每种需要不同的结果。
`Busy` 表示它的速率限额已经用完：立即重试仍会被拒绝，所以 [`retry_after`](../components/settlement/retries.md) 等待 30 秒。
`Unavailable` 表示它没有应答：`retry` 立即再试一次。
`Declined` 表示卡账户已经关闭，再试一次也是同样的答复，所以这笔退款被丢弃。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

[`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) 结算这条记录，并在延迟过去后把一份副本发布回 `refunds`。
副本带着重试次数消息头（[`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)），其值加一。
延迟的副本是至多一次的：在延迟结束前停止的进程会丢失它。
副本是位于主题末尾的一条新记录，所以排在这笔退款后面的记录不必等它。

在声明了上限时，[`retry`](api:ruststream::runtime::HandlerOutcome::retry) 也会变成一份副本，并立即发布。
没有上限时，它在 [`Commit::Tracked`](../components/settlement/tracked.md) 下把记录交还给订阅，[提交与投递保证](commits.md)展示了这一点。

## 上限与死信主题 {/* #the-cap-and-the-dead-letter-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) 统计一笔退款的每次投递，第一次也算在内。
第五次投递不再重试：[`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) 把它发布到 `refunds.dlq`，载荷和消息头保持到达时的样子。
被丢弃的退款完成结算，不会去任何地方；死信主题只存放用完了尝试次数的退款。
重试主题和死信主题是服务的基础设施：像其他主题一样创建它们，框架只负责向它们发布。

## 读取死信主题 {/* #reading-the-dead-letter-topic */}

死信主题是一个普通主题，`review` 像读取其他主题一样读取它。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

转入死信的记录，其重试次数消息头保存它经历的投递次数，这里是 5。
`review` 用 `ctx.headers()` 读取它，并把这笔退款记入日志，留给人来查看。

## 多个主题上的重试 {/* #retries-on-several-topics */}

记录的副本发布到订阅读取的主题。
[`KafkaTopic`](../components/subscriptions/kafka-topic.md) 读取一个主题，所以不需要额外的设置。
[`KafkaTopics`](../components/subscriptions/kafka-topics.md) 和 [`KafkaPartitions`](../components/subscriptions/kafka-partitions.md) 读取的是一个单次发布到达不了的集合，所以它们的注册要指定副本的去向，否则服务拒绝启动。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#regional"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#regional"
```

</TabItem>
</Tabs>

应用用 `.out_retry(Publish::default()).transform(ToSourceTopic)` 挂载 `process_regional_refund`。
[`ToSourceTopic`](../components/settlement/to-source-topic.md) 把每份副本送回其投递所来自的主题，所以来自 `refunds.us` 的重试退款会回到 `refunds.us`。
它从 [Kafka 上下文](../components/context/index.md)读取主题，所以处理器要接收一个上下文键，比如 `Ctx<Topic>`；在手写形式中，它的上下文类型是 `KafkaContext`。
`.out_retry(Publish::default()).to("refunds.retry")` 则把所有副本发到你自己的一个主题；把这个主题列入订阅，副本才会回来。
同样的规则也适用于从不重试的订阅：每个基于一组主题的注册都要指定重试目的地。

## 测试延迟 {/* #testing-the-delay */}

示例文件中的测试在暂停的时钟上于进程内运行应用，所以 `tb.advance(BACKOFF)` 让 30 秒瞬间过去。
[测试 Kafka 服务](testing.md)讲解了测试工具。
