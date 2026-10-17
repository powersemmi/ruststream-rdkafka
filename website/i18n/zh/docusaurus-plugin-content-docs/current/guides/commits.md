---
title: 提交与投递保证
sidebar_position: 3
description: "选择 ack 和 nack 在 Kafka 上提交什么：librdkafka 自己的提交、Commit::Tracked 下精确的至少一次，以及逐个元素结算的批。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 提交与投递保证

本指南选择 `ack` 和 `nack` 在 Kafka 上提交什么：librdkafka 自己的提交、[`Commit::Tracked`](../components/settlement/tracked.md) 下精确的至少一次，以及逐个元素结算的批。

Kafka 不结算单条消息。
消费者组为每个分区提交一个位置，位置之前的一切都算作已读。
处理器仍然结算每次投递，所以由订阅的 [`Commit`](../components/settlement/index.md) 模式决定这些结算如何变成位置。

| 模式 | `ack` | `drop` | `retry` | 崩溃之后 |
|---|---|---|---|---|
| [`Auto`](../components/settlement/auto.md)（默认） | 什么也不做 | 什么也不做 | 报告不支持，记录不会回来 | 已处理记录的尾部被重新读取，或者已存储但未处理的记录被跳过 |
| [`Tracked`](api:ruststream_rdkafka::Commit::Tracked) | 结算该偏移量 | 结算该偏移量 | 记录会回来 | 每条未结算的记录都被重新读取 |
| [`Transactional`](api:ruststream_rdkafka::Commit::Transactional) | 结算该偏移量，随流水线的事务一起提交 | 与 `ack` 相同 | 让窗口停住，直到事务的截止时间 | 打开的窗口被重新读取 |

示例是一个付款服务：付款不能丢，页面浏览可以丢。
[事务与精确一次](exactly-once.md)讲解第三种模式。

## `Commit::Tracked` 下的至少一次 {/* #at-least-once-with-committracked */}

`pay_out` 通过银行发出一笔付款，并以确认作为回复。
银行没有应答时，付款会回来，再试一次。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/commits.rs#tracked"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/commits.rs#tracked"
```

</TabItem>
</Tabs>

在 [`Commit::Tracked`](api:ruststream_rdkafka::Commit::Tracked) 下，`ack` 把已提交的位置上移到仍未完成的最低投递处。
并发工作者发来的 ack 不按顺序到达，而位置绝不会越过仍在处理中的付款。
[`retry`](api:ruststream::runtime::HandlerOutcome::retry) 把投递交还给它的订阅，订阅会在接下来拉取的任何内容之前再次投递它。
在此之前，它的偏移量一直未完成，所以位置不会越过它。
先停止的进程再次启动时，会从 Kafka 重新拿到这笔付款。

因此付款绝不会丢失，但可能被处理两次：崩溃前一次，崩溃后一次。
银行调用必须能接受重复的付款，比如按付款 id 去重。
没有尝试上限的 `retry` 会无休止地回来；[重试与死信主题](retries.md)加上这个上限。

## librdkafka 自己的提交 {/* #librdkafkas-own-commits */}

`count_view` 在分析存储中统计页面浏览。
这里丢一次浏览无关紧要，所以订阅保留默认模式。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/commits.rs#auto"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/commits.rs#auto"
```

</TabItem>
</Tabs>

在 [`Commit::Auto`](api:ruststream_rdkafka::Commit::Auto) 下，librdkafka 在把记录交给应用时存储位置，并按计时器提交。
`ack` 和 `drop` 不改变任何东西。
`retry` 报告自己不受支持，浏览记录不会回来。
这种模式开销最小，而崩溃可能丢失已处理的记录，或者跳过已存储但未处理的记录。

## 批 {/* #batches */}

`reconcile` 以最多 500 行的[批](../components/subscriptions/batches.md)接收对账单行，并逐行单独结算。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/commits.rs#batch"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/commits.rs#batch"
```

</TabItem>
</Tabs>

一批是一次投递加上 librdkafka 已经拉取到的记录，不会额外等待，所以它往往小于挂载点指定的大小。
返回的列表逐个元素结算这一批；在手写形式中，`Ok(())` 对每个元素 ack，`Err(outcomes)` 逐个结算。
在 `Commit::Tracked` 下，请求重试的元素单独回来，已提交的位置停在它前面，直到它结算。
先停止的进程会重新读取它后面已 ack 的元素：结果是重复，而不是丢失。

librdkafka 在队列中保留多少记录是消费者属性，用 [`KafkaTopic::config`](../components/subscriptions/passthrough.md) 设置。
队列满时，librdkafka 暂停拉取 `fetch.queue.backoff.ms`（默认 1000 毫秒），这会让慢处理器后面的大记录变慢。
`.config("fetch.queue.backoff.ms", "10")` 缩短这次暂停。

## 生产者一侧 {/* #the-producer-side */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/commits.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/commits.rs#app"
```

</TabItem>
</Tabs>

端到端的至少一次也需要生产者一侧配合。
`enable.idempotence` 让 librdkafka 自己重发的记录只落地一次；[`producer_config`](../components/broker/config.md) 设置它以及其他所有生产者属性，包括 [`acks`](../components/publishing/durability.md)。
每次发布都等待集群的投递报告，所以返回 `Ok` 的发布已被 Kafka 接受。

librdkafka 的本地队列已满时，发布会等待空间，这让处理器放慢到集群能承受的速度。
[`queue_timeout`](api:ruststream_rdkafka::KafkaPublish::queue_timeout) 限制这次等待，并把超时变成错误。
关闭时，Broker 会把仍在传输中的记录刷出去，最多等待 [`flush_timeout`](../components/broker/shutdown.md)（默认 30 秒）。
[`ClosedKafkaBroker::unflushed_records`](api:ruststream_rdkafka::ClosedKafkaBroker::unflushed_records) 统计没能发出的记录。

示例文件中的测试在进程内运行应用，并用失败一次的客户端替换银行和分析存储。
[进程内 Kafka](../components/testing/index.md) 像集群一样提交，所以付款在 `Commit::Tracked` 下会回来，页面浏览在 `Commit::Auto` 下不会回来。
