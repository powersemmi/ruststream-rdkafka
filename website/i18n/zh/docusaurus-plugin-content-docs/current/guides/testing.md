---
title: 测试 Kafka 服务
sidebar_position: 9
description: "用 TestApp 在进程内测试 Kafka 服务：生产环境的应用、带键的记录、回复、延迟，以及在集群上运行的同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试 Kafka 服务

测试用 [`TestApp`](api:ruststream::testing::TestApp) 运行服务自己的应用，连接的是[进程内 Kafka](../components/testing/index.md) 或者集群。

被测服务负责银行卡扣款。
付款到达 `card-payments`，它的记录键就是要扣款的账户。
处理器在 `card-receipts` 上以一张收据作为回复。
没有键的付款被丢弃。
超出银行即时限额的付款会被暂缓：处理器用 [`retry_after`](../components/settlement/retries.md) 要求一分钟后再投递它。
经过三次投递后，付款进入[死信主题](../components/settlement/dead-letters.md) `card-payments.review`。
应用由 `service(broker)` 构建，所以测试可以选择 Broker，而 `main` 使用生产环境的 Broker。
测试需要在 `[dev-dependencies]` 中启用 `ruststream-rdkafka` 的 `testing` feature。

## 带键的付款 {/* #a-keyed-payment */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) 把 [`KafkaBroker`](../components/broker/index.md) 连接到进程内 Kafka。
测试从不连接引导地址。
测试按生产类型访问 Broker：`tb.broker::<KafkaBroker>()`。
Kafka 的记录键以 [`PARTITION_KEY_HEADER`](../components/publishing/keys.md) 消息头传递，所以测试用 `with_headers` 设置它。
Broker 把这个消息头变成记录的原生键，处理器再用 [`Ctx<Key>`](../components/context/index.md) 把它读回来。
`publish` 在处理器及其回复都完成结算后才返回。
所以接下来的几行断言的是已经完成的状态：订阅者如何结算，以及什么到达了 `card-receipts`。
测试主体把已启动的应用作为参数接收，所以下面的集群测试也运行它。

## 没有键的付款 {/* #a-payment-without-a-key */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#keyless"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#keyless"
```

</TabItem>
</Tabs>

`settled` 检查处理器返回的结果。
`assert_not_called` 检查没有发布任何收据。

## 不必等待的延迟 {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

测试在暂停的时钟上启动，[`advance`](api:ruststream::testing::TestApp::advance) 让时钟前进。
一分钟的 `HOLD` 不花费任何时间。
Kafka 无法扣留记录，所以 `retry_after` 在延迟结束时把记录的副本发布到它的主题。
副本在消息头中带着重试次数，`published` 把它和测试自己的记录一起显示在 `card-payments` 上。
第三次投递是挂载点允许的最后一次，所以这条记录按到达时的样子进入 `card-payments.review`。
每次被暂缓的投递都记录为 `Outcome::Nack`。

## 进程内 Kafka 模拟了什么 {/* #what-the-in-process-kafka-models */}

进程内 Kafka 读取 librdkafka 会读取的配置：Broker 的 [`config`](../components/broker/config.md) 和 `producer_config`、每个描述符的选项，以及 librdkafka 的默认值。
librdkafka 拒绝的属性会以同样的方式让测试启动失败。
它模拟的是 Kafka，而不是一个队列：

- 每个消费者组对一条记录只读取一次，每个组读取自己的一份。
- [提交模式](../components/settlement/index.md)决定提交什么：[`Commit::Tracked`](api:ruststream_rdkafka::Commit::Tracked) 下的 `retry` 会回来，[`Commit::Auto`](api:ruststream_rdkafka::Commit::Auto) 下则报告自己不受支持。
- 记录键决定记录放在哪里，并回到处理器。
- [seek](../components/positions/seek.md) 把订阅移到某个偏移量、日志的任一端，或者某个时间戳。
- `read_committed` 读者在[事务](../components/transactions/index.md)提交时看到它的记录，事务中止后则永远看不到。

在那里，每个主题只有一个分区。
多个分区、协作式再均衡、保留策略以及真实组加入的时序，都需要集群。

## 在集群上运行同一个测试 {/* #the-same-test-on-a-cluster */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live_within`](../components/testing/live.md) 把同一个应用连接到正在运行的 Kafka。
它的截止时间是一次发布等待其引发的反应完成结算的最长时间；真实集群需要比进程内默认值更长的时间。
测试用 `live::url`（示例自带的辅助函数）从 `KAFKA_TEST_URL` 读取地址，未设置时跳过。
它加入自己的消费者组，所以不会读到之前运行留下的任何东西。
它先创建 `card-payments`：订阅在主题已有的分区上打开，之后才出现的主题要从 `auto.offset.reset` 指定的位置读取。
断言与进程内的完全相同。

```bash
cargo test
KAFKA_TEST_URL=localhost:9092 cargo test -- --test-threads=1
```
