---
title: 快速开始
sidebar_position: 1
description: "安装 ruststream-rdkafka，启动本地 Kafka，运行第一个订阅者，并在没有集群的情况下测试它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-rdkafka`，启动一个本地 Kafka，并让第一个服务连上它运行。

## 安装 {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-rdkafka = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-rdkafka = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供默认编解码器，`logging` 把服务的日志打印到终端。
`ruststream-rdkafka` 是 Kafka Broker。
它的 `testing` feature 加入测试所用的[进程内 Kafka](../components/testing/index.md)。

这个 Broker crate 有以下 feature：

| Feature | 提供的内容 |
|---|---|
| `json` *（默认）*、`msgpack`、`cbor` | 同名的编解码器 |
| `schema-registry` | Confluent [Schema Registry](../components/schema-registry/index.md) 客户端及其线上格式 |
| `avro` | 基于 Schema Registry 的 [Avro](../components/avro/index.md) |
| `protobuf` | 带 Confluent 信封的 [Protobuf](../components/protobuf/index.md) 消息 |
| `asyncapi` | 生成的 AsyncAPI 文档中的 [Kafka 绑定](../components/asyncapi/index.md) |
| `testing` | `TestApp` 背后的进程内 Kafka |
| `ssl` | 通过系统 OpenSSL 提供 [TLS](../components/broker/security.md) |
| `ssl-vendored` | 通过从源码构建的 OpenSSL 提供 TLS |
| `zstd` | `zstd` 压缩编解码器 |

## 系统要求 {/* #system-requirements */}

本 crate 从源码构建 librdkafka，即 Kafka 的 C 客户端。
构建需要 C 编译器和 `make`；librdkafka 用自己的脚本完成配置。
在 Debian 和 Ubuntu 上，`build-essential` 同时提供这两者。
`ssl` feature 还需要 OpenSSL 头文件（`libssl-dev`），`ssl-vendored` 需要 `perl` 来构建 OpenSSL。
CMake 构建或系统自带的 librdkafka 是 [rust-rdkafka](https://docs.rs/rdkafka) 的 feature：把 `rdkafka` 连同所需的 feature 加入你自己的依赖，构建就会采用它。

## 本地 Kafka {/* #a-local-kafka */}

开发时，一个 KRaft 模式的 Kafka 节点就够了：

```yaml title="compose.yaml"
services:
  kafka:
    image: apache/kafka:4.0.0
    ports:
      - "9092:9092"
```

启动它，并创建服务读取的主题：

```bash
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic orders
```

Broker 监听 `localhost:9092`。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` 从 `orders` 主题读取 JSON 订单。
带金额的订单被 ack，没有金额的订单被丢弃。
[`KafkaBroker::new`](../components/broker/index.md) 接收引导服务器地址，只把它们记录下来。
连接在服务启动时才打开。
Kafka 只在消费者组内读取主题。
[`default_group`](api:ruststream_rdkafka::KafkaBroker::default_group) 是所有没有指定组的订阅所用的组。
`#[ruststream::app]` 把 `app` 变成 `main`；在手写形式中，[`run_main`](api:ruststream::runtime::cli::run_main) 做同样的事。

运行服务：

```bash
cargo run -- run
```

在另一个终端里，用镜像自带的控制台生产者发布一个订单：

```bash
echo '{"id":1,"amount_cents":1250}' | docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders
```

服务记录一条带订单 id 的 `order taken` 日志。
<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。

`cargo generate` 可以生成同类服务的脚手架，其中带有路由器和回复：

```bash
cargo generate --git https://github.com/powersemmi/ruststream-rdkafka templates/kafka-topic --name my-service
```

## 第一个测试 {/* #the-first-test */}

示例文件里还有两个测试。
每个测试都用 `TestApp::start` 启动同一个 `app`，发布一个订单，然后检查 `take_order` 如何结算它。
在测试中，`KafkaBroker` 连接的是进程内 Kafka，所以测试不需要集群：

```bash
cargo test
```

[测试 Kafka 服务](testing.md)逐一讲解这些测试，以及进程内 Kafka 模拟了什么。

## 从 rust-rdkafka 迁移 {/* #coming-from-rust-rdkafka */}

基于 rust-rdkafka 的服务要自己编写消费者循环和生产者。
下表列出每一部分在 RustStream 中的位置：

| rust-rdkafka | RustStream |
|---|---|
| `ClientConfig::new().set("group.id", ..)` | [`default_group`](api:ruststream_rdkafka::KafkaBroker::default_group)，或按订阅设置的 [`KafkaTopic::group`](../components/subscriptions/kafka-topic.md) |
| 用 `ClientConfig::set` 设置其他任何属性 | [`KafkaBroker::config`](../components/broker/config.md)、[`producer_config`](api:ruststream_rdkafka::KafkaBroker::producer_config)、[`KafkaTopic::config`](api:ruststream_rdkafka::KafkaTopic::config) |
| `StreamConsumer::subscribe` 加一个 `recv` 循环 | 一个 `#[subscriber]` 处理器 |
| 反序列化 `message.payload()` | 处理器的参数类型，由编解码器解码 |
| 处理完成后调用 `store_offset` 或 `commit_message` | [`Commit::Tracked`](../components/settlement/tracked.md) 加上处理器的结果（[提交与投递保证](commits.md)） |
| `FutureProducer::send` | 回复或 `Out` 参数，经由[发布策略](../components/publishing/index.md)发送 |
| `seek` 和 `assign` | 挂载点上的 [`start_at`](../components/positions/start-at.md)，或处理器中的 [seek 句柄](../components/positions/seek.md)（[重放与 seek](replay.md)） |
| `send_offsets_to_transaction` | [`EosPublish`](../components/transactions/exactly-once/index.md)（[事务与精确一次](exactly-once.md)） |
