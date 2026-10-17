---
title: 连接生产集群
sidebar_position: 10
description: "把 Kafka 服务连接到生产集群：SASL 与 TLS、生产者持久性、超时、按订阅覆盖的配置，以及对配置的测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 连接生产集群

生产集群要求[认证和 TLS](../components/broker/security.md)，服务则要设置生产者的[持久性](../components/publishing/durability.md)和自己的超时。

## 基于 TLS 的 SASL {/* #sasl-over-tls */}

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

```toml title="Cargo.toml"
[dependencies]
ruststream-rdkafka = { version = "0.7", features = ["ssl"] }
```

部署环境通过环境变量提供地址和凭据。
每个 librdkafka 属性都经由 [`KafkaBroker::config`](../components/broker/config.md) 设置，它把属性应用到各个消费者和生产者上。
`security.protocol` 选择基于 TLS 的 SASL，`sasl.mechanism` 选择 SASL 机制。
TLS 要求 librdkafka 以 OpenSSL 构建：`ssl` feature 链接系统的 OpenSSL，`ssl-vendored` 从源码构建一个。
SCRAM 和 OAUTHBEARER 需要同样的 feature，因为 librdkafka 基于 OpenSSL 实现它们。
基于 `SASL_PLAINTEXT` 的 SASL `PLAIN` 不需要任何 feature。
Kerberos（`GSSAPI`）是 [rust-rdkafka](https://docs.rs/rdkafka) 的 `gssapi` feature：把 `rdkafka` 连同该 feature 加入你的依赖。
使用 TLS 而不使用 SASL 的集群，把 `security.protocol` 设为 `SSL`，由 `ssl.*` 属性指定证书。

## 持久性与超时 {/* #durability-and-timeouts */}

[`producer_config`](api:ruststream_rdkafka::KafkaBroker::producer_config) 只在生产者上设置属性。
`acks=all` 让集群在每个同步副本都拿到记录后才确认它。
`enable.idempotence` 防止生产者重试把一条记录写入两次。
每次发布都等待集群的确认，所以发布成功意味着集群已经接受了记录。

Broker 有自己的超时：

- [`connect_timeout`](../components/broker/startup.md) 限制启动时检查集群是否可达的时间。
- [`flush_timeout`](../components/broker/shutdown.md) 限制关闭时刷出未发送记录的时间。
- [`assignment_timeout`](api:ruststream_rdkafka::KafkaBroker::assignment_timeout) 限制启动时等待组为订阅分配分区的时间。

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

librdkafka 的本地队列已满时，发布会等待队列腾出空间。
[发布策略](../components/publishing/index.md)上的 [`queue_timeout`](api:ruststream_rdkafka::KafkaPublish::queue_timeout) 把更长的等待变成错误。

订阅用 [`KafkaTopic::config`](../components/subscriptions/passthrough.md) 设置自己的属性，它们优先于 Broker 的属性。
有两个属性属于[提交模式](../components/settlement/index.md)：`enable.auto.commit` 和 `enable.auto.offset.store`。
在 [`Commit::Tracked`](../components/settlement/tracked.md) 和 `Commit::Transactional` 下，设置了其中任何一个的订阅都拒绝启动，错误信息会指出该属性和模式。

## 测试配置 {/* #testing-the-configuration */}

测试在 `TestApp::start` 下启动生产环境的 `service`，配上生产环境的 `broker` 和测试凭据。
[进程内 Kafka](../components/testing/index.md) 像 librdkafka 那样校验配置，但不会去连接集群。
librdkafka 拒绝的属性名或属性值会让启动失败，并带上 librdkafka 的错误消息：

```text
Invalid value "SASL_TLS" for configuration property "security.protocol"
```

测试需要与服务相同的 feature。
不带 OpenSSL 构建的 librdkafka 会拒绝把 `SASL_SSL` 和 `SSL` 作为 `security.protocol` 的值。
认证本身只在真实集群上进行。
