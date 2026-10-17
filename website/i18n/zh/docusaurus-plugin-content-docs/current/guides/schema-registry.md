---
title: Schema Registry：Avro 与 JSON Schema
sidebar_position: 7
description: "在 Confluent 信封下读写 Avro 和 JSON Schema 记录，每个 schema 都在启动时与 Schema Registry 核对。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Schema Registry：Avro 与 JSON Schema

本指南在 [Confluent 信封](../components/schema-registry/envelope.md)下读写 Avro 和 JSON Schema 记录，每个 schema 都在启动时与 [Schema Registry](../components/schema-registry/index.md) 核对。

由注册中心支持的记录以 Confluent 信封开头：一个零字节，然后是写入它所用 schema 的四字节 id。
schema 本身保存在注册中心里。
在 RustStream 中，schema 属于编解码器，所以处理器接收和返回的都是普通的 serde 结构体。

```toml title="Cargo.toml"
[dependencies]
ruststream-rdkafka = { version = "0.7", features = ["avro"] }
apache-avro = { version = "0.22", features = ["derive"] }
```

`avro` feature 在 `schema-registry` 之上加入 Avro 编解码器，后者带来注册中心客户端和信封。
`apache-avro` 提供 `AvroSchema` derive；使用本 crate 所依赖的版本。

## 模型 {/* #the-models */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#models"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#models"
```

</TabItem>
</Tabs>

`AvroSchema` 推导出每个结构体的 Avro schema。
`currency` 是在发票 schema 的第二个版本中加入的。
`#[avro(default)]` 是不含该字段的记录读出时所取的值。

## 处理器 {/* #the-handler */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#handler"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#handler"
```

</TabItem>
</Tabs>

`issue_receipt` 读取一张发票，并在 `receipts` 主题上以一张收据作为回复。
其中没有任何代码知道 Avro 或信封的存在。

## 注册中心、预取与编解码器 {/* #the-registry-the-prefetch-and-the-codec */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#service"
```

</TabItem>
</Tabs>

[`SchemaRegistry::new`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::new) 记录注册中心的 URL，此时还不发送任何请求。
它的每个克隆共用一个[缓存](../components/schema-registry/cache.md)，所以每个进程对一个 schema 只获取一次。
[`SchemaPrefetch`](../components/schema-registry/schema-prefetch.md) 负责网络工作，[`schema_prefetch`](api:ruststream_rdkafka::KafkaBroker::schema_prefetch) 把它挂到 Broker 上。
Broker 连接时，预取解析编解码器发布时所用的每个 subject。
记录到达时，它在编解码器运行之前获取信封所指的写入方 schema。
所以编解码器本身从不等待网络。

[`AvroCodec::registry`](../components/avro/index.md) 基于预取构建 Avro 编解码器。
[`register`](api:ruststream_rdkafka::avro::AvroCodec::register) 指定某个类型的值加帧时所用的 subject：收据以 `receipts-value` 发出。
解码不需要注册，因为每条记录都指出了自己的写入方 schema。

`with_broker_codec` 用 Avro 编解码器解码作用域中的每个处理器。
回复用其挂载点上指定的编解码器编码，没有指定时用默认的 JSON 编解码器。
所以 `out_reply(KafkaPublish::default()).codec(avro)` 为收据指定 Avro 编解码器。

## 读取较旧的写入方 schema {/* #reading-an-older-writer-schema */}

[`resolve_onto`](../components/avro/resolve-onto.md) 让 Avro 把每条记录解析到读取方 schema 上，这里是 `Invoice::get_schema()`。
在 schema 第一个版本下写入的记录没有 `currency`。
解析时用该字段的默认值填充它，所以处理器读到的是 `"EUR"`。
读取方 schema 作用于其编解码器解码的每条记录，所以一个带读取方 schema 的编解码器只服务于一个类型。
有自己读取方 schema 的第二个类型，要在自己的路由器中使用自己的编解码器。

## 注册 subject 与缺失的 subject {/* #registering-subjects-and-a-missing-subject */}

服务本身不注册 schema：由拥有主题的一方[注册它的 subject](../components/schema-registry/registration.md)，比如在部署步骤中。
[`register_avro`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register_avro) 把某个类型的 Avro schema 注册到一个 subject 下，[`register_json`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register_json) 注册它的 JSON Schema，[`register`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register) 注册以文本给出的 schema。

当编解码器发布时所用的 subject 不在注册中心里时，Broker 不会连接，错误信息会指出这个 subject。
[`MissingSubject`](../components/schema-registry/missing-subject.md) 选择改为怎样处理：

| 策略 | 行为 |
|---|---|
| `Refuse` *（默认）* | 服务不启动 |
| `AutoRegister` | 把该类型自己的 schema 注册到这个 subject 下，并记录一条警告 |
| `PublishUnframed` | 发布不带信封的裸数据，并记录一条警告 |

在基于预取构建编解码器之前，用 [`on_missing_subject`](api:ruststream_rdkafka::schema_registry::SchemaPrefetch::on_missing_subject) 设置它。

## 启动时的兼容性检查 {/* #compatibility-at-startup */}

连接时，每个已注册类型的 schema 都按注册它时的方式接受检查。
subject 的兼容性级别决定与哪些版本比较：最新版本，或者在 `_TRANSITIVE` 级别下的每个版本。
与 subject 不一致的模型会让服务停止，错误中附有注册中心对差异的说明。
[`check_compatibility(false)`](api:ruststream_rdkafka::schema_registry::SchemaPrefetch::check_compatibility) 关闭这项检查，用于级别为 `NONE` 的 subject。

## 信封下的 JSON Schema {/* #json-schema-under-the-envelope */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#dispute"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#dispute"
```

</TabItem>
</Tabs>

[`SchemaFramed`](../components/schema-registry/schema-framed.md) 把核心的 JSON 编解码器包裹在信封中。
解码时跳过信封，读取其中的 JSON 文档。
编码时，用 [`register`](api:ruststream_rdkafka::schema_registry::SchemaFramed::register) 为该类型指定的 subject 的 id 给值加帧，与 Avro 相同。
在上面的服务中，一个路由器把这个编解码器交给 `disputes` 订阅，因为最具体的编解码器优先。
`JsonSchema` 推导出类型的 JSON Schema，由 `register_json` 注册。

## 固定的本地 schema {/* #a-pinned-local-schema */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#archive"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#archive"
```

</TabItem>
</Tabs>

[`AvroCodec::local`](../components/avro/local.md) 持有一个 schema，并对每条记录都使用它。
主题承载裸 Avro 数据：路径上没有信封、没有注册中心，也没有网络。
因此这个归档服务不需要注册中心就能运行和测试。

## 转码路径 {/* #the-transcode-path */}

服务也可以让普通的 serde 模型保持使用默认的 JSON 编解码器，在 Broker 的边界上做转换。
[`KafkaBroker::schema_registry`](../components/schema-registry/json-transcode.md) 在编解码器运行之前，把每次带帧的投递转换成 JSON。
[`SchemaFrame`](api:ruststream_rdkafka::schema_registry::SchemaFrame) 通过 `RustStream::publish_layer` 添加，它按目的 subject 所保存的格式给每个发出的 JSON 载荷加帧：Avro、JSON Schema 或 Protobuf。
注册中心中没有 subject 的主题，按原样发布。
这条路径每条消息要做一次 JSON 转换，并且不会把记录解析到读取方 schema 上。
一个 Broker 只用其中一条路径：挂上 `schema_registry` 之后，交给编解码器的会是 JSON。

## 接入其他注册中心 crate {/* #bringing-another-registry-crate */}

[`SchemaRegistry::with_client`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::with_client) 接收你自己的 [`RegistryClient`](../components/schema-registry/registry-client.md)，比如基于另一个 HTTP 栈或另一个客户端 crate 的实现。
之后，编解码器、预取和转码路径都通过它访问注册中心。

自己完成[全部 schema 工作](../components/schema-registry/third-party.md)的 crate，比如 `schema_registry_converter`，则改为在字节上工作。
处理器通过一个 `#[derive(Deserialized)]` 类型接收记录的字节，并通过一个 `#[derive(Serialized)]` 类型返回编码后的回复。
那个 crate 与 RustStream 之间只传递字节，所以它自己的 `apache-avro` 版本不必与本 crate 一致。
