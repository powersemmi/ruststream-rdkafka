---
title: 带 Confluent 信封的 Protobuf
sidebar_position: 8
description: "处理带 Confluent 信封、由 prost 生成的 Protobuf 消息：不用注册中心读取带帧的记录，并发布带帧的回复。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 带 Confluent 信封的 Protobuf

本指南处理带 [Confluent 信封](../components/schema-registry/envelope.md)、由 prost 生成的 Protobuf 消息：不用注册中心读取带帧的记录，并发布带帧的回复。

由注册中心支持的 Protobuf 记录以 Confluent 信封开头：一个零字节、四字节的 schema id，以及一条消息索引路径，指出后面跟着的是 schema 中的哪个消息。
生成的消息不是 serde 类型，所以它不经过编解码器。
它自己完成编码和解码，Broker 负责加上和去掉信封。

```toml title="Cargo.toml"
[dependencies]
ruststream-rdkafka = { version = "0.7", features = ["protobuf"] }
prost = "0.14"
```

使用本 crate 所依赖的 `prost` 版本。

## 处理器中的生成类型 {/* #generated-types-in-a-handler */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/protobuf.rs#messages"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/protobuf.rs#messages"
```

</TabItem>
</Tabs>

`Parcel` 和 `Label` 是 `prost-build` 从下面这个 schema 生成的类型，再加上 RustStream 的 derive：

```protobuf title="shipping.proto"
syntax = "proto3";
package shipping;

message Parcel {
  int64 id = 1;
  string address = 2;
}

message Label {
  int64 parcel = 1;
  string address = 2;
}
```

在构建脚本中，prost-build 的 `message_attribute` 把 derive 加到生成的结构体上。
`#[derive(Deserialized)]` 让处理器可以接收这个消息，`#[derive(Serialized)]` 让处理器可以返回它。
`#[wire(..)]` 指定读写字节的函数。
在手写形式中，由 impl 代替这些 derive。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/protobuf.rs#handler"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/protobuf.rs#handler"
```

</TabItem>
</Tabs>

`print_label` 接收生成的 `Parcel`，返回生成的 `Label`。
没有地址的包裹被丢弃，不为它发布任何东西。

## 读取带帧的记录 {/* #reading-framed-records */}

[`decode_confluent`](../components/protobuf/index.md) 是 `Parcel` 的解码函数。
它跳过信封和消息索引路径，然后由 `prost` 读取消息。
它不需要注册中心：生成的类型已经知道自己的字段。
没有信封的记录按裸消息读取，所以在一些生产者加帧、另一些不加帧的主题上，解码依然成功。

## 发布带帧的记录 {/* #publishing-framed-records */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/protobuf.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/protobuf.rs#service"
```

</TabItem>
</Tabs>

`Label` 写出裸 Protobuf，由发布策略在它前面加上信封。
[`KafkaPublish::framed`](../components/protobuf/framed.md) 就是这个策略，通过 `out_reply` 为回复指定。
第一次向某个主题发布时，它查找该主题的 subject（`labels-value`），并缓存其 schema id。
索引路径默认指向 schema 中的第一个消息。
[`message`](api:ruststream_rdkafka::protobuf::KafkaFramedPublish::message) 为某个主题指定另一个消息，[`subject`](api:ruststream_rdkafka::protobuf::KafkaFramedPublish::subject) 指定另一个 subject。

subject 在第一次发布时解析，而不是在启动时，因为直到那时才知道主题。
注册中心中没有这个 subject，或者那里保存的是另一种格式时，发布失败，投递以重试结算。
在[默认提交模式](../components/settlement/auto.md)下，重试不会重新投递；[提交与投递保证](commits.md)讲解了各种模式。

[`ProtobufFrame`](../components/protobuf/protobuf-frame.md) 为整个应用加帧，通过 `RustStream::publish_layer` 添加。
当目的 subject 保存的是 Protobuf schema 时，它为经由 `Out` 槽位以及挂载点未指定的发布者发出的内容加帧，其余内容原样通过。
回复直接交给它自己的发布者，不经过这一层，所以会回复的处理器要指定 `KafkaPublish::framed`。
已经带有信封的载荷保持不变，所以这一层和该策略可以组合使用。

## 在进程内测试 {/* #testing-in-process */}

读取不需要注册中心，所以 `print_label` 可以在 [`TestApp::start`](../components/testing/index.md) 下读取带帧的记录，既不需要注册中心，也不需要集群。
示例的第一个测试发布一个没有地址的带帧包裹，检查它被丢弃，并且没有发布任何标签。
带帧的发布需要 subject 的 schema id，所以标签的测试要连接注册中心运行。
[测试 Kafka 服务](testing.md)讲解了测试工具。

## 限制 {/* #limits */}

Protobuf schema 无法从生成的类型推导出来。
因此 `.proto` 源文件要单独注册，可以用 [`SchemaRegistry::register`](../components/schema-registry/registration.md) 和 `SchemaType::Protobuf`，也可以用注册中心自己的工具。
为带帧主题注册的 schema 只能导入知名类型（`google/protobuf/*.proto`）；其他 schema 引用不会被解析。
