# Kafka Broker

`ruststream-rdkafka` 通过 [rdkafka](https://docs.rs/rdkafka) / librdkafka，把一个
[RustStream](https://github.com/powersemmi/ruststream) 服务跑在 Apache Kafka 上。

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-rdkafka = "0.7"
serde = { version = "1", features = ["derive"] }
```

一个最小的服务就是一个处理器加一个应用函数：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_quickstart.rs:handler"
```

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_quickstart.rs:app"
```

## 传输模型 { #the-transport-model }

- 一次订阅就是一个消费者读一个主题。[`KafkaTopic`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing) 描述它，消费者组也在其中；
  光写字符串的 `#[subscriber("orders")]` 形态，从 Broker 的 `default_group` 取消费者组。
- 出站消息声明的名字就是目的主题。声明了 `#[outgoing(name = "confirmations")]` 的回复类型发往
  `confirmations`，订阅者只写不带名字的 `publish`。没有声明名字的回复类型，发往挂载点指定的主题，
  也就是 `publish("enriched-orders")`。分区键消息头成为记录的原生 key，因此按键的顺序由 Kafka
  自己保证（参见[发布](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#publishing)）。
- 结算跟随 Kafka 已提交的位置，而不是逐条消息的一个帧。默认的 `Commit::Auto` 把这个位置交给
  librdkafka 的自动提交；`Commit::Tracked` 让每次 `ack` 都成为一次精确的单条确认，落在一条连续的
  水位之上。参见[主题与消费者组](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing)。
- 配置一律委托给 librdkafka。你不设的选项保持 librdkafka 的默认值；Broker 和描述符上的
  `config(key, value)`、生产者的 `producer_config(key, value)`，能触及本 crate 没有做成类型化选项
  的每一个属性。

## Broker 的生命周期 { #the-lifecycle-ladder }

连接的每个状态都是独立的类型，因此顺序错了的调用不会通过编译：

```text
KafkaBroker::new(servers)          只记配置，同步，无 I/O
  |
  | .connect().await?              探测集群
  v
ConnectedKafkaBroker               订阅和活的发布者都挂在它上面
  |
  | .shutdown().await?             把在途的发布刷出去
  v
ClosedKafkaBroker                  终结见证：unflushed_records()
```

`KafkaBroker::new` 只记下配置，因此服务可以用同步的 `#[ruststream::app]` 构建器组装。运行时在启动
时调用一次 `connect`，在已连接的 Broker 上打开每一条订阅，最后再关掉它。

编译期的保证属于句柄的持有者。与这条连接共享的那些句柄（更早实例化的发布者、仍然打开的订阅者），
在关闭之后返回 `KafkaError::Closed`，而不是对着一条死连接照样成功。

发布者也是同样的拆分。`KafkaPublish` 是**策略**，它构造出**活的** `KafkaPublisher`；
`transactional_id` 把它变成事务策略，`per_partition` 再把事务策略变成按分区的策略，
`KafkaEosPublish` 则是精确一次管线的策略。

策略在注册处理器时指定（回复用 `b.include(handler).out_reply(policy)`，`Out<..>` 槽位用
`.out(marker, policy)`），启动时策略在已连接的 Broker 上实例化发布者。只做回复的处理器什么都不用
指定，发布者由 Broker 的默认策略构造。

## 能力 { #capabilities }

框架的可选能力 trait，以及本 Broker 原生实现了其中哪些：

| 能力 | 原生 | 细节 |
|---|---|---|
| `Subscribe` | 是 | `#[subscriber("orders")]` 只凭主题名订阅，落在 [Broker 的默认消费者组](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing)里。 |
| `BatchSubscriber` | 是 | 整批消费：一次投递加上 librdkafka 已经拉到的一切，不额外等待，且不超过挂载点指定的大小 - [批量消费](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#batches)。 |
| `TransactionalPublisher` | 是 | 在 Kafka 事务里发布，每个句柄同时只有一个打开的事务：[事务](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#transactions)。 |
| `OwnedTransactions` | 否 | 一个 Kafka 生产者同时只持有一个 Broker 端事务，因此事务不能是一个独立持有的值；并发的流程改用[按分区的发布者](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#transactions)或[精确一次管线](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#exactly-once-pipelines)。 |
| `RequestReply` | 否 | Kafka 没有回复关联机制；请求-回复要靠你自己的回复主题加一个关联消息头。 |
| `Partitioned` | 是 | 有序的工作分区，键取自投递的来源分区，或者在 `LaneKey::RecordKey` 下取自记录 key：[按键的工作分区](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing)。 |
| `Seekable` + `Positioned` | 是 | 从处理器里重新定位这个消费者持有的分区，用的是 `SeekHandle` 上下文键，它和投递自身的 `Position` 并列：[重新定位订阅](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#positions-and-seeking)。 |
| `DescribeServer` | 是 | 生成的 AsyncAPI 文档在 `kafka` 协议下列出 bootstrap 服务器，旁边还有 Schema Registry：[AsyncAPI 文档](#the-asyncapi-document)。 |

## AsyncAPI 文档 { #the-asyncapi-document }

`asyncapi gen` 打印一份服务自述的文档，而本 crate 在其中填上 Kafka 自己的词汇 - 规范把它叫做
`kafka` binding。打开它只要一个 feature：

```toml
ruststream-rdkafka = { version = "0.7", features = ["asyncapi"] }
```

于是服务器报告它配置的 Schema Registry，channel 报告它背后的主题，`receive` 操作报告读它的消费者
组：

```json
--8<-- "docs/snippets/asyncapi-bindings.json"
```

客户端标识在描述符的原样透传指定了 `client.id` 时出现。消费者组只在描述符自己指定时出现：文档在任
何连接之前就已构建，只用描述符，而描述符从来看不到那个带着 `default_group` 的 Broker。文档里没有
`protocolVersion`：Kafka 的协议版本由客户端和集群按每个 API key 分别协商，没有哪一个数字能说明它们
讲的是什么。

`KafkaTopics` 订阅报告自己的消费者组，不报告主题：binding 里的 `topic` 指的是一个，而这样的订阅读
的是一组。走 Schema Registry 的发布者再加一条 message binding：schema id 以 Confluent 编码放在载荷
里，subject 由它的命名策略找出。

服务发布过去的 channel 也报告自己的主题。这个主题就是挂载点解析出的目的地：注册上的
`publish("dest")` 从句、回复类型自己的 `#[outgoing(name)]`、槽位条目的名字、声明的死信主题。发布
策略带的是生产者设置，不带目的地，所以它没有自己的主题可报。

Registry 的地址进入文档时会去掉用户信息，理由和 bootstrap 地址一样 - 文档会被发布、被转手，而进入
文档的密码就已经离开了服务。

## 生成服务骨架 { #scaffold-a-service }

```text
cargo generate --git https://github.com/powersemmi/ruststream-rdkafka templates/kafka-topic --name my-service
```

这个模板接好了：一个带默认消费者组的 Kafka Broker、一个在声明的尝试上限与死信主题之下精确提交的订
阅者，以及一条发布出去的回复。它的 `#[ruststream::app]` 入口给二进制程序带来 `run` 和 `asyncapi gen` 两条命令。

## 文档在哪里 { #where-the-documentation-is }

参考资料在 docs.rs 上，一个任务一节。
[订阅](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing) 讲描述符、消费者组、起始偏移量、提交模式、按键的工作分区、批量消费、
重试与重新定位。
[发布](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#publishing) 讲发布策略、记录 key 与显式分区、投递保证、事务与精确一次管线。
[`schema_registry`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/schema_registry/index.html) 讲 Confluent 信封，旁边是
[`avro`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/avro/index.html) 和 [`protobuf`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/protobuf/index.html)，
[`testing`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/testing/index.html) 讲进程内 Broker。

安装、教程和 Broker 列表在框架自己的站点上：
<https://powersemmi.github.io/ruststream/>。
