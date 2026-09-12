# 发布

出站消息的名字就是目的主题。`KafkaPublish` 声明队列超时和事务 id，你在注册处理器的地方指定它。
启动时，策略在已连接的 Broker 上实例化 `KafkaPublisher`。

普通发布者通过 Broker 共享的生产者发送。事务发布者拿到自己的一个生产者，由它的 id 隔离。每次发布
都等待集群的投递报告，因此返回 `Ok` 就意味着 Kafka 收下了这条记录。

发布有两套词汇，它们属于不同的文件。处理器主体导入 `ruststream::prelude`，并用它需要的**能力**
约束注入进来的槽位（`Out<impl Publisher>`、`Out<impl TransactionalPublisher>`），因此它完全不提任何
Broker 类型。挂载点导入 `ruststream_rdkafka::prelude`，它在核心的 prelude 之上加了以概念命名的
**策略**：`Publish`、`TransactionalPublish`、`PartitionedPublish` 和 `EosPublish`。因此挂载点在每个
Broker 上读起来都一样。

策略在哪里指定：

- 只写 `b.include(handler)` - 返回的回复经由 Broker 的默认策略 `KafkaPublish::default()` 发布。
- `b.include(handler).out(Reply, policy)` - 处理器的回复发布者。
- `b.include(handler).out(marker, policy).build()` - `Out<..>` 参数收到的发布者，用槽位的标记指定
  （参数没有声明标记时是 `DefaultSlot`）。
- `b.after_startup(policy, hook)` - 作用域一级的钩子，在所有订阅打开之后，带着活的发布者跑一次。
- `connected.publisher(policy)` - 在运行时之外，用在你自己连接的 Broker 上（参见 `kafka_producer`
  这个例子）。

有一个发布者不来自策略。`broker.retry_publisher()` 来自*尚未连接*的 Broker，只为一处需要活发布者
的接线：`retry_via`，也就是顶替 Kafka 所没有的延迟重投的那个延迟重新发布（参见
[批次结算](topics.md#how-batch-settlement-maps-onto-kafka)）。它在启动时解析连接。`connect` 之前
它返回 `KafkaError::NotConnected`，Broker 关闭之后返回 `KafkaError::Closed`。

## 发布构建器 { #the-publish-builder }

每个发布者都用同一种方式开始一次发布，经由通用的 `PublishExt`：`message(&value)`，然后 `to(..)`
指定目的地、`with_headers(..)` 指定消息头、`with_codec(..)` 换一个编解码器，最后 `publish()` 发出。
处理器的 `Out` 参数、放在应用状态里的发布者，以及从已连接 Broker 建出来的句柄，都经由这几个调用
发布；不同的只是 `message(..)` 用的编解码器。已经编码好的载荷是一个带
`#[derive(Outgoing, Serialized)]` 的 newtype，同样的调用把它发出去，中间没有编码这一步。

本 crate 加了自己的一个步骤 `partition(..)`，用来指定记录的目的分区。它来自 `KafkaPublishSteps`
trait，因此调用它的处理器主体要导入本 crate 的 prelude，并按选项类型约束自己的槽位：
`Out<impl Publisher<Options = KafkaOptions>, Marker>`。这条约束是处理器主体唯一一处提到本 crate 而
非框架的东西，也正是它挡住这个步骤跑到别的 Broker 的发布者构建器上。

记录 key 走的是发布的消息头位置：那是框架自己的分区键约定，Kafka 把它映射到原生的记录 key。两者
都在下面。

不逐条消息变化的放置规则，写成挂载点链上的一个 `PublishTransform` 步骤（下面的 `RoundRobin`）。

`Out` 参数指定的是一种能力，不是一个发布者类型。本 crate 声明了自己的一种，`PartitionLanes`：它按
来源分区各交出一个事务发布者。处理器写 `Out(lanes): Out<impl PartitionLanes>`，而 `per_partition()`
策略在它背后构造出具体的 `TransactionalPartitions`（参见
[事务作用域与工作池](#transaction-scopes-and-worker-pools)）。

一条通道经由槽位交出来的那个发布者发布，因此它的消息进入 Broker 的发布日志，而不是槽位的测试记录
（`tb.out::<Marker>()`）。在测试里，通道的流量用发布日志断言；槽位记录覆盖的是经由槽位本身发布的
处理器。

## 记录 key { #record-keys }

分区键消息头在发布时成为记录的原生 key，并且不再作为 Kafka 消息头重复一份。经由本 crate 消费时，
这个 key 用同一个消息头名字报告回来。Kafka 把共用一个 key 的记录路由到同一个分区，按键的顺序就是
这样保住的：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_producer.rs:producer"
```

没有这个消息头时，分区由配置的分区器挑选。

## 显式分区与轮转分发 { #explicit-partitions-and-round-robin-distribution }

`partition(n)` 把一条记录钉在一个确切的分区上。显式分区盖过记录 key，记录 key 盖过配置的分区器。
这个数字是 `i32`，因此没有拼错的余地；指定主题没有的分区，发布会返回一个投递错误。

在发布构建器上，这个步骤写作 `out.message(&item).partition(3).publish()`。构建器之下 - 你自己持有
的发布者、灌数据的工具 - 同一个设置就是裸 `publish` 所接受的那个 `KafkaOptions` 值：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_producer.rs:partition"
```

`RoundRobin` 则把回复摊匀。librdkafka 没有哪个分区器是逐条轮转放置记录的，没有 key 的记录还可能
一整批都粘在同一个分区上。每条消息的处理耗时长且近乎恒定时，这意味着一个消费者过热、其余的闲着。
这个变换给每一条既没有 key、也没有自己分区的回复，设上循环中的下一个分区：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_distribution.rs:round_robin"
```

这个数目要写明，而且必须与目的主题的分区数一致：少了会让末尾的分区闲着，多了会让发往不存在分区的
发布返回错误。

变换触及的是记录，不是那次发布调用，因此它经由 `kafka-partition` 消息头（一个 ASCII 十进制数）指定
分区，发布者从那里读取；这个消息头本身不会发送出去。你自己的变换要表达别的放置规则，用的也是这
条通路。调用点则用那个步骤。

## 投递保证 { #delivery-guarantees }

持久性取决于生产者的 `acks` 设置。librdkafka 的其余属性（`enable.idempotence`、
`message.timeout.ms`、压缩）由你自己在 Broker 上设置：

- `KafkaBroker::producer_config(key, value)` - 只作用于生产者的属性。
- `KafkaBroker::config(key, value)` - 作用于整个客户端的属性（消费者和生产者）。

要做到端到端的至少一次，把幂等生产者（`producer_config("enable.idempotence", "true")`）与消费侧的
`Commit::Tracked` 组合起来（参见[主题与消费者组](topics.md)）。

## 事务 { #transactions }

`KafkaPublish::default().transactional_id("orders-svc-1")` 构造出
`KafkaTransactionalPublisher`，它添上核心的 `TransactionalPublisher` 能力。`begin_transaction` 与
`commit` 之间的发布原子地变为可见：处于 Kafka 默认 `read_committed` 隔离级别的读取端，要么全看到，
要么一条都看不到。`abort` 在 Broker 端把它们丢弃。

在打开的事务之外，这个句柄像普通句柄一样发布。没有打开的事务时调用 `commit` 或 `abort` 返回
`NoTransaction` 错误，第二次 `begin_transaction` 返回 `TransactionBusy`。

一次调用做一次原子的多目的地分发，末尾提交，遇到第一个错误就中止：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:fanout"
```

处理器用注入的 `Out` 参数取到活的发布者，而策略在订阅打开之后把它实例化出来。一次中止不留下任何
可见的东西，因此处理器请求重新投递，把整次分发从头再跑一遍：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:handler"
```

id 由挂载点指定，每个并发的生产者一个：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:id"
```

策略构造发布者时，创建事务生产者并初始化它的事务。这次初始化把先前持有该 id 的生产者隔离掉，因此
这个句柄从存在的那一刻起就受隔离保护。这个 id 必须稳定，并且每个并发生产者各不相同：为每一条
并发的事务流程各指定一个策略。`transaction_timeout` 限定控制调用的时长，Kafka 自己的
`transaction.timeout.ms` 则通过 `producer_config` 设置。

把消费的偏移量绑进生产者事务，也就是完整的 consume-transform-produce 形态，是下面的
[精确一次管线](#exactly-once-pipelines)。

### 事务作用域与工作池 { #transaction-scopes-and-worker-pools }

这里的一切由两个 Kafka 事实决定：一个生产者同一时刻只跑一个事务，一个事务 id 只属于一个活的生产者
（初始化第二个会把第一个隔离掉）。因此 `workers(n, by_key)` 这样一个池不能共用一个事务发布者：把
两条通道的消息并进一个事务，会把一条流程的记录和另一条的一起提交。

能与工作池组合的作用域是来源分区。在默认的 `LaneKey::Partition` 通道下，一个分区的投递在一条通道
上串行处理。`per_partition()` 策略构造出 `TransactionalPartitions`：每个分区一个发布者，id 形如
`"{base}-p{partition}"`，因此每条通道各跑一个独立的事务，彼此不需要协调。这组 id 跟随主题的分区，
而不是工作者数量，因此改动 `workers(n)` 既不改 id，也不改隔离关系：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:partitions"
```

挂载点指定基础 id：
`.out(DefaultSlot, Publish::default().transactional_id("billing-svc-1").per_partition())`。
`TransactionalPartitions` 在第一次用到某个分区时才创建并初始化它的发布者，`for_partition` 因此是
异步的，并且会返回初始化错误。

按分区的作用域不与按记录 key 的通道（`LaneKey::RecordKey`）组合：那种通道把一个分区摊到多条通道上，
于是两条通道会在这个分区的发布者上撞车。整个池共用一个 id，那是下面的精确一次管线。

### 精确一次管线 { #exactly-once-pipelines }

`KafkaEosPublish` 覆盖完整的 consume-transform-produce 形态（KIP-447），并构造出活的 `EosPipeline`。
一个事务生产者服务所有通道，并在自己的事务内提交消费的偏移量（`send_offsets_to_transaction`），
因此来源位置与发布出去的记录原子地一起前进。崩溃或者中止的窗口把两者一起回退：处理器重新处理这些
投递，而输出主题绝不会看到重复。

订阅上的 `Commit::Transactional("enrich-svc-1")` 关掉它自己消费者的提交，并把它的水位注册到这个 id
的管线上。`KafkaEosPublish::new("enrich-svc-1")` 是生产者这一侧，而管线本身只有在这条策略于已连接
的 Broker 上实例化它之后才存在。

三个地方指定同一个 id：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:eos"
```

会发布的处理器不需要显式发布。把管线指定为挂载点的策略，在其后加上 `EosReplies` 变换，每条回复就
会与它那次投递消费的偏移量一起，进入打开的窗口：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:eos_wiring"
```

`EosReplies` 把投递的来源坐标复制到回复上，管线正是靠这个把两者配对的。它是挂载点链上一个普通的
步骤，因此你可以照常在它前后放 `.codec(..)` 和别的 `.transform(..)`。不加它，第一条回复的发布会
返回一个关于坐标缺失的错误，而不是把它发到窗口之外。

对于你自己写的发布，`EosPipeline::publish` 把投递坐标作为参数接收，而处理器像读任何别的
`KafkaContext` 字段一样，从 `Ctx<Source>` 参数读到自己的坐标。处理器不能把管线当作 `Out` 槽位取用：
槽位的约束指定的是一种能力，而本 crate 没有为管线的显式形态声明能力。

活的管线经由启动钩子到达处理器。`b.after_startup(EosPublish::new("enrich-svc-1"), hook)` 在 Broker
连接好之后，带着 `EosPipeline` 调用 `hook`，而钩子不接收应用状态，因此服务用一个单元格在两者之间
传递管线：一个 `Arc<OnceLock<EosPipeline>>`，克隆一份进 `on_startup` 返回的状态，再克隆一份进负责
填充它的钩子。处理器经由 `State<..>` 读这份状态，并用 `pipeline.publish(&source, msg)` 发布。这个
id 只在一处指定：同一个事务 id 上的第二条管线会把第一条隔离掉。

发布加入管线打开的窗口。每隔一个 `commit_interval`（默认 100 毫秒，也是 Kafka Streams 精确一次的
默认值）窗口关闭一次：管线等待它的参与者结算，把结算好的位置和消费者的消费者组元数据加进事务，然后
提交。消费者组元数据在服务端隔离掉过期的消费者，因此窗口中途的一次再均衡会让提交返回错误，而不是
去提交消费者已经不再拥有的偏移量。

三件事会中止窗口：一次发布错误、一次提交错误，或者一次结算停滞（处理器卡住，或者请求重试超出了
发布者的事务超时）。此时消费者回拨到最后已提交的偏移量，因此整个窗口很快重新投递，并重新发布进一个
新事务。发布进已中止窗口的记录，对 `read_committed` 的读取端从来都不可见 - 而这正是 librdkafka 在
这里的默认值。

实践要点：

- 每个服务实例一个管线 id，和任何事务 id 一样：它是隔离的单位。
- 端到端延迟至少是一个提交间隔：记录在窗口提交时才可见，而不是在发布时。
- 参与者的一次 `retry()` 会把它的窗口拖到事务期限然后中止，因此在 EOS 处理器里，毒消息最好用
  `drop()` 和死信处理。
- `retry_after` 的延迟重新发布退路（`retry_via`，参见
  [批次结算](topics.md#how-batch-settlement-maps-onto-kafka)）不适用于 EOS 回复：一份延迟的副本会
  破坏偏移量与记录的配对。
- 回复这条路只对处于 `Commit::Transactional` 模式、并且指定了本管线 id 的订阅有效；来自其他订阅的
  回复会返回错误。
- 它在默认的 `LaneKey::Partition` 通道上效果最好，那里每个分区都跟在自己通道的头部按顺序结算。

## 背压与关闭 { #back-pressure-and-shutdown }

librdkafka 的本地队列满了时，一次发布会无限期地等待空位，这就是天然的背压行为。
`KafkaPublish::queue_timeout` 给这段等待设上限，发布随后返回一个队列已满的错误。

`ConnectedKafkaBroker::shutdown` 把在途的发布刷出去，并在它们未能在 `KafkaBroker::flush_timeout`
（不设就是 30 秒）之内送达时返回错误。它消耗掉已连接的 Broker，返回 `ClosedKafkaBroker` 见证，它的
`unflushed_records()` 数出 librdkafka 当时还握着多少条。关闭之前创建的发布者作为值仍然可用，而经由
它们的每一次发布都返回 `KafkaError::Closed`。
