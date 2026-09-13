# 主题与消费者组

一次订阅，就是一个消费者在一个主题上加入一个消费者组。`KafkaTopic` 描述它，其中只有主题名是你必须
给的。你不设的每个选项，都保持 librdkafka 的默认值。

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_topics.rs:descriptor"
```

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_topics.rs:app"
```

## 消费者组 { #consumer-groups }

Kafka 无法在没有消费者组的情况下订阅主题。用 `KafkaTopic::group` 为每条订阅各指定一个，或者用
`KafkaBroker::default_group` 为整个 Broker 指定一次。Broker 一级的消费者组覆盖所有没有自己指定的
订阅，包括光写字符串的 `#[subscriber("orders")]` 形态。最终没有消费者组的订阅是一个启动错误，除非
它指定了自己的分区：手动分配不加入任何消费者组。

## 起始偏移量 { #start-offsets }

一个消费者组对某个分区没有有效的已提交偏移量时，`StartOffset` 决定它从哪里开始读。消费者组从未提
交过这个分区时就没有；保留期删掉了已提交的偏移量，或者它已经越界时，也没有。长期空闲的消费者组会
撞上第二种情况：在 librdkafka 的 `latest` 重置下，它直接跳到日志末尾，而不是重新处理。这个选项映射
到 librdkafka 的 `auto.offset.reset`：

- `Committed`（默认）- 把选择交给 librdkafka（它的默认值重置到最新的偏移量）。
- `Earliest` - 从保留下来的最早偏移量开始。
- `Latest` - 从最新的偏移量开始，因此只有消费者组组成之后发布的消息才会到达。

## 提交模式 { #commit-modes }

Kafka 每个分区提交一个位置，而不是逐条结算消息。`Commit` 模式决定 `ack` 和 `nack` 怎样映射到这个
模型上：

- `Commit::Auto`（默认，也就是 librdkafka 自己的行为）：位置在消息交给应用的那一刻存下，每隔
  `auto.commit.interval.ms` 提交一次。`ack` 和 `nack` 不起作用。一次崩溃可能丢掉已处理但未提交的
  那一段尾巴，也可能跳过位置已经存下、却还没处理的投递。
- `Commit::Tracked`：精确的至少一次。一次 `ack` 把已存的位置推进到最低的、仍未结算的投递之下，
  因此来自并发工作分区的乱序 ack 绝不会把提交推过一条还没处理的消息。消费者根本收不到的那些偏移量
  空洞（事务标记、被压实掉的记录）挡不住这个位置。这条订阅会关掉 `enable.auto.offset.store`，而
  自动提交仍然在后台把已存的位置刷出去，消费者关闭时再刷一次。
- `Commit::Transactional("pipeline-id")`：精确一次。事务 id 与之匹配的 `EosPipeline` 在生产者事务
  内部提交偏移量，与处理器发布的记录原子地一起提交，消费者自己什么都不提交。`ack` 推进共享水位的
  方式与 `Tracked` 完全一样；参见[精确一次管线](publishing.md#exactly-once-pipelines)。

`Tracked` 下的否定结算：

- `nack(false)`（丢弃）结算这个偏移量，位置因此可以越过它。
- `nack(true)`（重新入队）让偏移量保持未结算。已提交的位置停在它下面，因此下一次拉取这个分区时，
  Kafka 从那里重新投递 - 而下一次拉取随再均衡或重启到来。未结算的偏移量还会拖住水位，因此其后每
  一次 ack 都保持未提交，直到那个偏移量结算为止：一个不断 nack 的处理器会把已提交的位置钉死。
  一条消息能得到多少次投递、用完之后去哪里，都在挂载点声明，参见[重试与死信](#retries-and-dead-lettering)。

## 多主题与主题模式 { #multiple-topics-and-patterns }

`KafkaTopics` 用一个消费者、一个消费者组消费多个主题。所有匹配到的主题共用这个处理器，因而也共用
它的载荷类型；每次投递仍然报告自己来自哪个主题：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_multi_topic.rs:multi"
```

主题集合开放时，用 librdkafka 的主题正则订阅所有匹配的主题。这个正则必须以 `^` 开头，librdkafka
正是靠这个锚点判断一个名字是正则：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_multi_topic.rs:pattern"
```

`KafkaTopics` 带着与 `KafkaTopic` 相同的消费者组、起始偏移量、提交模式和工作分区选项。它之所以自
成一个类型，是因为读取一组主题的订阅无处安放重试副本，参见[重试与死信](#retries-and-dead-lettering)。
把以 `^` 开头的名字交给 `KafkaTopic`，启动时会被拒绝，错误信息指向这里。

进程内测试能覆盖按确切名字的多主题订阅；正则需要一个集群。

## 分区分配 { #partition-assignment }

`KafkaTopic::assignment` 决定消费者组怎样在成员之间均衡分区（librdkafka 的
`partition.assignment.strategy`）：`Assignment::Range`、`Assignment::RoundRobin` 或
`Assignment::CooperativeSticky`。cooperative-sticky 策略增量地再均衡：它不移动的那些分区，在再均衡
进行期间继续投递。不设置就是 librdkafka 的默认值（`range,roundrobin`）。同一个消费者组里，协作式
策略和急切式策略不能混用。

## 手动分区分配 { #manual-partition-assignment }

`KafkaPartitions` 恰好接管你指定的那些分区，既不加入消费者组，也不参与再均衡。这适合钉在某个分区上
的读取端、检查或重放工具，以及一个分区一个消费者的部署方式。

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_topics.rs:assign"
```

消费者组在这里仍然可选，它只改变偏移量的处置方式。指定了消费者组时，消费者不加入它，却把偏移量提交
进去：`Commit::Tracked` 存位置的方式与普通订阅完全一样，`StartOffset::Committed` 从它们恢复。不指定
消费者组时提交是关的，因此起始偏移量必须写明（`Earliest` 或 `Latest`），ack 也不起作用；此时
`Commit::Tracked` 和 `StartOffset::Committed` 都是启动错误。

没有消费者组的读取端，运行时仍然带着某个 `group.id`，因为 librdkafka 连手动分配也要求一个：占位的
`ruststream.standalone` 从不加入、也从不提交。

手动分配指定的是一个主题的确切分区 - 它因此是一个独立的描述符，而不是 `KafkaTopic` 上的一个选项：
读取一组名字的订阅根本没有分区可指定。它不与 `Commit::Transactional` 组合，也至少要指定一个分区，两
者都是启动错误。进程内测试 Broker 不模拟分区，会拒绝这样的描述符，所以这属于真实集群上的测试。

手动分配能与按键的工作分区组合。在默认的 `LaneKey::Partition` 下，每个分到的分区各得一个工作分区，
因此 `KafkaPartitions::new("orders", [0, 2, 5])` 配上 `workers(n, by_key)`，会按顺序处理每一个分到
的分区。`n` 要照着
分区列表定：工作分区比分区少，分区就共用工作分区，顺序依然保持；工作分区比分区多，多出来的就闲着。

## 重新定位订阅 { #repositioning-a-subscription }

Kafka 保留日志，因此你可以在日志里移动一条订阅：从更早的点重放、跳过一段毒消息，或者每次启动都从头
重建一份投影。位置是一个 `KafkaPosition`，用它的构造函数建出来：

| 位置 | 作用于 | 恢复自 |
|---|---|---|
| `KafkaPosition::earliest()` | 所有分到的分区 | 保留下来的最早偏移量 |
| `KafkaPosition::latest()` | 所有分到的分区 | 日志末尾（只有新记录） |
| `KafkaPosition::offset(partition, n)` | 一个分区 | 绝对偏移量 `n` |
| `KafkaPosition::timestamp(millis)` | 所有分到的分区 | 该时刻或之后的第一条记录（没有则是末尾） |

每次投递也报告自己的位置，钉在主题、分区和偏移量上。定位到它，会重新投递恰好那条记录，以及它后面
按顺序排列的后缀。

位置用在两处。`start_at(..)` 子句让订阅每次启动都从那里打开，不管消费者组此前提交了什么；
`StartOffset` 只在消费者组对该分区没有已提交偏移量时才起作用：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_seek.rs:start_at"
```

第二处是运行中的处理器。它可以用 `Ctx(seeker): Ctx<SeekHandle>` 参数取到订阅的 seeker，在函数体里
重新定位这条订阅。并列的 `Position` 键报告正在处理的这次投递位于何处，而重放这条记录要的正是它：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_seek.rs:handler"
```

批量处理器改为声明 `KafkaBatchContext`，并从它读同一个 `SeekHandle` 键。这个上下文不持有任何逐条
投递的东西：一批横跨很多记录，没有哪一个位置能描述它。定位到哪里，从批次的元素里取：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_seek.rs:batch"
```

作用范围与记账：

- **一次定位移动的是这个消费者实例，不是消费者组。** 它重新定位这个成员当前持有的分区；其他成员
  继续读它们原来的位置，也不会替谁提交任何东西。定位到一个本消费者并不持有的分区，会返回错误。
- **一次再均衡作废一次定位。** 这次重新定位属于它所应用的那份分配。成员加入或离开、会话超时、
  主题元数据变化，都会撤走那些分区，接手它们的人（包括本实例）从消费者组已提交的偏移量继续。必须
  熬过重启的位置属于 `start_at(..)`，它每次启动都应用一遍。
- **偏移量跟随读取位置。** 在 `Commit::Tracked` 下，一次定位清掉它移动的每个分区的跟踪位置，也清掉
  librdkafka 自己的偏移量存储，因此之后的提交不可能越过那些定位重放过、却没人处理过的记录。结算
  一条定位之前取到的投递不改变任何事，因为它指的是订阅已经不再读取的位置。在精确一次管线里，定位
  落下时仍然打开的事务窗口会中止而不是提交，因此消费者组绝不会越过重放的那一段，重放出来的投递
  则在一个新的窗口里处理。

会重新定位的服务，不用集群也能测。进程内测试 Broker 保留它路由过的一切，并在那份日志上把同样的
seeker 交给订阅，因此上面的处理器原封不动地挂在它上面。它能解析哪些位置、拒绝哪些位置，参见
[进程内的重新定位](testing.md#repositioning-in-process)。

## 按键的工作分区 { #keyed-worker-lanes }

Kafka 按原生的记录 key 分区，本 crate 能把这个 key 当作分区键，因此 `workers(n, by_key)` 把按键
的顺序从生产者一路保到处理器：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_keys.rs:consumer"
```

分区键是什么，由 `KafkaTopic::lane_key` 决定。默认值 `LaneKey::Partition` 按来源分区划分工作分区，
也就是 Kafka 自己的排序单位：一个分区投递的一切，包括没有 key 的记录，都在一个工作分区上按顺序
处理，并发来自同时消费多个分区：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_keys.rs:partition_lanes"
```

`LaneKey::RecordKey` 把工作分区收窄到记录 key，正如上面第一个例子：共用一个记录 key 的投递保持
有序，同一个分区里不同的 key 并发处理。这时没有 key 的投递就没有分区键，它在各个工作分区之间轮转，
连自己分区的顺序都丢掉了。

## 重试与死信 { #retries-and-dead-lettering }

Kafka 既不能把一条记录扣住，也不统计自己的投递次数，所以这两件事都由框架来做。处理器回答
`retry_after` 时，这次投递被丢掉；延迟结束后，它的一份副本被重新发布回这条订阅，框架的
`x-ruststream-retry-count` 消息头加一。

紧跟在 `include` 后面的两步，说明一条消息能得到多少次投递、用完之后去哪里：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_retries.rs:declaration"
```

`max_attempts(n)` 把第一次投递算作第一次。`dead_letter(topic)` 是用尽的投递被原样重新发布到的主
题，载荷和消息头都照旧。只设上限而不指定去处，用尽的投递会被拒绝；只指定去处而不设上限，每一份副
本都会被送走，而不是送回。两步在任何 Broker 上读起来都一样。

处理器这一侧就是一个普通的结果：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_retries.rs:retry_after"
```

在声明了上限的情况下，立即的 `retry()` 也是同一份副本，只是马上发布，而不是等过延迟。计数正是这样
随消息一起走的：Kafka 自己的重新投递不带计数，永远到不了上限。

副本回到订阅所读的那个主题，而 `KafkaTopic` 自己就能回答那是哪个主题。`KafkaTopics` 和
`KafkaPartitions` 读的是一组，往其中任何一个成员发布副本都会回错地方，所以这类注册自己指定去处：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_retries.rs:named"
```

`.to(topic)` 属于副本经由的那个发布者，所以它跟在 `out_retry` 后面。`out_retry(policy)` 也是替换这
个发布者的方式 - 换一条策略、换一个编解码器、加一个变换 - 每条注册一次。读取一组主题却两者都没指定
的注册，会让服务起不来。

这样的注册即使处理器从不重试，也要指定去处：重试发布者本来就在，而拒绝启动挡住的正是一份无处可去的
副本。

`.to(topic)` 是固定重试主题的写法。对 `KafkaTopics` 和 `KafkaPartitions`，该取的是
`ToSourceTopic`：它把每一份副本送回它自己那次投递所来的主题，于是 `orders-eu` 上重试的记录仍然留在
`orders-eu`：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_multi_topic.rs:naming_transform"
```

它从 Kafka 上下文里读出主题，而这个上下文在处理器自己也读它时才会到达重试位置 - 用本 crate 的任意
一个 `Ctx<..>` 键。批量处理器带的是批次上下文，其中没有单条记录的主题，所以批量注册用
`.to(topic)` 指定。

重试主题和死信主题是你的基础设施：框架只负责往里发布。死信的消费者就是一条普通订阅，计数消息头说明
这条消息走到了哪一步：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_retries.rs:dead_letter"
```

副本在延迟窗口内是至多一次的：进程在定时器触发前退出，副本就丢了。`Commit::Auto` 下这一切都不适
用，因为记录一交给应用位置就已存下，原件再也丢不掉了。

## 批量消费 { #batches }

参数是一个切片的处理器整批消费：说明这一点的是签名，不是属性。一批就是一次投递加上 librdkafka 已经
拉到的一切，不额外等待：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_batches.rs:handler"
```

处理器能接受的最大批次由挂载点指定，不指定的批量处理器无法通过编译。这个大小一直传到消费者的
poll，它交给函数体的记录数绝不超过它；拉取队列里只有那么多时，批次就更小：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_batches.rs:size"
```

librdkafka 本地排多少队是另一个消费者端设置，它留在描述符的原始配置透传里
（`queued.max.messages.kbytes` 之类）。其余方面，批量处理器和别的处理器一样用 `include` 挂载：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_batches.rs:app"
```

### 批次结算怎样映射到 Kafka { #how-batch-settlement-maps-onto-kafka }

批量处理器用一个 `HandlerOutcome` 结算整批，或者返回 `Vec<HandlerOutcome>` 逐个元素结算，其中第
`i` 项结算第 `i` 个元素：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_batches.rs:selective"
```

Kafka 每个分区提交一个位置，而不是每条消息一个，因此各种结果按下面的方式映射上去。以下全部以
`Commit::Tracked` 为前提：在 `Commit::Auto` 下每一次结算都不起作用，这一段都不适用。

- **整批统一 `Ack`** - 这批结算，位置前进。
- **逐个元素、全是 `Ack`** - 同样精确。并发的多个批次之间，ack 可能乱序落下，而位置始终推进到最低
  的未结算投递之下。
- **逐个元素、中间有一个 `retry()`** - 每个元素确实各自结算了，但已提交的位置停在第一个要重试的
  元素之前，并一直停在那里，直到那个偏移量在下一次拉取这个分区时重新投递。排在它后面、已经 ack
  的元素那时也一并重放：这是至少一次的重复，不是丢失。因此就已提交的位置而言，选择性 ack 只推进
  到第一次重试为止；当一个毒元素不该拖住整批时，在这条注册上声明尝试上限和死信主题。
- **逐个元素、用了 `retry_after(..)`** - Kafka 没有原生的延迟重投，因此运行时退回到延迟重新发布。元素立刻结算，
  位置越过它，延迟结束后一份副本发布到主题末尾，`x-ruststream-retry-count` 消息头加一。这份副本
  丢掉了它在顺序中的位置，并且在延迟窗口内是至多一次的：定时器触发前的一次崩溃会把它丢掉。

    重试位置就是一个普通槽位，因此 `.codec(..)`、`.transform(..)` 和 `.map_publisher(..)` 都能
    跟在它后面。副本带的是投递自身的字节，所以那里指定的编解码器只解析位置、不做编码，而各个变换
    照常作用在副本上。

    这份副本去哪里，由订阅决定。`KafkaTopic` 用它的主题回答，往那里发布能到达每一个读它的消费者
    组。`KafkaTopics` 和 `KafkaPartitions` 读的是一组，一次发布触及不到它，所以由注册自己用
    `.out_retry(policy).to(topic)` 指定去处；两者都没指定的注册会拒绝启动，而不是在运行期悄悄丢
    副本。
- **结果向量比批次短** - 它没覆盖到的元素一律重试，并记录下这处不匹配。

### 并发 { #concurrency }

批量注册上的 `workers(n)` 让最多 `n` 个批次同时在处理中，它们的 ack 乱序到达时，跟踪的位置依然
正确。`by_key` 对批次没有意义：那里的按键策略表现得就像一个同样大小的普通池。按键的顺序属于单条
消息的处理器，在那里 `workers(n, by_key)` 把共用一个分区键的投递放在同一个工作分区上（参见按键
工作分区的例子）。

进程内测试 Broker 也用同样的方式原生成批，把已入队的排空，因此批量处理器挂在哪个 Broker 上都不用
改。

## 消费错误 { #consume-errors }

消费者错误会在订阅上浮现，除了 librdkafka 自己已经在重试的那些：今天恰好是
`UnknownTopicOrPartition`，也就是订阅了一个还不存在的主题。这样一段插曲开始时，订阅打一条警告，
这条警告就是该采取行动的监控信号；重复和恢复都走 debug。因此姗姗来迟的主题（Broker 自动创建、
资源开通的竞态）会自行恢复，不会淹没分发的错误日志，而永远不出现的主题会让那条警告一直挂着。

## 原始配置透传 { #raw-configuration-passthrough }

`KafkaTopic::config(key, value)` 设置本 crate 没有做成类型化选项的任何 librdkafka 消费者属性
（`fetch.min.bytes`、`session.timeout.ms`、`isolation.level` 等等）。它最后应用，因此盖过类型化
选项。设置某个提交模式所依赖的键，例如 `enable.auto.offset.store`，会改变那个模式的行为。
