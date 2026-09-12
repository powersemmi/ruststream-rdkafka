# 测试

`testing` feature 提供 `KafkaTestBroker`，一个 Kafka 的进程内替身：同样的处理器和描述符，不需要
集群。它走的生命周期和真实 Broker 一样（`new` -> `connect` -> `shutdown`），本 crate 的生产环境发布
策略也能在它已连接的形态上构造发布者 - `KafkaPublish`、`KafkaTransactionalPublish` 和
`KafkaPartitionedPublish` - 因此你挂载处理器和它们的发布者的方式，与生产环境完全一致。把它加进
dev-dependencies：

```toml
[dev-dependencies]
ruststream-rdkafka = { version = "0.7", features = ["testing"] }
```

切勿在生产构建里启用它。

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:handler"
```

`tb.broker::<KafkaTestBroker>().publish(topic, &value)` 在这次发布触发的每个处理器都结算之后才返回，
因此断言读到的是最终状态，测试不需要 sleep：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:testapp"
```

## 断言逐条记录的设置 { #asserting-on-per-record-settings }

发布者把 `partition(..)` 折进记录本身，因此 Broker 日志不再显示调用点要求了什么。回答这个问题的是
槽位视图：`tb.out::<Marker>().with_options(&KafkaOptions::default().partition(3))` 读回经由这个槽位
的一次发布带着什么设置，而 `assert_options_default()` 是与之对应的断言，用来确认一次发布什么都没
指定，把放置交给了生产者。

进程内传输给每个主题只有一个分区，因此它记下这个数字，却不按它放置任何东西。多分区的放置要在集群上
测。

## 进程内的重新定位 { #repositioning-in-process }

进程内传输保留它路由过的每一条消息，因此订阅可以在这份日志上重新定位。一次投递的偏移量就是它在所属
主题日志里的下标。

你可以用 `Ctx<Position>` 读到这个位置，用 `Ctx<SeekHandle>` 移动订阅，这与处理器面对集群时用的是
同样的键。在批量处理器里，`SeekHandle` 这个键在 `KafkaBatchContext` 上起作用。订阅从哪里打开，你在
include 处理器时用 `start_at(..)` 选定。

所以，会重放或跳过的服务就是一个普通的 `TestApp` 测试。例子里日志在应用启动之前就灌好了，因此用
`start_at(..)` 打开的订阅会把它重放一遍。`tb.settle()` 在不发布任何新消息的情况下把这次重放推到静止，
于是断言读到的是最终状态：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:seek"
```

对于这个传输解析不了的位置，定位返回 `KafkaError::InvalidOptions`：时间戳（它不给记录打时间戳）、
`0` 以外的分区（这里每个主题恰好一个分区），以及订阅并不读取的主题。按时间戳解析的定位和多分区放置
要在集群上测。

## 进程内的事务 { #transactions-in-process }

在事务里发布的处理器挂在这里时，路由文件一行都不用改：处理器指定能力，挂载点指定生产环境的
`Publish::default().transactional_id(..)` 策略，替身据此构造出一个进程内的事务发布者。
`per_partition()` 同样能构造，因此取用 `Out<impl PartitionLanes>` 的处理器，每个工作分区各得一个
独立事务。

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:transactions"
```

`begin_transaction` 与 `commit` 之间的发布先扣住，然后一起放出；`abort` 把它们丢弃；误用约定也照样
成立 - 第二次 `begin_transaction` 报告 `TransactionBusy`，没有打开的事务时 `commit` 或 `abort` 报告
`NoTransaction`：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_testing.rs:transaction_asserts"
```

### 进程内事务不能重现什么 { #what-the-in-process-transaction-does-not-reproduce }

Kafka 事务的保证真正依赖的一切都在 Broker 端，其中没有一样能在一个 channel 里重现。下面每一条都点
出它让哪一类断言站不住脚：

- **原子可见性。** 一次提交逐条路由缓冲区，因此这里的订阅者可能看到它的一个前缀。真实的
  `read_committed` 读取端要么看到整个事务，要么什么都看不到，而替身根本没有隔离级别可读。切勿在
  进程内断言读取端绝不会看到部分提交。
- **僵尸隔离。** 事务 id 照样携带、照样报告，却不隔离任何东西：同一个 id 上的两个发布者在这里共存，而
  `init_transactions` 本该把较旧生产者的 epoch 隔离掉。切勿在进程内断言被替换的副本已被隔离出去。
- **Broker 端持有的超时。** 打开后不管的事务会一直开到进程结束；Kafka 会中止超过
  `transaction.timeout.ms` 的事务。`transaction_timeout` 和 `queue_timeout` 照样接受，但不带任何
  行为，因此测试对这两者都断言不了什么。
- **构造发布者时做的工作。** 用真实策略构造会创建生产者并初始化它的事务，因此错误的事务 id 在启动
  时就失败；在这里构造只分配一个缓冲区，不会失败。启动期的配置错误在进程内是看不见的。
- **精确一次。** 把消费的偏移量耦合进事务（`send_offsets_to_transaction`）需要一个消费者组和它的
  元数据，而进程内传输两样都没有。因此 `KafkaEosPublish` 根本不与测试 Broker 构造发布者：在它上面
  挂载一条精确一次路由是**编译错误**，这是刻意为之，好过一个绿色却什么都证明不了的测试。

这些全都在 `tests/integration_rdkafka.rs` 里针对真实集群覆盖：
`partition_scoped_transactions_run_independently`、`eos_pipeline_commits_offsets_with_records`、
`eos_aborted_window_replays_without_output_duplicates`、
`seeking_inside_an_eos_window_replays_without_committing_past_the_target` 和
`eos_publishing_handler_replies_ride_the_window`。

## 进程内的竞争消费者 { #competing-consumers-in-process }

`KafkaTopic::group(..)` 在这里保持它的含义：一条记录到达每个消费者组的**一个**成员，而每个消费者组
各读一份自己的副本。因此副本共用一个消费者组的服务，在测试里分摊工作的方式与在集群上一样，而不是
每个副本都处理每一条记录。

具体是哪个成员，取决于分区分配，而不是轮转。这个传输给每个主题恰好一个分区，而 Kafka 把一个分区
交给一个消费者组里恰好一个成员，因此一个消费者组的记录全部落在同一个成员上，不会摊开。这是单分区
主题的真相，测试也因此不能断言两个 worker 各干了一半。持有者放掉自己的订阅时，下一个成员接管这个
主题，而这正是再均衡带来的效果。

`KafkaTestBroker::default_group(..)` 与 `KafkaBroker::default_group` 相对应，服务于没有自己指定消费
者组的订阅。被测服务设了它，你就设上：不设的话，两个光写字符串的 `#[subscriber("orders")]` 处理器
会各自独占一个消费者组，两个都看到每一条记录，而同样的接线对着 Kafka 并非如此。

## 结算就是一个读取位置 { #settlement-is-a-read-position }

`nack(true)` 不是把一条消息重新入队。它让偏移量保持未结算，并让订阅从已提交的位置继续，因此那条
记录**以及它之后的一切**都会再投递一遍 - 这就是一次真实回拨所产生的至少一次重复。`ack` 和
`nack(false)` 结算偏移量，而最低的未结算偏移量就是之后一次回拨的落点。

所以是提交模式决定一次重试做什么，在这里和在集群上都一样：

- `Commit::Tracked` - 就是上面那种回拨。
- `Commit::Auto`（描述符的默认值）- **不起作用**：librdkafka 在记录交给应用的那一刻就存下位置，
  因此 `nack(true)` 什么都带不回来。在默认模式下重试的处理器，重试的是空气，替身把这一点直说出来，
  而不是殷勤地再投递一遍。
- `Commit::Transactional` - 回拨方式与 `Tracked` 相同，因为这里没有可以把提交推迟给它的精确一次
  管线。

有一处刻意的例外：**按名字**而不是按描述符打开的订阅（也就是 `Subscribe` 能力，光写字符串的
`#[subscriber("orders")]` 形态和核心的一致性测试套件都用它），结算方式与 `Commit::Tracked` 相同。
核心的路由约定要求 `nack(true)` 重新投递，而一个光名字不带任何提交模式来另作决定，因此这一条路径
守住约定；以 `KafkaTopic` 形式到达 Broker 的订阅，则按它自己的模式来。只要测试要考察一次重试做
什么，就在描述符上指明模式。

## 字节路径的处理器 { #byte-lane-handlers }

读 Confluent 传输格式的处理器在这里也是普通处理器：`IncomingFrame` 和 `OutgoingFrame` 自带字节，
因此它们完全不需要集群，而在 Protobuf 这一侧，读取连注册表也不需要。`TestApp` 的写法和它的手写版
对应形态，参见 [Schema Registry 这一页](schema-registry.md#testing-a-lane-handler)。

## 测试 Broker 不模拟什么 { #what-the-test-broker-does-not-simulate }

进程内 Broker 实现了核心的路由约定 - 按确切主题名路由、消费者组、结算、消息头、分区键消息头和工作
分区 - 再加上其上保留的日志，以及事务中客户端可见的那一半。它不模拟 Kafka 本身：真实分区、比订阅
活得更久的位置、再均衡、保留期和记录时间戳都属于集群行为，上面列的事务相关内容也一样。具体来说：

- **分区。** 每个主题恰好一个分区，编号为零。`Ctx<Partition>` 永远读到 `0`，`partition(..)` 步骤
  和 `PARTITION_HEADER` 标记都改变不了记录的落点，定位到任何别的分区会被拒绝，而不是凭空造一个
  出来。工作分区*是*重现了的：一条订阅的 `LaneKey` 在这里的解析方式与上游完全一致，因此在默认的
  `LaneKey::Partition` 下，一个主题的记录共用一个工作分区（一个分区、一种顺序，没有 key 的记录也
  在内），在 `LaneKey::RecordKey` 下则按 key 划分工作分区。测试显示不出来的是：两个 key 落在不同
  分区、多个分区并发运行，以及一个消费者组的工作摊到各个成员上。
- **已提交的位置活不过它的订阅。** 位置存在于订阅上，因此一次“重启”什么都留不下：同一个消费者组里
  后来的订阅者会在日志末尾打开，而不是从前一个停下的地方继续，`start(StartOffset::Earliest)` 也
  是空转。改用挂载点上的 `start_at(KafkaPosition::earliest())`，它确实会在保留的日志上打开订阅；
  重启后续读这件事要在集群上测。
- **重试与死信。** 本 crate 的重试管线不重现：`retry(..)`、`dead_letter(..)` 和
  `max_deliveries(..)` 建立在真实消费者之上，在替身上都是空转，因此这里绝不会有往重试主题或死信
  主题的重新发布。`retry_after` 同样够不着，因为它需要一个构建期的发布者
  （`KafkaBroker::retry_publisher`），而进程内 Broker 不产出这种发布者。测试看到的是没有重试，而
  不是一次错误的重试。
- **手动分配与主题正则**出于同样的原因明确拒绝（`KafkaError::InvalidOptions`），而不是拿近似的
  东西糊弄。

真实语义在一个真实集群上跑：

```text
just brokers-up
KAFKA_TEST_URL=127.0.0.1:9092 cargo test --workspace --all-features -- --test-threads=1
```
