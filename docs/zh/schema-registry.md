# Schema Registry

统一到 Confluent Schema Registry 的 Kafka 部署，用 Confluent 传输格式给载荷加上信封 - 一个值为零的
魔数字节、一个 4 字节大端的 schema id，然后是编码后的数据 - 而 schema 本身存放在注册表里。
`schema-registry` 这个 cargo feature 覆盖两半，而消费它们有两条路。**首选编解码器。**

**编解码器**把 schema 放在序列化器该在的位置。`AvroCodec` 持有 schema，处理器收下模型、交回模型，
签名里不出现任何与传输格式有关的东西 - 这正是编解码器的用处，也让它成为读写 Avro 或 JSON Schema
载荷的唯一途径。Avro 恰好合乎这个位置：它是一种由 schema 驱动、带 serde 前端的格式；信封下的 JSON
载荷，则是核心自己的 `JsonCodec` 放进 `SchemaFramed`。

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_avro_codec.rs:handler"
```

**转码**在 Broker 的两个边缘上做转换，因此处理器在默认编解码器上保持普通的 serde 模型，从不见到
传输格式。这是兼容之路：服务不能携带生成类型、也不能携带从 Avro 导出的模型时，它是对的选择，代价
是每条消息多走一趟 JSON，以及失去 schema 解析 - JSON 处理器没有读取端 schema 可供解析。

这两条路在同一个 Broker 上互不混用。`KafkaBroker::schema_registry(registry)` 把转码挂到该 Broker
打开的每一条订阅上，因此编解码器拿到的会是 JSON；编解码器改用
`KafkaBroker::schema_prefetch(..)`，它解析 schema，却不碰载荷。

Protobuf 不是第三个选择。`prost` 消息不是 serde 类型，因此它永远进不了编解码器的位置，它的载荷
自己序列化自己，处理器收下和交回的就是这个生成类型。这条路见下面的 [Protobuf](#protobuf)。

## 每种格式能走到哪 { #what-each-format-reaches }

这些路对三种格式的覆盖并不齐整。有一处空白是类型系统逼出来的，永远不会补上；其余的是缺口，这张表
标明哪个是哪个，免得读者去猜。

| | Avro | JSON | Protobuf |
| --- | --- | --- | --- |
| 编解码器 | `AvroCodec::local`、`AvroCodec::registry` | `SchemaFramed<JsonCodec>` | 必然的空白，见下 |
| 处理器直接面对消息 | 编解码器 | 编解码器 | `#[wire(..)]` + `ProtobufFrame` |
| 转码 | 是 | 是 | 是 |
| 从类型上读出 schema | `AvroSchema` | `schemars::JsonSchema` | **否** |
| 从类型上注册 subject | `register_avro::<T>` | `register_json::<T>` | **否** |
| schema 引用（`import`） | 不适用 | 不适用 | **不解析** |
| 在 `connect` 时解析 subject | 编解码器 | 编解码器 | 必然的空白 |
| `MissingSubject` | 编解码器 | 编解码器 | 必然的空白 |
| 启动时 `check_compatibility` | 编解码器 | 编解码器 | 必然的空白 |
| 每次投递解析写入端 schema | 编解码器，它需要 | 不需要 | 不需要 |
| 共享的 id 与 subject 缓存 | 所有路径 | 所有路径 | 所有路径 |

**必然的空白。** Protobuf 永远不可能成为编解码器。关卡在 `Codec::encode<T: Serialize>`：`prost`
消息不是 serde 类型，因此它根本进不了编解码器的位置 - 信封是它唯一的家，这是格式本身的性质，
不是没做完的角落。每一行写着“必然的空白”的 Protobuf 条目，都是同一个事实再往下一步：`SchemaPrefetch`
预热的是编解码器注册过的东西，没有编解码器就没有东西可预热。

这处空白的代价比看上去小，因为它下面那一行是填满的。编解码器买来的那个*结果* - 处理器面对普通类型、
签名里不出现传输格式 - Protobuf 走另一条路也能拿到，见 [Protobuf](#protobuf)。它拿不到的是预取那套
机制，而那套机制正挂在编解码器这个位置上。

**为什么预取这几行写的是“编解码器”而不是“Avro”。** 那套机制里没有一样是 Avro 专有的。
`MissingSubject`、连接时的 subject 解析和启动时的兼容性检查都住在 `SchemaPrefetch` 上，作用于编解码
器注册过的一切，因此 `SchemaFramed` 下的 JSON 今天与 Avro 一样拿到它们 - 包括 `AutoRegister`，它
放回去的是由 `schemars` 导出的 JSON Schema。

**Protobuf 在发布发生的地方回答同一个问题，而不是用策略。** 加信封时解析的是目的主题的 subject，
而注册表描述不了的目的地，应用级的层放行、挂载点的策略拒绝。这个差别是刻意的，讲在
[一次设定，全应用生效](#setting-it-once-for-a-whole-app)里，所以这里没有 `MissingSubject` 要补。

**两处真正的缺口，都在 Protobuf，也都是同一件工作。** Protobuf 类型交不出自己的 schema，因此服务
要把自己的 `.proto` 写两遍 - 一遍是 `prost-build` 编译的那个文件，一遍是用来注册的字符串字面量 -
两份副本之间没有任何东西把它们绑在一起，这里也因此没有任何东西能从类型注册一个 Protobuf schema。
而且这里没有任何东西解析注册表里的 schema 引用，因此一个 `.proto` 只要 import 了已编译的 pool 里还
没有的东西，
就够不着：`google/protobuf/*` 这些标准类型能解析，`confluent/*`（注册表自己视为随处可用）不能，
你自己的任何 import 则需要 `references` 字段，而本 crate 从不写也从不读它。

## 编解码器 { #the-codec }

schema 的来源是编解码器的一部分，来源有两种。

`AvroCodec::local(schema)` 钉住一个 schema：传输上是裸数据，没有信封、没有注册表，整条路径上也没有
任何 I/O - 适合固定 schema 的主题，以及每一个单元测试。

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_avro_codec.rs:local"
```

`AvroCodec::registry(&prefetch)` 说的是 Confluent 传输格式，而 `register::<T>(subject)` 说明它发布
什么：编码时给每个值套上它那个类型的 subject id 作信封，解码时用该次投递信封所指的写入端 schema 来
读 - 因此还停在旧版本上的生产者依然可读。

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_avro_codec.rs:wiring"
```

`SchemaPrefetch` 是异步的那一半，它之所以存在，是因为 `Codec` 两端都是同步的，而查一次注册表不是。
这场会面没法安排在 `encode` 或 `decode` 里：从同步函数里阻塞一个运行时工作线程不是选项，猜 schema
则是数据损坏。于是查询搬到了两个本来就异步、本来就知道该在何时发生的地方 - Broker 的 `connect`，
负责编解码器发布时所用的 subject；以及投递路径，负责到来的信封所指的写入端 schema。因此不存在的
subject 在应用启动期间就有定论，而不是拖到第一次发布；预取解析不出来的 id 则成为一次解码失败，由
订阅的失败策略结算，而绝不是一次无声的猜测。

### 注册只关乎发布侧 { #registration-is-the-publish-side-only }

**一个编解码器，路由器经由它挂载多少消息类型都行。** 注册说明一个类型的值套在哪个 subject 下，
而解码一点也不需要这些：写入端 schema 来自信封里的 id，因此带着五种类型的订阅，经由一个一种都没被
告知的编解码器把五种全都解出来。这种不对称正是一个编解码器能服务整个作用域的原因；它也解释了为什么
`register` 住在故事的发布那一半，而消费那一半没有与之对应的东西。

subject 每次发布都要查一次，键是 **serde 给这个值的类型所起的名字**。显而易见的键本该是 `TypeId`，
但它用不了：它需要 `T: 'static`，而 `Codec::encode` 只用 `Serialize` 约束 `T`。与此同时，每个派生
出来的 `Serialize` 在碰第一个字段之前就把名字交给 `serialize_struct`，因此一个探测用的序列化器读到
名字就停下。这个键从道理上也是对的：它就是 `AvroSchema` 写进 Avro 记录的那个名字，而注册在只有类型
没有值时读的也正是它。`std::any::type_name` 是刻意不用的：标准库既不保证它唯一，也不保证它稳定。

两个 serde 名字相同的注册类型会让发布产生歧义，因此这种情况**在注册时就被拒绝**，早于应用运行。
给其中一个另起一个 `#[serde(rename = "..")]`。

剩下的代价是：你忘了注册的类型，会在**它第一次发布时成为运行期错误**，错误里点出这个类型，并列出
编解码器确实持有的那些。把它做成编译错误，就意味着编解码器的类型里要带上它的消息清单，而那正是
“每个挂载点一个类型参数”的形态 - 这套设计的存在就是为了避开它。

### 读取端 schema 是调优，不是必需 { #a-reader-schema-is-tuning-not-a-requirement }

`resolve_onto(schema)` 打开 Avro 自己的 schema 解析，于是写入端从来没有过的字段，由读取端 schema
的默认值补上。它作用于这个编解码器解码的**每一次**投递 - 解码只有类型没有值，因此没有名字可以用来
按类型区分读取端 schema - 这使它成为一个只读一种类型的编解码器才该有的设置。服务多种类型的编解码器
改用 Rust 这一侧的 `#[serde(default)]`，那能逐字段覆盖同样的场景。

### subject 不在了的时候 { #when-the-subject-is-gone }

生产者还在运行时，可能有人把 subject 从注册表里删掉，因此这里的反应是一条策略，而不是一个写死的答案：
`SchemaPrefetch::on_missing_subject`，一个默认值为 **`Refuse`** 的 enum。把“在别人的注册表里建
subject”当成启动的副作用，比干脆不启动更糟。`AutoRegister` 把这个类型自己的 schema 放回去并打警告，
`PublishUnframed` 写裸数据并打警告。每条警告都点出 subject、格式和 schema，因为光一句“schema 不见了”
对运维什么也没说。

这套词汇是刻意照搬 Confluent 的。他们的序列化器用三个设置覆盖这片地方，其中两个描述的是本 crate 的
常规路径而不是这条策略：编码永远用信封里 id 所指的 schema 来写，也就是 `use.latest.version`，而
`check_compatibility` 就是 `latest.compatibility.strict`。留给这个 enum 的，是那两者合起来回答的
问题：subject 根本不在时，生产者是创建它（`AutoRegister`，他们的 `auto.register.schemas=true`）、
拒绝（`Refuse`，他们的 `auto.register.schemas=false` 配上 `use.latest.version=true`），还是不带
信封凑合（`PublishUnframed`，Confluent 没有对应项）。它是一个 enum 而不是三个布尔值，因为这些布尔
值并不独立 - 自动注册开着时 `use.latest.version` 毫无意义 - 而毫无意义的组合正是 enum 让它无法表示
的东西。

`check_compatibility` 默认开启，Confluent 那边也是如此，它补上了早先“subject 挂在类型上”那套设计
补不上的缺口：在 `connect` 时，每个注册类型的 schema 都会与它的 subject 已经持有的版本对照，漂移了
的模型会带着注册表自己给出的差异说明（细到字段）把应用拦在启动阶段，而不是日后表现为一个读不懂别人
所写内容的消费者。

两种删除并不相同，这一点是在真实注册表上验证过的，不是想当然。**软**删除把 subject 藏起来 - 它的
`versions/latest` 回 404 - 而 `GET /schemas/ids/{id}` 仍然返回 schema，因此**消费者照常工作**，卡住
的只有生产者。**永久**删除连 id 一起移除，此后指着这个 id 的记录再没有任何东西能解码：这里没有哪条
策略帮得了消费者，因为这个 schema 对所有人都没了。事后重新注册会铸出一个*新*的 id，因此已经躺在主题
里的记录仍然读不出来。

编解码器这条路与 `SchemaFrame` 在这里分道扬镳，而且是刻意的。转码那一层把注册表不认识的 subject 当作
“这个主题不是注册表支持的”，原样发布，因为在那边这个条件确实有歧义：它为应用发布到的每个主题解析
subject，而其中大多数根本不由注册表支持。在编解码器这条路上歧义没了：写下
`register::<Order>("orders-value")` 就是在*声明*这个主题由注册表支持，因此缺失的 subject 是异常而不
是一个普通主题，默认值也就这么说。

### JSON 是同样的形态 { #json-takes-the-same-shape }

`SchemaFramed::new(&prefetch, JsonCodec).register::<Order>("orders-value")` 是同一个构建器，架在同一
个按名字索引的映射上，理由也相同。注册经由 `schemars` 捕获这个类型的 JSON Schema，`AutoRegister` 因
此有东西可以放回去。

### 注册表只指定一次 { #naming-the-registry-once }

在一个应用里，注册表只指定两次，而且从不在挂载点上：一次是构建预取时，一次是把它挂到 Broker 上时。
每个编解码器都由这唯一的预取铸出，因此逐个挂载点变化的是 subject - 而那本来就是各处真正不同的东西。

作用域这件事用的是核心自己的编解码器层叠，它已经做到了 schema 所需要的：为一个 Broker 作用域设定的
编解码器覆盖其中每个处理器，而挂载在里面的路由器为它所带的那些处理器覆盖它。最具体的胜出，与任何
别的编解码器完全一样。

现在一个编解码器服务整个作用域，因此覆盖是留给那些没法共享的设置的。最清楚的例子是读取端 schema：
它作用于它那个编解码器解码的每一次投递，因此带着它的编解码器只能服务单一的读取类型，而需要 Avro
解析的那个处理器就在自己的路由器里拿到自己的编解码器，作用域那个则继续服务其余处理器。发布到另一个
注册表、或者换一套 `MissingSubject` 策略的子树，也用同样的方式铸出来。

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_avro_codec.rs:cascade"
```

没有应用一级的层次，因为核心把编解码器的作用域定在 Broker 和路由器上，何况注册表本来就是按集群来的
东西。核心不提供的是*部分*覆盖 - “拿作用域的编解码器，只改其中一个设置” - 因为对它来说编解码器是一
个不透明的值。铸出第二个编解码器的那次预取就是这种覆盖，代价是一行代码。

### schema 演进 { #schema-evolution }

用写入端 schema 读一份数据，恢复出来的就是写入端写下的东西，不多不少。写入端从来没有过的字段，由
*读取端* schema 的默认值补上，这就是 Avro 自己的 schema 解析：`resolve_onto(schema)` 指定这个消费者
期待的 schema。不用它，带着写入端所没有字段的模型会反序列化失败，而这是诚实的结果 - 那个值确实不在
传输内容里。

### 信封下的 JSON { #json-under-the-envelope }

`SchemaFramed::new(&prefetch, JsonCodec)` 是 JSON 的注册表编解码器。信封在这里是可分离的，因为一份
JSON 文档是自描述的：id 说明它声称符合哪个 schema，而没有 id 文档照样解析得出来。一份 Avro 数据离开
它 id 所指的 schema 就读不了，`AvroCodec` 因此自己拥有信封，而不是搭在这层包装上；两者之间的界线正
是这个性质，而不是两种格式之分。

这层包装只加信封，不做校验。注册表里的 JSON Schema 是注册表在版本之间执行的兼容性约定，拿每条消息去
对照它，就意味着要携带一个 JSON Schema 校验器，并为每次投递付出代价 - Confluent 自己的序列化器也因
此把这一步做成可选。校验属于内层编解码器，而内层编解码器就在调用点指定：传一个会校验的进去，别传
普通的那个。

### 客户端记住什么 { #what-the-client-remembers }

`SchemaCachePolicy` 跟随的是注册表的一个性质，而不是某种偏好：**schema id 不可变**。id 按内容为每一
份不同的 schema 定义全局分配一次，因此 subject 的新版本铸出新的 id，旧 id 永远解析到旧 schema；而
subject 的*最新版本*，只要有人注册新版本就会挪动。

所以这两半需要相反的对待。按 id 索引的条目需要容量上限、不需要过期 - 给它们加 TTL 只会引来一次取回
同样字节的重新请求 - 而上限之所以要紧，是因为一个消费者遇到的 id 数量等于写入端的版本数，健康的拓扑
里很少，坏掉的拓扑里没有上界。按 subject 索引的条目需要过期、不需要容量上限。Confluent 自己的客户端
出于同样的理由，把 `latest.cache.ttl.sec` 的作用范围限定在最新版本的缓存上。
`SchemaCachePolicy::Disabled` 彻底关掉缓存 - 它是一个真正的配置，而不是伪装成零 TTL，其后果是：同步
的那些编解码器要读缓存、又不能 await 一次未命中，因此在它之下无法工作。

## 客户端 { #the-client }

`SchemaRegistry::new` 记下 URL，在第一次查询之前不发任何请求。你可以在客户端上设置 basic 或 bearer
认证，HTTPS 走 rustls。客户端的每一份克隆读写同一个缓存，因此一个 schema id 或一个 subject 在一个
进程里只走一次网络。

每个注册表请求都带一个期限，默认十秒，用 `request_timeout` 改。一次投递或一次发布要等这次查询，
因此这个期限限定的正是那种“接了连接然后沉默”的注册表。期限到了请求返回错误，两侧都把它当作任何别的
注册表错误来处理。

## Protobuf { #protobuf }

生成出来的消息以它自己的样子到来，也以它自己的样子离开。处理器就是普通类型之上的普通函数，与它在
Avro 编解码器上一模一样：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf_plain.rs:handler"
```

传输格式由类型自己带着，因为 `prost` 消息不是 serde 类型，永远进不了编解码器的位置。它改走核心的
字节路径：那些路径按类型挑选，并且只留给*不是* serde 类型的类型 - `#[wire(prost)]` 对 `prost` 消息
能用、而对等的 `#[wire(avro)]` 不可能存在，原因就在这里：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf_plain.rs:types"
```

两半是不对称的，而知道为什么，能让这套形态读起来是一件事而不是两件。**读取不需要注册表**，因此它可以
发生在类型里：Protobuf 的兼容性模型就是它传输格式自身的模型 - 字段按 tag 寻址，读取端不认识的字段
保留为未知字段，写入端从没写过的取默认值 - 因此一次投递就着读取端自己的生成类型解码，而信封摆在消息
前面的一切（魔数字节、id、消息索引路径）都不用问任何人就能解析。**写入需要一个数字**，也就是 subject
的 schema id，而一个值取不到它：`Serialized::wire_bytes` 是同步的，手上只有 `&self`。发布路径取得到，
因为它知道目的主题，而目的主题正是 subject 的命名依据。

于是这个类型把自己的传输路径一分为二 - `prost` 写消息，本 crate 读信封 - 而 id 和索引路径由回复自己
的发布者在出去的路上加上：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf_plain.rs:wiring"
```

处理器只返回它的回复，别的什么都没有：没有槽位参数、没有 `publish().await`、函数体里也没有错误分支。
回复类型自己也不带目的地，因为地址来自 `publish("confirmations")` 子句；它需要的只是
`#[derive(Serialized)]` 和 `#[wire(..)]` 的编码那一半。注册表只指定一次，在挂载点的策略里。

`KafkaPublish::framed(&registry)` 在任何指定发布策略的地方都能用，`Out` 槽位也算，因此发布多条消息的
处理器给它们加的信封是一样的。

### 为整个应用设一次 { #setting-it-once-for-a-whole-app }

`ProtobufFrame` 是同一套信封的 `publish_layer` 形态，给那些否则要在每个挂载点重复写策略的应用用：

<!-- inline-rust: one line of wiring; the compiled example shows the mount-site form, which is the one to reach for first -->
```rust
RustStream::new(info).publish_layer(ProtobufFrame::new(registry))
```

只有目的主题的 subject 已注册**并且持有一个 Protobuf schema** 时，它才加信封，其余一律原样放行 -
没有 subject 的主题，以及 subject 是 Avro 或 JSON 的主题。正是这一点，让一个应用在全局装上这层的
同时，还能同时带着 Avro 编解码器、JSON 编解码器和 Protobuf；这层宽松而策略严格，原因也在这里：这层
看到的是应用发布到的每一个主题，其中大多数不是 Protobuf，而在挂载点上指定策略是在*声明*那一个目的地
由注册表支持，因此那里缺失的 subject 是异常，发布失败。编解码器那条路上，`MissingSubject` 的 `Refuse`
默认值讲的是同一个道理。

这层做不到一件事：**它永远看不到 `publish(..)` 的回复。** 核心刻意把逐字节的回复直接路由给与它配对
的发布者，好让一个拥有自身字节的值原样发出 - 回复那套形态因此接收策略，而这层覆盖的是经由槽位、以及
经由挂载点从未指定过的发布者发出的那些发布。两者是叠加而不是冲突：谁先跑谁给载荷加信封，另一个发现
信封已经在那里，就不再动它。

已经带着信封的载荷在两条路上都原样通过，因此一条带着信封进来、又接着出去的消息不会加两次信封。
这个判断是精确的而不是猜的，因为一个裸的 `prost` 消息以一个字段 tag 开头，其字段号至少是 1，所以它的
第一个字节绝不会是那个值为零的魔数字节。

两者都不是 `SchemaFrame` 的同伴：那一层的约定是“这份载荷是一个 JSON 文档，把它转码成 subject 的格式”，
这两者说的是“这份载荷已经是数据本身，给它套上信封”。

### schema 里的哪个消息 { #which-message-of-the-schema }

信封里的索引路径说明写下的是哪个消息，因此它必须就是发布类型实际所是的那一个。策略和这一层默认都取
schema 里第一个顶层消息，那是常见情形，也正是 Confluent 优化成单个零字节的那一种，因此只有一个消息的
`.proto` 什么都不用写。其余情形用 `.message(topic, "pkg.Message")` 钉住，这也是 `SchemaFrame` 本来就
接受的那个调用。

subject 在第一次发布到每个目的地时解析，此后一直缓存。它不可能发生在启动时：策略与已连接的 Broker
配对时本可以做 I/O，但目的主题在那里还不知道 - 它来自挂载处的 `publish(..)` 子句，或者来自一次槽位
发布的调用点，而这两样都不会交给策略。因此缺失的 subject 表现为一次失败的发布，由处理器的失败策略
结算。

## 消费：入站转码 { #consuming-transcode-on-the-way-in }

`KafkaBroker::schema_registry(registry)` 把客户端放在消费这一边：该 Broker 的每条订阅，都在载荷到达
编解码器之前，把带信封的投递转换成普通 JSON。JSON Schema 的载荷留下字节、去掉信封。Avro 或 Protobuf
的数据，在该格式的 feature 打开时，经由它信封所指的注册表 schema 转换。此后处理器就是默认编解码器上
的普通订阅者：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_schema_registry.rs:handler"
```

不带信封的载荷原样通过，因此混着带信封和不带信封记录的主题照样工作。中间件转换不了的投递，会带着一条
警告不经转码地通过：要么 schema 查询返回了错误，要么该格式的 feature 是关的。这次投递之后如何，由
订阅者的解码失败策略决定。

## 发布：出站加信封 { #publishing-frame-on-the-way-out }

`SchemaFrame` 是发布中间件；用 `RustStream::publish_layer` 把它加到整个应用上。对每一次走应用管线的
发布，它解析目的主题的 subject，并把普通 JSON 载荷按该 subject **注册时的格式**加上信封：JSON Schema
的 subject 把字节留在信封下，Avro 或 Protobuf 的 subject 则做转换。一个主题的传输格式由注册表决定，
没有哪个发布者去声明它。

subject 默认遵循 Confluent 的 `TopicName` 策略，也就是 `{topic}-value`。用 `subject_strategy` 改这套
映射，或者用 `subject(topic, subject)` 钉住某个主题的 subject。`RecordName` 和 `TopicRecordName` 两种
策略按记录类型给 subject 命名，而发布路径并不传递记录类型，因此 subject 会是空的，或者以一个短横线
结尾。这两种都需要逐主题钉住 subject。

注册表不认识其 subject 的主题原样发布，因此一个应用不用配置就能同时服务注册表支持的主题和普通主题。
`SchemaFrame` 记住这个未注册的 subject 并只记一次日志；事后注册的 subject，要在重启或一次显式的
`warm` 之后才生效。

加不上信封的发布返回错误：要么 subject 查询返回了错误，要么 subject 的 schema 拒收了这份载荷。此时
发布的处理器对它那次投递 nack 以请求重试，因此不会有加错信封的记录进入主题。

subject 在**第一次发布到它的主题时惰性解析**，因此 subject 在注册表里已经存在的服务不需要启动步骤。
拥有自己 schema 的生产者，在启动时直接从消息类型注册它们：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_schema_registry.rs:types"
```

回复类型声明目的地：`#[outgoing(name = "confirmations")]` 指定主题，订阅者写不带名字的 `publish`。

`register` 提交一份你自己写的定义，而完全相同的 schema 保留它已有的 id。`register_json::<T>` 经由
schemars 从类型导出 JSON Schema，本 crate 把 schemars 重新导出。`warm` 不注册任何东西：它解析一个
已有的 subject 并缓存下来，用于生产者不得创建 schema 的部署（`auto.register.schemas` 关闭）。

运行时配对出来的每个发布者都经由应用的管线发布，因此回复、事务发布者、按分区的发布者和精确一次管线
都会加上信封，在 include 处不用配置任何东西。你自己从已连接 Broker 配出来的发布者在管线之外，给它
什么就发什么。

## 转码路径上的各种格式 { #formats-on-the-transcoding-path }

- **JSON**（只要 `schema-registry`，用默认的 `json` 编解码器）：信封加上去又摘下来，文档本身原封
  不动。文档不会与注册的 schema 对照，因此实际的约定是处理器的类型。
- **Avro**（`avro` feature）：数据在两个边缘上经由注册表 schema 转换。`register_avro::<T>` 从类型
  导出 schema，`AvroSchema` 派生宏也重新导出了：

    ```rust
    --8<-- "crates/ruststream-rdkafka/examples/kafka_avro.rs:wiring"
    ```
- **Protobuf**（`protobuf` feature）：消息经由从注册表 `.proto` 源码编译出的描述符，在 JSON 与
  Protobuf 之间来回转换。标准类型可用；超出它们的注册表 schema 引用不解析。消息索引在两个边缘上都
  读写，因此嵌套的、含多个消息的 schema 都能工作。出站消息取 schema 的第一个顶层消息；用
  `SchemaFrame::message("topic", "pkg.Message")` 可以逐主题指定另一个：

    ```rust
    --8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf.rs:wiring"
    ```

转码的取舍：注册表主题上每条消息多走一趟 JSON，换来的是统一的处理器模型 - 同一个结构体、同一个编解
码器、任意传输格式 - 而放弃的是 JSON 文档承载不了的东西。JSON 没有对应形状的 Avro 类型熬不过这一趟；
而在 subject 旧版本下写入的数据只做解码、不做解析：处理器看到的是写入端的字段，没有读取端 schema
来补上生产者从未写过的部分。服务必须在注册表支持的主题上保持普通 serde 模型时，走这条路；其余情况走
编解码器。

## 测试 Protobuf 处理器 { #testing-a-protobuf-handler }

Protobuf 处理器就是普通处理器，因此 `TestApp` 和进程内的 `KafkaTestBroker` 不用集群、也不用注册表
就能驱动它，因为读取不需要注册表。灌进去的那条记录带着注册表支持的生产者会写的信封，而处理器看不到它：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf_testing.rs:testapp"
```

给回复加信封是需要注册表的那一半，因此在进程内回复是裸着出去的，断言看的是消息而不是信封。对着集群
时，挂载点指定的是 `KafkaPublish::framed(&registry)`。

不启用 `macros` feature 时，同一个处理器是同样两条轴之上的一个 `Handle` 实现 - 生成的消息进、生成的
消息出 - 挂载指定订阅和回复的目的地：

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf_testing.rs:manual"
```

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf_testing.rs:mount"
```
