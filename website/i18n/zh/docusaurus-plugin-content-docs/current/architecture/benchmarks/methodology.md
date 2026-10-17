---
title: 方法论
description: "ruststream-rdkafka 如何测量自己：三个循环、在真实 Broker 上的运行、受 Broker 限制的判定计算，以及代码计数。"
---

# 方法论

`ruststream-rdkafka` 如何测量自己：三个循环、在真实 Broker 上的运行、受 Broker 限制的判定计算，以及代码计数。流程就是框架自己的流程（[RustStream 方法论](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)），本页说明它在 Kafka 上是什么样子。

## 三个循环 {/* #the-three-loops */}

一个进程把每个场景跑三遍，作为三个循环，它们只有一处不同：承载记录的是什么。

- **裸客户端。** 一个 rdkafka `StreamConsumer`，直接驱动：接收、解码、存储偏移量。
- **Broker crate。** 本 crate，上面什么也没有：Broker 建立连接，[主题描述符](../../components/subscriptions/kafka-topic.md)打开一个订阅，一个循环从它的流中取出投递、解码并 ack。
- **RustStream。** 用户写的服务：一个 `#[subscriber]` 处理器及其外围的应用。

其余一切在三个循环中都相同：消费者属性、消费者组、[提交模式](../../components/settlement/index.md)、ack 的位置、解码成同一个结构体、载荷字节、带四个工作线程的 tokio 运行时，以及二进制程序。

消息体是 512 字节的 JSON。它解码成一个包含两个整数字段的结构体，因此解码不做内存分配，数字反映的是本 crate，而不是 JSON 解析器。

## 运行 {/* #the-run */}

- **每次运行使用新的主题和新的消费者组。** 主题只有一个分区，运行结束后删除，因此一次运行永远看不到之前的运行留下的东西。
- **消费者先加入。** 生产者使用自己的客户端，在集群报告出组的成员之后才开始。
- **在三个循环中，计时窗口都从第一次投递开始，到最后一次投递解码完成时结束。** 加入组和创建主题都在窗口之外。
- **消息条数来自一次探测**，它测量裸循环的速率，因此在任何机器上，测量的运行都至少持续五秒。
- **各循环交替进行**：裸、crate、RustStream，共三轮。每个循环报告它最佳、中位和最差的一轮。

当裸客户端在运行中一直在等套接字时，该行标为 `受 Broker 限制`。这是算出来的：一次探测为一个元数据请求及其应答计时，第二次探测统计 librdkafka 每次投递发送的请求数，二者之积达到每条消息测得时间的一半时，该行就被标记。往返时间与机器信息一起公布，因此这个计算可以核对。

## 代码计数 {/* #the-code-count */}

[开销图](index.md#cost-of-the-code)的每个场景都是用户写的服务，基于 [`KafkaBroker`](../../components/broker/index.md)，连接同一个 Broker，构建时不带 `testing` 特性。一个消费者组读取一个只有一个分区的主题，另一个线程在测量区间打开之前把主题填满。

指令数来自 callgrind，内存分配来自 DHAT。一个场景依次在一次投递、1000 次、2000 次上运行。后两次之差是一条消息的稳态开销，单次投递就是冷启动。

Broker 是真实的，所以同一个二进制程序每次运行的计数都会变动：服务线程有多少次发现队列为空并等待，取决于时序。三次运行中，消费场景和批场景的结果相差在万分之一以内，应答场景变动了 2.3%。因此，与保存的基线对照时，某个场景的指令数多出百分之五就算失败，而核心的上限是百分之二。内存分配在每次运行中都受每个场景声明的下限约束。

## 自己运行 {/* #running-it-yourself */}

```bash
just bench
```

这条命令按 `docker-compose.test.yml` 启动测试环境，运行两个对照场景，停止测试环境，并重写 `docs/benchmarks/results.json`。它大约需要十五分钟，并且需要独占机器。它会清空 `RUSTFLAGS`，因此二进制程序不会绑定到构建它的 CPU 上。两条命令都按发布版本的方式构建基准测试，跨 crate 启用完整的链接时优化（fat LTO），因此这些数字描述的正是服务实际使用的代码。

```bash
just bench-code
```

代码计数需要一两分钟，并且需要 valgrind。在 `main` 上运行 `just bench-code --save-baseline=main` 记录基线，在分支上运行 `just bench-code --baseline=main` 与它对照。
