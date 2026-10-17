---
title: 基准测试
description: "ruststream-rdkafka 在每条消息上的开销：与裸 rdkafka 客户端对照的吞吐量，以及它自身代码的指令数和内存分配次数。"
---

import {KafkaEnvironment, KafkaResults, MovedSections} from '@site/src/components/Benchmarks';

export const labels = {"loading": "正在读取结果...", "scenario": "场景", "raw": "裸客户端", "adapter": "Broker crate", "framework": "RustStream", "overhead": "额外开销", "against": "（比客户端 {percent}）", "indistinguishable": "无法区分", "brokerBound": "受 Broker 限制", "measured": "测量于", "details": "这些数字是怎样测得的", "pending": "尚未公布结果：{brokers}。", "instructions": "指令数", "allocations": "内存分配", "cold": "冷启动", "table": "以表格显示数字", "axis": "各循环比裸客户端最佳一轮低多少", "rounds": "各轮，从最佳到最慢", "best": "最佳一轮", "median": "中位一轮", "noise": "轮次的波动范围", "comparison": "与裸客户端对照", "code": "代码的开销", "broker": "Broker", "plugin": "{plugin} 插件", "pluginRaw": "不用插件", "pluginAdapter": "手写的插件", "pluginFramework": "crate 自带的插件", "pluginAxis": "各循环比不用插件的应用最佳一轮低多少", "pluginAgainst": "（比不用插件 {percent}）", "pluginOverhead": "比手写插件", "pluginOverHand": "（比手写插件 {percent}）"};

export const machine = {"cpu": "处理器", "architecture": "架构", "cpu_frequency": "频率", "cores": "核心数", "memory": "内存", "memory_speed": "内存速度", "os": "操作系统", "broker": "Broker", "rustc": "编译器", "profile": "构建配置", "features": "特性", "rustflags": "RUSTFLAGS", "round_trip": "往返时间", "valgrind": "Valgrind"};

# 基准测试

`ruststream-rdkafka` 在每条消息上的开销：与裸 rdkafka 客户端对照的吞吐量，以及它自身代码的指令数和内存分配次数。

## 与裸客户端对照 {/* #against-a-raw-client */}

第一张图在真实的 Kafka Broker 上把同一个场景用三种方式运行。`裸客户端` 直接驱动一个 librdkafka 消费者。`Broker crate` 用一个自己的循环驱动本 crate 的订阅，上面没有运行时。`RustStream` 是用户写的服务：应用里的一个 `#[subscriber]` 处理器。第一项差值是本 crate 比它所封装的客户端多付出的开销，第二项是运行时在其上增加的开销。

每个循环画成一条横条，从最佳一轮延伸到最慢的一轮，圆点标出最佳一轮，缺口标出中位一轮。坐标轴的零点是裸客户端的最佳一轮，圆点在零点右侧表示更慢。空心圆点表示该循环与裸客户端的差异小于各轮的波动范围。图表下方的表格列出最佳一轮，括号里是中位数。

两个场景的区别在于 ack 的含义。在自动提交下，librdkafka 在交出记录时就存储了偏移量，ack 只起建议作用。在 `Commit::Tracked` 下，每次 ack 都会存储一个偏移量，[结算](../settlement.md)一页用图说明了这一点。

## 代码的开销 {/* #cost-of-the-code */}

第二张图统计一条消息在服务线程上的开销：稳态下 callgrind 测得的指令数和 DHAT 测得的内存分配次数。每个场景都是一个基于 `KafkaBroker`、连接同一个 Broker 的服务。计数包括框架的分发、本 crate 的代码，以及在该线程上发起的 librdkafka 调用。拉取、组协议、生产和投递报告运行在 librdkafka 自己的线程上，不计入。`冷启动` 是启动服务、加入组并取得第一条记录一次性付出的开销。

## 结果 {/* #results */}

<KafkaResults labels={labels} details="/architecture/benchmarks/methodology/" />

## 机器 {/* #the-machine */}

<KafkaEnvironment labels={machine} />

数字来自本版本的 `docs/benchmarks/results.json`，由基准测试命令写入。[方法论](methodology.md)说明它们是怎样测得的，[数字](numbers.md)说明可以从中得出什么结论。

<MovedSections />
