---
title: Methodology
description: "How ruststream-rdkafka measures itself: the three loops, the run on a real broker, the broker-bound arithmetic and the code count."
---

# Methodology

How `ruststream-rdkafka` measures itself: the three loops, the run on a real broker, the
broker-bound arithmetic and the code count. The procedure is the framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)),
and this page states what it is on Kafka.

## The three loops

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the records.

- **Raw client.** An rdkafka `StreamConsumer`, driven directly: receive, decode, store the offset.
- **Broker crate.** This crate and nothing above it: the broker connects, the topic descriptor opens
  a subscription, and a loop pulls deliveries off its stream, decodes them and acks them.
- **RustStream.** The service a user writes: a `#[subscriber]` handler and the app around it.

Everything else is equal across the three: the consumer properties, the consumer group, the commit
mode, the place of the ack, the decode into the same struct, the payload bytes, the tokio runtime
with four worker threads, and the binary.

The body is 512 bytes of JSON. It decodes into a struct of two integer fields, so the decode
allocates nothing and the number is about this crate rather than about the JSON parser.

## The run

- **A fresh topic and a fresh consumer group per run.** The topic has one partition, and it is
  dropped after the run, so a run never sees what an earlier one left.
- **The consumer joins first.** The producer, on a client of its own, starts once the cluster
  reports the group's member.
- **The window opens at the first delivery and closes when the last one is decoded**, in all three
  loops. Joining the group and creating the topic are outside it.
- **The message count comes from a probe**, which measures the raw loop's rate, so a measured run
  lasts at least five seconds on any machine.
- **The loops are interleaved**: raw, crate, RustStream, three rounds. Each loop reports its best,
  median and worst round.

A row is `broker-bound` when the raw client spent the run waiting on the socket. That is
arithmetic: a probe times one metadata request and its answer, a second probe counts the requests
librdkafka sends per delivery, and the row is flagged when their product reaches half the measured
time per message. The round trip is published with the machine, so the arithmetic can be checked.

## The code count

Each scenario of the [cost chart](index.md#cost-of-the-code) is the service a user writes, on
`KafkaBroker` against the same broker, built without the `testing` feature. A consumer group reads a
topic of one partition, and another thread fills the topic before the measured region opens.

Instructions come from callgrind and allocations from DHAT. A scenario runs over one delivery,
then 1000, then 2000. The difference between the last two is the steady cost of a message, and the
single delivery is the cold start.

The broker is real, so a count moves between runs of one binary: how often the service's thread
finds the queue empty and waits depends on timing. Across three runs, consume and batch agreed
within a hundredth of a percent and the reply moved by 2.3 percent. So a run against a saved
baseline fails on five percent more instructions in a scenario, where the core's limit is two.
Allocations are held to a floor each scenario declares, on every run.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs both comparison scenarios, stops
the stand and rewrites `docs/benchmarks/results.json`. It takes about fifteen minutes and wants the
machine to itself. It clears `RUSTFLAGS`, so the binary is not tied to the CPU it was built on.

```bash
just bench-code
```

The code count takes a minute or two and needs valgrind. `just bench-code --save-baseline=main` on
`main` records a baseline, and `just bench-code --baseline=main` on a branch compares with it.
