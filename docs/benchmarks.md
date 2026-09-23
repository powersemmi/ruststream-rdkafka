# Benchmarks

Everything between the Kafka client and your code costs time on every message: the subscription
stream, the owned delivery, the decode, the dispatch, the ack. This page says how much, measured
against the same work written by hand on `rdkafka`.

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the deliveries. Everything else is held equal - the consumer properties, the consumer
group, the commit mode, the position of the ack, the decode into the same type, the payload bytes,
the tokio runtime and the build. The procedure is the framework's own and is described on the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

## The numbers

The best of three interleaved rounds, with the slowest round in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Adapter", "framework": "RustStream", "adapterOverhead": "Adapter overhead", "overhead": "Overhead", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "roundTrip": "Round trip", "build": "Build", "versions": "Versions", "measured": "Measured", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is
a copy that could have gone stale.

The three measured columns are three loops over the same scenario. `Raw client` drives a
librdkafka consumer directly, receiving, decoding and storing the offset in a loop of its own.
`Adapter` drives this crate and nothing above it: the broker connects, the topic descriptor opens
a subscription, and a loop pulls deliveries off the stream it yields, decodes them and acks them.
`RustStream` is the service a user writes, with a `#[subscriber]` handler and the app around it.

`Adapter overhead` is therefore what this crate's own consumer costs over the client it wraps, and
the gap between it and `Overhead` is what the runtime costs on top. The first figure is this
repository's to answer for. The second belongs to the core, and is published here because how the
runtime meets a transport that delivers in fetched batches is a fact about this crate; the core
publishes the runtime's own cost, in instructions per message, on its own page.

Both rows are one consumer reading one topic through one consumer group; they differ in what an ack
means. Under auto-commit librdkafka owns the committed position and an ack is advisory. Under
`Commit::Tracked` every ack stores an offset, over a watermark that never runs past a delivery
still in flight.

What the adapter column costs is mostly the price of an owned delivery. This crate hands out a
message that owns itself - the body, the headers, the topic and the partition - so a delivery is
copied out of the client's buffer before anything reads it, while the hand-written loop decodes in
place from the buffer librdkafka lends it and copies nothing. That is the trade the crate makes: an
owned delivery outlives the poll loop, which is what lets code await, spread work over worker lanes
and acknowledge later.

A row marked `broker-bound` is one where the raw client spent the run waiting on the socket. That
is decided by arithmetic, not by impression: a probe outside the loops times one request and its
answer, a second probe counts the requests librdkafka actually sends per delivery, and the row is
flagged when their product reaches half the measured time per message. The round-trip time it
measured is published below, so the arithmetic can be checked.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-rdkafka/latest/benchmarks/results.json).

## The machine

<div id="benchmark-environment"></div>

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one consumer, one topic of one partition, a small body and a single-node cluster on the
loopback. It measures what a delivery costs inside this crate, not what Kafka can carry, and a row
here is not comparable with a row published for another broker: the transports do different work
per message.

The window a run measures opens at the first delivery and closes when the last one has been
decoded, in all three loops alike. The offset is stored after that point, so one stored offset out
of the millions a run carries sits outside the number everywhere.

A topic of several partitions, a pool of worker lanes, a batch handler and a transactional pipeline
each answer a different question, and none of them is measured here.

The numbers are a snapshot of one machine on one day. They are re-measured by hand, on a machine
given to the run alone: the difference this page is about is smaller than the noise of a shared one.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs both scenarios, stops the stand
and rewrites `docs/benchmarks/results.json` with what it measured. It takes about fifteen minutes
and wants the machine to itself. The message count is not fixed: a probe run sets it so that every
measured run lasts at least five seconds on whatever machine it is taken on.
