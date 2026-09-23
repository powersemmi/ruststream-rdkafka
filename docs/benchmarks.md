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

The best of three interleaved rounds, with the median round in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Adapter", "framework": "RustStream", "adapterOverhead": "Adapter overhead", "overhead": "Overhead", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "roundTrip": "Round trip", "build": "Build", "versions": "Versions", "measured": "Measured", "instructions": "Instructions per message", "allocations": "Allocations per message", "cold": "Cold start (instructions / allocations)", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

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

## The crate's own code

<div id="benchmark-code"></div>

The second table is what a message costs on the service's thread, counted rather than timed:
instructions under callgrind and allocations under DHAT. Each scenario is the service a user
writes, on `KafkaBroker` against the same stand: a consumer group reads a topic of one partition,
and another thread fills the topic before the measured region starts. Everything the service's
thread does is counted: the framework's dispatch, this crate's code and the librdkafka calls the
crate makes on that thread. What librdkafka does on its own threads is not counted: fetching, the
group protocol, producing records and the delivery reports. A reply's round trip to the broker is
therefore not in its row, while the work the service's thread does to send the reply and take its
delivery report is.

Instructions and allocations are per message in the steady state: the slope between a run of 1000
deliveries and a run of 2000. The last column is what starting the service, joining the consumer
group and taking the first delivery cost once. The numbers are absolute, the framework's own cost
included; the core publishes that cost alone on its
[benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).

The broker is real, so a count moves a little between runs of one binary: how often the service's
thread finds librdkafka's queue empty, or waits for a delivery report, depends on timing, and every
wait costs a wakeup. Three runs of one binary made exactly the same allocations and agreed on
instructions per message within a hundredth of a percent for consume and batch; a reply moved by up
to three percent. `just bench-code` fails on an allocation above the floor a scenario declares (the
highest count of three runs plus 0.1 percent), and on more than five percent more instructions than
the run it compares with: the previous run, or `main` with `--baseline=main`. A pull request that
changes the cost cites its numbers.

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
each answer a different question, and the comparison measures none of them. The code table counts a
batch handler, on the same stand.

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

```bash
just bench-code
```

The recipe starts the same stand, counts the code table under valgrind, stops the stand and
rewrites the `code` section of the same document. It takes a minute or two and needs valgrind and
the benchmark runner: `cargo install --locked gungraun-runner --version =0.19.4`.
