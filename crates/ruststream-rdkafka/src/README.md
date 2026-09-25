Apache Kafka broker for the [RustStream](https://github.com/powersemmi/ruststream) messaging
framework, backed by [`rdkafka`](https://docs.rs/rdkafka) and librdkafka.

A subscription is one consumer joining one consumer group on one topic, and a publish is one
record on the topic the outgoing message names. Kafka settles by committed position rather
than per message, so what `ack` and `nack` mean here is a property of the subscription's
[`Commit`] mode. Everything this crate does not surface as a typed option is a librdkafka
property, reachable through the `config` passthroughs; an option left unset keeps the
librdkafka default.

The Confluent Schema Registry lives in [`schema_registry`], with the two formats that need
code of their own in [`avro`] and [`protobuf`]. A test runs the service's own app with the
broker connected in process ([Testing](#testing)).

# A service

A handler is an `async fn` over the decoded payload, mounted on a broker inside the
application object, and the attribute writes the `main`:

```
# #[cfg(feature = "json")]
# mod demo {
use ruststream_rdkafka::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
        |b| {
            b.include(handle);
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it. [`KafkaBroker::new`] only records configuration, which is what
lets the whole service fit the synchronous builder; the runtime climbs the ladder for it:

```text
KafkaBroker::new(servers)      configuration only, synchronous, no I/O
  .connect()   -> ConnectedKafkaBroker   subscriptions and live publishers hang off this
  .shutdown()  -> ClosedKafkaBroker      terminal witness, carrying unflushed_records()
```

Each state is its own type, so subscribing before the connect or publishing after the
shutdown does not compile. What stays dynamic is aliasing: a publisher paired before the
shutdown reports [`KafkaError::Closed`] rather than succeeding against a dead connection.

Connecting probes the cluster and validates the publish settings against it; the producer
itself is opened by the first publish, so a service that only consumes runs no producer client
and holds one connection rather than two.

# Subscribing

The attribute names the subscription, and the descriptor carries its settings:

| Form | What it reads |
|---|---|
| `#[subscriber("orders")]` | the topic `orders`, in the broker's [`default_group`](KafkaBroker::default_group) |
| `#[subscriber(KafkaTopic::new("orders").group("svc"))]` | one topic in one group |
| `#[subscriber(KafkaTopics::new(["orders-eu", "orders-us"]))]` | several topics through one consumer and one group |
| `#[subscriber(KafkaTopics::pattern("^orders-.*"))]` | every topic matching a librdkafka regex, which must start with `^` |
| `#[subscriber(KafkaPartitions::new("orders", [0, 2]))]` | exactly these partitions, joining no group and never rebalancing |

Kafka cannot read a topic without a consumer group, so a subscription that names none and
whose broker names no [`default_group`](KafkaBroker::default_group) fails at startup.
[`KafkaPartitions`] is the exception: a manual assignment joins no group, and a group there
only decides whether offsets are committed.

[`StartOffset`] says where a group starts when it holds no valid committed offset for a
partition, which is the case before its first commit and again after retention deleted one:
`Committed` (the default) leaves the choice to librdkafka's `auto.offset.reset`, `Earliest`
starts at the oldest retained record, `Latest` at the end of the log. [`Assignment`] picks the
group's rebalance strategy, and [`LaneKey`] what a worker lane is keyed by.

```
# #[cfg(feature = "json")]
# mod demo {
use ruststream_rdkafka::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Payment {
    id: u64,
}

/// One lane per record key, so payments of one account stay ordered while others run
/// concurrently.
#[subscriber(
    KafkaTopic::new("payments")
        .group("payments-svc")
        .commit(Commit::Tracked)
        .start(StartOffset::Earliest)
        .lane_key(LaneKey::RecordKey),
    workers(8, by_key)
)]
async fn charge(payment: &Payment) -> HandlerOutcome {
    if payment.id == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]),
        |b| {
            // Five deliveries of one payment, the first included; the sixth goes to the
            // dead-letter topic instead, payload and headers as they arrived.
            b.include(charge)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments.dlq");
        },
    )
}
# }
# fn main() {}
```

## Settlement

[`Commit`] decides what a settlement does, and it is the one setting to get right before
anything else:

- [`Commit::Auto`] (the default) is librdkafka's own behaviour: a position is stored as the
  record is handed to the application and committed on a timer, so `ack` and `nack(false)` are
  advisory no-ops, `nack(true)` reports `AckError::Unsupported` because nothing brings the
  record back, and a crash can lose the tail of processed work or skip records that were stored
  but never handled.
- [`Commit::Tracked`] is precise at-least-once. An `ack` advances the stored position to just
  below the lowest still-unsettled delivery, so acks arriving out of order from concurrent
  lanes never commit past unprocessed work, and offset gaps the consumer never sees
  (transaction markers, compacted records) cannot block it. `nack(false)` settles the offset;
  `nack(true)` leaves it unsettled, so the partition redelivers from the committed position on
  its next fetch and every later ack stays uncommitted until that offset settles.
- [`Commit::Transactional`] is exactly-once: the subscription commits nothing of its own and
  registers its watermark with the [`EosPipeline`](crate::EosPipeline) of the same id.

## Retries and dead-lettering

Kafka holds no record back and counts no deliveries, so the framework does both and there is
nothing native to map onto. `.max_attempts(n)` counts the first delivery as one;
`.dead_letter(topic)` is where a spent delivery is republished. A `retry_after` outcome, and an
immediate `retry()` under a declared cap, become a copy published back to the subscription with
the framework's retry-count header incremented - which is what makes the count travel, Kafka's
own redelivery carrying none. The copy is at-most-once over the delay window: a process that
exits before the timer fires loses it.

Where that copy goes is the descriptor's answer. [`KafkaTopic`] addresses its own topic, so a
registration over it needs nothing. [`KafkaTopics`] and [`KafkaPartitions`] read a set that no
single publish addresses, so such a registration names the destination itself and refuses to
start otherwise: `.out_retry(policy).to("orders.retry")` for a fixed topic, or
`.out_retry(policy).transform(ToSourceTopic)` to send each copy back to the topic its own
delivery arrived on. Retry and dead-letter topics are your infrastructure; the framework only
publishes to them.

## Batches

A handler whose payload parameter is a slice consumes whole batches, and the mount site names
the largest one it accepts. A batch is one delivery plus everything librdkafka has already
fetched, with no added waiting, so it is often smaller than the size asked for:

```
# #[cfg(feature = "json")]
# mod demo {
use ruststream_rdkafka::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Event {
    id: u64,
}

#[subscriber(KafkaTopic::new("events").group("indexer").commit(Commit::Tracked))]
async fn index(events: &[Event]) -> HandlerOutcome {
    println!("indexing {} events", events.len());
    HandlerOutcome::ack()
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("indexer", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]),
        |b| {
            b.include(index.batch(nonzero!(500usize)));
        },
    )
}
# }
# fn main() {}
```

Returning `Vec<HandlerOutcome>` settles element by element. Under `Commit::Tracked` the
committed position still stops in front of the first element asking for a redelivery and stays
there until that offset comes back, so the acked elements behind it replay with it: duplicates,
not loss. A cap and a dead-letter topic are what stop one poison element from holding a
partition back. How much librdkafka keeps queued locally is a consumer property
(`queued.max.messages.kbytes` and friends) and stays in the descriptor's `config` passthrough.

## Positions and seeking

Kafka keeps the log, so a subscription can be moved through it. [`KafkaPosition::earliest`],
[`latest`](KafkaPosition::latest), [`offset`](KafkaPosition::offset) (one partition),
[`topic_offset`](KafkaPosition::topic_offset) (one partition of one named topic) and
[`timestamp`](KafkaPosition::timestamp) (epoch milliseconds, resolved per partition) are the
positions. `start_at(position)` at the mount site opens the subscription there on every
startup, whatever the group committed before: the position lands on the first assignment the
group hands over, ahead of its first record. Under [`Assignment::CooperativeSticky`] that first
assignment may be a part of the group's share, and partitions arriving in a later increment are
not repositioned. A handler repositions the running subscription
through the [`SeekHandle`](context::keys::SeekHandle) context key, reading where it currently
sits from [`Position`](context::keys::Position).

A seek moves this consumer instance and not the group: it repositions the partitions this
member holds, commits nothing on anyone's behalf, and is discarded by the next rebalance, since
whoever gets those partitions resumes from the group's committed offsets. Under
`Commit::Tracked` a seek clears the tracked position of every partition it moves, so a later
commit cannot advance past records the seek replayed but nobody handled.

## The per-delivery context

Typing the context parameter as `Context<'_, KafkaContext>`, or binding one field with `Ctx<K>`,
gives a handler [`Topic`](context::keys::Topic), [`Partition`](context::keys::Partition),
[`Offset`](context::keys::Offset), [`TimestampMillis`](context::keys::TimestampMillis),
[`Key`](context::keys::Key), [`Source`](context::keys::Source) (the coordinates an
exactly-once publish takes), [`Position`](context::keys::Position) and
[`SeekHandle`](context::keys::SeekHandle). A batch handler gets
[`KafkaBatchContext`](context::KafkaBatchContext) instead, which carries only the seek handle:
a batch spans many records and no single position describes it, and the two being separate
types is what rejects a batch body naming a per-delivery field at compile time.

A consume error surfaces on the subscription, except the ones librdkafka retries by itself:
today exactly an unknown topic or partition, where one warning opens the episode and the
repeats go to debug, so a topic that appears late recovers without flooding the error log.

# Publishing

An outgoing message's name is the destination topic. A publisher is a policy plus a live form:
the policy holds settings only and is constructible anywhere, and pairing it with the connected
broker at startup produces the publisher, so "not connected yet" is not representable. Under
the prelude's names the policies are [`Publish`](prelude::Publish),
[`TransactionalPublish`](prelude::TransactionalPublish),
[`PartitionedPublish`](prelude::PartitionedPublish) and [`EosPublish`](prelude::EosPublish).

Where a policy is named:

- `b.include(handler)` alone - a returned reply goes through the broker's default policy.
- `b.include(handler).out_reply(policy)` - the reply publisher.
- `b.include(handler).out(marker, policy).build()` - the publisher an `Out` parameter receives.
- `b.include(handler).out_retry(policy)` - the publisher a deferred retry copy leaves through.
- `b.after_startup(policy, hook)` - a scope hook run once with the live publisher.
- [`ConnectedKafkaBroker::publisher`] - outside the runtime, on a broker you connected yourself.

A reply type declares where it goes: `#[outgoing(name = "confirmations")]` on the type plus a
bare `publish` clause on the subscriber, or no name on the type and `publish("confirmations")`
at the mount site. Kafka has no reply correlation, so this crate implements no request/reply
capability; a synchronous exchange is a reply topic of your own plus a correlation header.

## Record keys and partitions

The framework's partition-key header becomes the record's native key on publish and is not
repeated as a Kafka header; consuming through this crate reports it back under the same name.
Kafka routes records sharing a key to one partition, which is what keeps per-key ordering.

[`KafkaOptions`] is this crate's per-message setting, and it holds one field: the destination
partition. On a publish builder it is the `partition(n)` step of [`KafkaPublishSteps`]; an
explicit partition wins over the record key, and the record key wins over the configured
partitioner. A placement rule that is not per-message is a transform of the mount chain
instead: [`RoundRobin`] spreads replies over a fixed partition count, placing only records that
carry neither a key nor a partition of their own.

```
# #[cfg(feature = "json")]
# mod demo {
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[derive(Serialize, Outgoing)]
struct Audit {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Audit)]
struct Journal;

/// The slot is bounded on the options type, which is what puts `partition(..)` on its builder.
#[subscriber("orders")]
async fn record(
    order: &Order,
    Out(journal): Out<impl Publisher<Options = KafkaOptions>, Journal>,
) -> HandlerOutcome {
    if journal
        .message(&Audit { id: order.id })
        .to("audit")
        .partition(0)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
        |b| {
            b.include(record).out(Journal, Publish::default()).build();
        },
    )
}
# }
# fn main() {}
```

## Delivery guarantees

Durability is the producer's own `acks` setting, and every other librdkafka property goes
through [`KafkaBroker::producer_config`] (producer only) or [`KafkaBroker::config`]
(client-wide). At-least-once end to end is an idempotent producer
(`producer_config("enable.idempotence", "true")`) together with `Commit::Tracked` on the
consuming side. Every publish awaits the cluster's delivery report, so an `Ok` means Kafka
accepted the record.

## Transactions

`Publish::default().transactional_id("orders-svc-1")` is the transactional policy, and the
publisher it pairs into carries the core's `TransactionalPublisher` capability: publishes
between `begin_transaction` and `commit` become visible together to a reader at
`read_committed` isolation, and `abort` discards them. Outside an open transaction the handle
publishes like a plain one; a second `begin_transaction` reports
[`KafkaError::TransactionBusy`], and a `commit` or `abort` with nothing open reports
[`KafkaError::NoTransaction`].

Two Kafka facts shape the rest. One producer runs one transaction at a time, and one
transactional id belongs to one live producer, since initializing a second fences the first.
So a transaction is not an independently owned value here, and a `workers(n, by_key)` pool
cannot share one transactional publisher. The scope that does compose with a pool is the source
partition: [`per_partition`](KafkaTransactionalPublish::per_partition) yields one publisher per
partition under the ids `"{base}-p{partition}"`, handed to a handler bounded
`Out<impl PartitionLanes>`, so each lane runs an independent transaction. It does not compose
with `LaneKey::RecordKey`, which spreads one partition across lanes.

```
# #[cfg(feature = "json")]
# mod demo {
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Refund {
    order_id: u64,
    lines: u64,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "refund-lines")]
struct RefundLine {
    order_id: u64,
    line: u64,
}

#[derive(OutSlot)]
#[publishes(RefundLine)]
struct Lines;

/// The body names the capability and nothing of this crate's; the mount site names the policy.
#[subscriber("refunds")]
async fn refund(
    order: &Refund,
    Out(lines): Out<impl TransactionalPublisher, Lines>,
) -> HandlerOutcome {
    if lines.begin_transaction().await.is_err() {
        return HandlerOutcome::retry();
    }
    for line in 0..order.lines {
        let entry = RefundLine {
            order_id: order.order_id,
            line,
        };
        if lines.message(&entry).publish().await.is_err() {
            lines.abort().await.ok();
            return HandlerOutcome::retry();
        }
    }
    if lines.commit().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("refunds", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("refunds-svc"),
        |b| {
            b.include(refund)
                .out(Lines, Publish::default().transactional_id("refunds-svc-1"))
                .build();
        },
    )
}
# }
# fn main() {}
```

## Exactly-once pipelines

[`EosPublish::new(id)`](KafkaEosPublish::new) covers the full consume-transform-produce shape
(KIP-447): one transactional producer serves every lane and commits the consumed offsets inside
its own transaction, so source positions move atomically with the records published. Three
places name one id - `Commit::Transactional(id)` on each participating subscription, the policy
at the mount site, and nothing else - because the id is the fencing unit.

A publishing handler needs no explicit publish: name the pipeline as the mount site's policy
and add the [`EosReplies`] transform after it, and every reply joins the open window together
with its delivery's consumed offset. Leaving the transform off makes the first reply fail with
the missing coordinates rather than sending it outside the window. For a publish written by
hand, [`EosPipeline::publish`] takes those coordinates as an argument and a handler reads its
own from the [`Source`](context::keys::Source) key; the live pipeline reaches such a handler
through `b.after_startup(EosPublish::new(id), hook)`.

Every `commit_interval` (100 ms by default) the window closes: the pipeline waits for its
participants to settle, adds the settled positions and the consumer's group metadata to the
transaction, and commits. A publish error, a commit error or a settle stall aborts the window
instead, the consumers seek back to the last committed offsets, and the whole window replays
into a fresh transaction. So end-to-end latency is at least the commit interval, a `retry()`
from a participant stalls its window until the transaction deadline, and the deferred
`retry_after` copy does not apply to pipeline replies at all, a delayed copy being exactly what
breaks the offset-record pairing.

## Back-pressure and shutdown

A publish waits indefinitely for space when librdkafka's local queue is full, which is the
natural back-pressure. [`KafkaPublish::queue_timeout`] bounds that wait and turns it into a
queue-full error instead. The connected broker's
[`shutdown`](ruststream::ConnectedBroker::shutdown) flushes in-flight publishes within
[`KafkaBroker::flush_timeout`] (30 seconds unless set) and returns the
[`ClosedKafkaBroker`] witness, whose [`unflushed_records`](ClosedKafkaBroker::unflushed_records)
counts what librdkafka still held.

# The prelude

`use ruststream_rdkafka::prelude::*;` is the one glob a Kafka service imports: the core's own
prelude plus this crate's broker, descriptors and their settings, publish policies under their
concept names, the per-delivery context keys, and the capability traits a handler names.

```
use ruststream_rdkafka::prelude::*;

let orders = KafkaTopic::new("orders")
    .group("orders-svc")
    .commit(Commit::Tracked)
    .start(StartOffset::Earliest);

let replies = Publish::default();
let lines: TransactionalPublish = Publish::default().transactional_id("refunds-svc-1");
let lanes: PartitionedPublish = lines.clone().per_partition();
let pipeline = EosPublish::new("enrich-svc-1");
# let _ = (orders, replies, lanes, pipeline);
```

A handler body imports the framework prelude and bounds an injected slot with the capability it
needs, so it names no broker type. The stated exception is a body that adjusts a per-record
setting: `partition(..)` needs [`KafkaPublishSteps`] in scope and the slot bounded
`Out<impl Publisher<Options = KafkaOptions>, Marker>`, which is what the example above writes.

# The `AsyncAPI` document

With the `asyncapi` feature the generated document carries Kafka's own vocabulary, the
specification's `kafka` binding: a server reports the schema registry it is configured with, a
channel the topic behind it, and a `receive` operation the consumer group that reads it. The
bootstrap addresses reach the document as bare `host:port` coordinates, with the scheme and any
userinfo stripped, for the reason a registry URL does - the document is published and shared.

A channel the service publishes to reports its topic too, and that topic is the destination the
mount site resolved: a registration's `publish("dest")` clause, a reply type's own
`#[outgoing(name)]`, the name of a slot entry, a declared dead-letter topic. A publish policy
carries producer settings and no destination, so it has none of its own to report.

What it cannot report follows from when it is built, which is before anything connects, from
the descriptor alone. A group appears only when the descriptor names one, never the broker's
`default_group`; a client id appears only when the `config` passthrough sets `client.id`; a
[`KafkaTopics`] subscription reports its group and no topic, the binding's `topic` field naming
one. `protocolVersion` stays out, because Kafka negotiates its version per API key between the
client and the cluster.

# Testing

A test runs the service's own app: the builder `main` runs, on [`KafkaBroker`], handed to the
framework's `TestApp` harness unchanged. With the `testing` feature in `[dev-dependencies]`,
`TestApp::start` connects the broker in process instead of reaching a cluster, and the test
addresses it by its production type, `tb.broker::<KafkaBroker>()`. `TestApp::start_live` runs
the same test body against a running Kafka. The harness's usage is the core's:
<https://docs.rs/ruststream/latest/ruststream/testing/index.html>.

```
# #[cfg(all(feature = "json", feature = "testing"))]
# mod demo {
use ruststream::testing::TestApp;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "payments")]
pub struct Payment {
    pub amount: u64,
}

#[subscriber(KafkaTopic::new("payments").commit(Commit::Tracked))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

/// The app `main` runs.
pub fn app() -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        KafkaBroker::new(["kafka:9092"]).default_group("payments-svc"),
        |b| {
            b.include(accept);
        },
    )
}

pub async fn zero_amounts_are_dropped() -> Result<(), Box<dyn std::error::Error>> {
    let tb = TestApp::start(app()).await?;

    // The publish returns once the handler it woke has settled.
    tb.broker::<KafkaBroker>()
        .message(&Payment { amount: 0 })
        .publish()
        .await?;

    tb.broker::<KafkaBroker>()
        .subscriber("payments")
        .assert_called_once()
        .with(&Payment { amount: 0 })
        .settled(HandlerOutcome::drop());

    tb.shutdown().await?;
    Ok(())
}
# }
# #[cfg(all(feature = "json", feature = "testing"))]
# #[tokio::main(flavor = "multi_thread", worker_threads = 2)]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
#     demo::zero_amounts_are_dropped().await
# }
# #[cfg(not(all(feature = "json", feature = "testing")))]
# fn main() {}
```

In process, the broker's connected form carries an in-process Kafka cluster in place of
librdkafka, and so do its subscribers, its publishers, its seeker and its deliveries: the
descriptors and publish policies of the routes file are the production ones, `EosPublish`
included. The cluster has no settings of its own. It reads what librdkafka would read: the
broker's `config` and `producer_config`, each descriptor's typed options and passthrough, and
librdkafka's defaults for everything left unset. A property librdkafka refuses fails the connect
or the subscription the same way.

It models Kafka's semantics rather than a queue:

- Topics and partitions. A topic comes into being with one partition, the broker's
  `num.partitions` default, when a record is produced to it or a subscription names it. A record
  lands on the partition a `partition(n)` step names, the one its key hashes to, or the next one
  in turn; a partition the topic lacks, an illegal topic name and a record over
  `message.max.bytes` are refused.
- Consumer groups. A group hands each partition to one member, so each group reads a record
  once and every group reads its own copy; a pattern subscription reads every topic its pattern
  matches, and a [`KafkaPartitions`] reader reads its partitions apart from any group. A member
  joining or leaving rebalances the group, and a partition resumes from the group's committed
  offset, or from `auto.offset.reset` where the group has none.
- Offsets. The commit mode decides what is committed, as on a cluster: auto-commit commits as
  records are handed over, so a requeue under [`Commit::Auto`] reports itself unsupported; a
  [`Commit::Tracked`] retry holds the committed position below the record, which comes back,
  with everything after it, when the partition is next fetched from that position.
- Positions. A seek moves the partitions the subscription reads, to an offset, either end of the
  log, or the first record at a timestamp.
- Transactions. A `read_committed` reader, librdkafka's default, reads a transaction's records
  when it commits and never after an abort; a second pairing of a transactional id fences the
  first; a transaction open past `transaction.timeout.ms` is aborted; and offsets sent to a
  transaction commit with it, which is what an exactly-once pipeline rests on. A publish into an
  open transaction keeps the harness's reaction open until the transaction ends, so an
  exactly-once reply is on its topic when the publish that caused it returns.

What only a cluster has belongs to the live mode, over the same test body: a topic created with
more than one partition, a partition assignment the cooperative protocol keeps sticky,
retention, and the timing of a real group join. The crate's live suites run against the stand in
`docker-compose.test.yml` (`just test-brokers`).

# Operations

- Addresses. [`KafkaBroker::new`] takes the bootstrap servers; a `PLAINTEXT://` or `SASL_SSL://`
  prefix is accepted and trimmed for the generated document.
- Authentication. SASL `PLAIN`, `SCRAM` and `OAUTHBEARER` are built into librdkafka and need no
  cargo feature: set `security.protocol`, `sasl.mechanism`, `sasl.username` and `sasl.password`
  through [`KafkaBroker::config`]. Kerberos (`gssapi`) needs a system library and is reached by
  depending on `rdkafka` directly, cargo features being additive across the graph.
- TLS. The `ssl` feature links the system OpenSSL, `ssl-vendored` builds it; either turns on
  `security.protocol=SSL` and the `ssl.*` properties. `zstd` adds that compression codec, the
  others being built in.
- Timeouts. [`connect_timeout`](KafkaBroker::connect_timeout) bounds the startup reachability
  probe, [`flush_timeout`](KafkaBroker::flush_timeout) the shutdown flush, and
  [`queue_timeout`](KafkaPublish::queue_timeout) a publish waiting for local queue space. All
  three default to this crate's own values rather than a librdkafka property.
- Passthroughs. `config(key, value)` on the broker, the descriptor and
  `producer_config(key, value)` on the broker reach every property not surfaced as a typed
  option. A descriptor's passthrough is applied last, so it wins over the typed options - except
  over a property the subscription's [`Commit`] mode owns (`enable.auto.offset.store`,
  `enable.auto.commit`). Setting one of those under [`Commit::Tracked`] or
  [`Commit::Transactional`] would leave the committed position to librdkafka while every ack
  decided nothing, so the subscription refuses to open and the error names the property and the
  mode. [`Commit::Auto`] owns neither and takes both.
- Known gaps. There is no request/reply capability and no owned transaction, for the Kafka
  reasons above. The registry paths do not resolve schema references, so a `.proto` importing
  anything outside the well-known types is out of reach, and a Protobuf schema cannot be derived
  from a generated type, so a service registers its `.proto` separately. [`KafkaPartitions`]
  does not combine with `Commit::Transactional`.

# Cargo features

`json` (default), `msgpack` and `cbor` forward the core's codecs; the default codec is what a
mount site with no `.codec(..)` step encodes with. `schema-registry` adds the Confluent client,
the wire-format envelope and the subscriber-side prefetch; `avro` and `protobuf` add those
formats on top of it. `asyncapi` contributes the Kafka bindings to the generated document,
`testing` adds the in-process mode `TestApp::start` connects the broker through, and `ssl`,
`ssl-vendored` and `zstd` map onto rdkafka's own backends.
