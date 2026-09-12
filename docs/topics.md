# Topics and groups

A subscription is one consumer joining one consumer group on one topic. `KafkaTopic` describes it,
and the topic name is the only part you have to give. Every option you leave unset keeps the
librdkafka default.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_topics.rs:descriptor"
```

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_topics.rs:app"
```

## Consumer groups

Kafka cannot subscribe to a topic without a consumer group. You name one per subscription with
`KafkaTopic::group`, or once for the whole broker with `KafkaBroker::default_group`. The
broker-wide group covers every subscription that names none, including the bare-string
`#[subscriber("orders")]` form. A subscription that ends up with no group is a startup error,
unless it names its own partitions: a manual assignment joins no group.

## Start offsets

`StartOffset` picks where a group starts reading when it has no valid committed offset for a
partition. There is none when the group has never committed that partition, and none when
retention deleted the committed offset or it fell out of range. A long-idle group meets the
second case: with librdkafka's `latest` reset it skips to the end of the log instead of
reprocessing. The option maps to librdkafka's `auto.offset.reset`:

- `Committed` (default) - leave the choice to librdkafka (its default resets to the latest offset).
- `Earliest` - start from the earliest retained offset.
- `Latest` - start from the latest offset, so only messages published after the group formed arrive.

## Commit modes

Kafka commits one position per partition instead of settling individual messages. The `Commit`
mode picks how `ack` and `nack` map onto that model:

- `Commit::Auto` (default, librdkafka's own behavior): positions are stored as messages are handed
  to the application and committed every `auto.commit.interval.ms`. `ack` and `nack` are
  advisory no-ops. A crash can lose the tail of processed-but-uncommitted work or skip
  unprocessed deliveries that were already stored.
- `Commit::Tracked`: precise at-least-once. An `ack` advances the stored position to just below
  the lowest still-unsettled delivery, so out-of-order acks from concurrent worker lanes never
  commit past an unprocessed message. Offset gaps the consumer never receives (transaction
  markers, compacted-away records) cannot block that position. The subscription switches
  `enable.auto.offset.store` off, and auto-commit still flushes the stored position in the
  background and once more when the consumer closes.
- `Commit::Transactional("pipeline-id")`: exactly-once. The `EosPipeline` with the matching
  transactional id commits the offsets inside the producer transaction, atomically with the
  records the handlers publish, and the consumer commits nothing of its own. `ack` advances the
  shared watermark exactly as under `Tracked`; see
  [Exactly-once pipelines](publishing.md#exactly-once-pipelines).

Negative settlement under `Tracked`:

- `nack(false)` (drop) settles the offset so the position can move past it.
- `nack(true)` (requeue) leaves the offset unsettled. The committed position stays below it, so
  Kafka redelivers from there on the next fetch of the partition, which comes with a rebalance or
  a restart. The unsettled offset also holds the watermark back, so every later ack stays
  uncommitted until that offset settles: a handler that nacks in a loop pins the committed
  position. Retry topics, seek-back redelivery and dead-letter routing are descriptor options, see
  [Retries and dead-lettering](#retries-and-dead-lettering).

## Multiple topics and patterns

One subscription can consume several topics through one consumer and one group. All matched
topics share the handler, and therefore its payload type; each delivery reports the topic it came
from:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_multi_topic.rs:multi"
```

For an open-ended set, a librdkafka topic regex subscribes to every matching topic. The pattern
must start with `^`, the anchor that tells librdkafka a name is a pattern:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_multi_topic.rs:pattern"
```

In-process tests cover a multi-topic subscription by exact name; a pattern needs a cluster.

## Partition assignment

`KafkaTopic::assignment` picks how the group balances partitions across its members (librdkafka's
`partition.assignment.strategy`): `Assignment::Range`, `Assignment::RoundRobin`, or
`Assignment::CooperativeSticky`. The cooperative-sticky strategy rebalances incrementally: the
partitions it does not move keep delivering while the rebalance runs. Unset means the librdkafka
default (`range,roundrobin`). A cooperative strategy and an eager one cannot mix within one group.

## Manual partition assignment

`KafkaTopic::partitions` switches the subscription from the group protocol to manual assignment:
the consumer takes exactly the partitions you name, without joining a group and without
rebalancing. That fits a reader pinned to a partition, an inspection or replay tool, and a
one-consumer-per-partition deployment.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_topics.rs:assign"
```

A group stays optional here, and it changes only how offsets are handled. With one named, the
consumer commits into the group without joining it: `Commit::Tracked` stores positions exactly as
on a normal subscription, and `StartOffset::Committed` resumes from them. Without one, commits are
off, so the start offset has to be explicit (`Earliest` or `Latest`) and acks are advisory;
`Commit::Tracked` and `StartOffset::Committed` are then startup errors.

A group-less reader still runs under a `group.id`, because librdkafka requires one even for a
manual assignment: the placeholder `ruststream.standalone` never joins and never commits.

Manual assignment names exact partitions of one topic, so it does not combine with `and_topic` or
`pattern`. It does not combine with `Commit::Transactional` either. The in-process test broker
does not simulate partitions and refuses the descriptor, so this belongs in a live test.

Manual assignment composes with keyed worker lanes. Under the default `LaneKey::Partition` each
assigned partition gets a lane of its own, so `partitions([0, 2, 5])` with `workers(n, by_key)`
processes every assigned partition in order. Size `n` against the partition list: fewer lanes than
partitions makes partitions share lanes, which keeps their order, and more lanes than partitions
leaves some idle.

## Repositioning a subscription

Kafka keeps the log, so you can move a subscription through it: replay from an earlier point, skip
a poison run, or rebuild a projection from the beginning on every boot. A position is a
`KafkaPosition`, built with its constructors:

| Position | Applies to | Resumes at |
|---|---|---|
| `KafkaPosition::earliest()` | every assigned partition | the earliest retained offset |
| `KafkaPosition::latest()` | every assigned partition | the end of the log (only new records) |
| `KafkaPosition::offset(partition, n)` | one partition | the absolute offset `n` |
| `KafkaPosition::timestamp(millis)` | every assigned partition | the first record at or after that time (its end when there is none) |

Every delivery also reports its own position, pinned to topic, partition and offset. Seeking to it
redelivers exactly that record and the ordered suffix behind it.

A position is used in two places. The `start_at(..)` clause opens the subscription there on every
startup, whatever the group committed before; `StartOffset` only applies when the group has no
committed offset for the partition:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_seek.rs:start_at"
```

The second place is a running handler. It can take the subscription's seeker as a
`Ctx(seeker): Ctx<SeekHandle>` parameter and reposition the subscription from inside the body. The
sibling key `Position` reports where the delivery being handled sits, which is what replaying that
record takes:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_seek.rs:handler"
```

A batch handler declares `KafkaBatchContext` instead and reads the same `SeekHandle` key from it.
That context holds nothing per-delivery: a batch spans many records, and no single position
describes it. Where to seek comes from the elements:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_seek.rs:batch"
```

The scope and the bookkeeping:

- **A seek moves this consumer instance, not the group.** It repositions the partitions this
  member currently holds; other members keep reading where they are, and nothing is committed on
  anyone's behalf. Seeking to a position that names a partition this consumer does not hold
  returns an error.
- **A rebalance discards a seek.** The reposition belongs to the assignment it was applied to. A
  member joining or leaving, a session timeout or a change in topic metadata revokes those
  partitions, and whoever gets them next, this instance included, resumes from the group's
  committed offsets. A position that has to survive restarts belongs in `start_at(..)`, which
  applies it on every startup.
- **The offsets follow the read position.** Under `Commit::Tracked` a seek clears the tracked
  position of every partition it moves, and librdkafka's own offset store with it, so a later
  commit cannot advance past records the seek replayed but nobody handled. Settling a delivery
  pulled before the seek changes nothing, because it names a position the subscription no longer
  reads from. In an exactly-once pipeline a transaction window still open when the seek lands is
  aborted instead of committed, so the group is never carried past the replayed range, and the
  replayed deliveries are processed into a fresh window.

A service that repositions is testable without a cluster. The in-process test broker keeps what it
routes and hands the subscription the same seeker over that log, so the handlers above mount on it
unchanged. See [repositioning in-process](testing.md#repositioning-in-process) for which positions
it resolves and which it refuses.

## Keyed worker lanes

Kafka partitions by the native record key, and this crate can use that key as the lane key, so
`workers(n, by_key)` keeps per-key ordering from the producer to the handler:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_keys.rs:consumer"
```

`KafkaTopic::lane_key` picks what the lane key is. The default, `LaneKey::Partition`, lanes by the
source partition, Kafka's own ordering unit: everything one partition delivers, keyless records
included, processes in order on one lane, and concurrency comes from consuming several partitions:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_keys.rs:partition_lanes"
```

`LaneKey::RecordKey` narrows the lane to the record key, as in the first example above:
deliveries that share a record key stay ordered, and different keys of one partition process
concurrently. A keyless delivery then has no lane key and rotates across lanes, so it loses even
its partition order.

## Retries and dead-lettering

Without a policy, `nack(true)` keeps Kafka's native meaning: the offset stays unsettled and
redelivers on the next fetch of the partition. `KafkaTopic::retry` replaces that with a policy
that acts at once:

- `Retry::Topic("orders.retry")` republishes the message to the retry topic with the attempt count
  in the `kafka-retry-count` header, then settles the original. It republishes before it settles,
  so a crash between the two steps duplicates the message and never loses it.
- `Retry::SeekBack` seeks the partition back and re-consumes the message in place; everything
  after it on that partition replays too, and the attempt count is kept only for the current
  session.
- `Retry::Drop` treats `nack(true)` like the drop path.

`max_deliveries(n)` caps how many times one message is delivered, the original counting as the
first. Once the next retry would exceed the cap, the drop path runs instead. The cap counts
against a retry policy or a dead-letter topic, so setting it alone is a startup error.

`dead_letter("orders.dlq")` sends the drop path, `nack(false)` included, to a dead-letter topic
and then settles the original. The copy is stamped with the `kafka-dlq-source-topic`,
`-partition` and `-offset` headers. Without a dead-letter topic the drop path only settles.

Retry and dead-letter topics are your infrastructure: the crate only publishes to them. These
policies also need a cluster, because the in-process test broker re-enqueues `nack(true)` in place
instead of running them.

The usual arrangement puts the retry topic on the same subscription with `and_topic`, so a retried
copy comes back to the same handler:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_retries.rs:retry_topic"
```

`Retry::SeekBack` keeps strict partition order at the cost of throughput: nothing overtakes a
failed message while it retries:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_retries.rs:seek_back"
```

A dead-letter consumer is an ordinary subscription; the `kafka-dlq-source-*` headers name the
origin of the failed delivery:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_retries.rs:dead_letter"
```

## Batches

A handler whose parameter is a slice consumes whole batches: the signature says so, not the
attribute. A batch is one delivery plus everything librdkafka has already fetched, with no added
waiting:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_batches.rs:handler"
```

The mount site names the largest batch the handler accepts, and a batch handler does not compile
without it. The size reaches the consumer's poll, which never hands the body more records than
that; a batch is smaller whenever that is all the fetch queue had:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_batches.rs:size"
```

How much librdkafka keeps queued locally is a separate, consumer-side setting, and it stays in the
descriptor's config passthrough (`queued.max.messages.kbytes` and friends). Batch handlers
otherwise mount with `include`, like every other handler:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_batches.rs:app"
```

### How batch settlement maps onto Kafka

A batch handler settles the whole batch with one `HandlerOutcome`, or element by element by
returning `Vec<HandlerOutcome>`, where entry `i` settles element `i`:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_batches.rs:selective"
```

Kafka commits one position per partition, not one per message, so the outcomes map onto it as
follows. Everything below assumes `Commit::Tracked`; under `Commit::Auto` every settlement is an
advisory no-op and none of it applies:

- **Uniform `Ack`** - the batch settles, the position advances.
- **Per-element with `Ack`s only** - also exact. Acks may land out of order across concurrent
  batches, and the position always advances to just below the lowest unsettled delivery.
- **Per-element with a `retry()` in the middle** - each element does settle individually, but the
  committed position stops in front of the first retried element and stays there until that offset
  redelivers, on the next fetch of the partition. The acked elements behind it replay then too:
  at-least-once duplicates, not loss. As far as the committed position is concerned, selective ack
  therefore reaches only up to the first retry; when one poison element must not hold the batch
  back, you can give the subscription a retry topic.
- **Per-element with `retry_after(..)`** - Kafka has no native delayed redelivery, so the runtime
  falls back to a deferred republish. With `retry_via(broker.retry_publisher())` on the scope, the
  element settles immediately and the position moves past it, and after the delay a copy is
  published to the end of the topic with an incremented `x-ruststream-retry-count` header. The
  copy loses its place in the order and is at-most-once across the delay window: a crash before
  the timer fires loses it. Without `retry_via` the delay is dropped with a warning and the
  element behaves like the plain `retry()` above.
- **A result vector shorter than the batch** - the elements it does not cover are retried, and the
  mismatch is logged.

### Concurrency

`workers(n)` on a batch registration keeps up to `n` batches in flight at once, and the tracked
position stays correct when their acks arrive out of order. `by_key` has no meaning for batches: a
keyed policy there behaves like a plain pool of the same size. Per-key ordering belongs to
single-message handlers, where `workers(n, by_key)` keeps the deliveries that share a lane key on
one lane (see the keyed lanes example).

The in-process test broker batches natively the same way, draining what is enqueued, so a batch
handler mounts on either broker unchanged.

## Consume errors

A consumer error surfaces on the subscription, except the ones librdkafka is already retrying by
itself: today exactly `UnknownTopicOrPartition`, a subscribed topic that does not exist yet. The
subscription logs one warning when such an episode starts, and that warning is the monitoring
signal to act on; the repeats and the recovery go to debug. A topic that appears late (broker
auto-creation, a provisioning race) therefore recovers on its own without flooding the dispatch
error log, while a topic that never appears leaves the warning standing.

## Raw configuration passthrough

`KafkaTopic::config(key, value)` sets any librdkafka consumer property this crate does not surface
as a typed option (`fetch.min.bytes`, `session.timeout.ms`, `isolation.level`, and the rest). It
is applied last, so it wins over the typed options. Setting a key a commit mode relies on, such as
`enable.auto.offset.store`, changes what that mode does.
