# Topics and groups

A subscription is one consumer joining one consumer group on one topic. `KafkaTopic` is the
descriptor: everything besides the topic name is optional, and unset options mean the
librdkafka defaults - this crate does not impose its own.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_topics.rs:descriptor"
```

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_topics.rs:app"
```

## Consumer groups

Kafka cannot subscribe without a group, so every subscription needs one from somewhere:
`KafkaTopic::group` per subscription, or `KafkaBroker::default_group` for everything that does
not name its own (including the bare-string `#[subscriber("orders")]` form). A subscription
that ends up with no group at all is a clear startup error, not a silent default.

## Start offsets

`StartOffset` maps to librdkafka's `auto.offset.reset` and applies when the group has no valid
committed offset for a partition: it has never committed one, or the committed offset was
deleted by retention / is out of range (which is how a long-idle group on the default `latest`
reset skips to the end instead of reprocessing):

- `Committed` (default) - leave the choice to librdkafka (its default resets to the latest offset).
- `Earliest` - start from the earliest retained offset.
- `Latest` - only messages published after the group formed.

## Commit modes

Kafka commits one position per partition instead of settling individual messages. The `Commit`
mode picks how `ack` and `nack` map onto that model:

- `Commit::Auto` (default, the librdkafka behavior): positions are stored as messages are handed
  to the application and committed every `auto.commit.interval.ms`. `ack` and `nack` are
  advisory no-ops. A crash can lose the tail of processed-but-uncommitted work or skip
  unprocessed deliveries that were already stored.
- `Commit::Tracked`: precise at-least-once. `enable.auto.offset.store` is switched off, and an
  `ack` advances the stored position to just below the lowest still-unsettled delivery, so
  out-of-order acks (concurrent worker lanes) never commit past an unprocessed message, and
  offset gaps the consumer never receives (transaction markers, compacted-away records) cannot
  block the position. Auto-commit still flushes the stored position in the background and once
  more when the consumer closes.
- `Commit::Transactional("pipeline-id")`: exactly-once. The consumer never commits its own
  offsets - the `EosPipeline` with the matching transactional id commits them inside the
  producer transaction, atomically with the records the handlers publish. `ack` advances the
  shared watermark exactly like `Tracked`; see
  [Exactly-once pipelines](publishing.md#exactly-once-pipelines).

Negative settlement under `Tracked`:

- `nack(false)` (drop) settles the offset so the position can move past it.
- `nack(true)` (requeue) leaves the offset unsettled: the committed position stays below it, so
  Kafka redelivers from there when the partition is next fetched (a rebalance or a restart).
  The unsettled offset also blocks the watermark, keeping every later ack uncommitted until
  then: precise, and a handler that nacks in a loop holds the committed position back. Retry
  topics, seek-back redelivery, and dead-letter routing are descriptor options, see
  [Retries and dead-lettering](#retries-and-dead-lettering).

## Multiple topics and patterns

One subscription can consume several topics through one consumer and one group. All matched
topics share the handler, and therefore its payload type; each delivery still reports the
topic it came from:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_multi_topic.rs:multi"
```

For open-ended sets, a librdkafka topic regex subscribes to every matching topic - the pattern
must start with `^` (librdkafka's anchor for distinguishing patterns from literal names):

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_multi_topic.rs:pattern"
```

The in-process test broker supports multi-topic subscriptions (exact-name routing per topic)
but not patterns.

## Partition assignment

`KafkaTopic::assignment` picks how the group balances partitions across members (librdkafka's
`partition.assignment.strategy`): `Assignment::Range`, `Assignment::RoundRobin`, or
`Assignment::CooperativeSticky` for incremental rebalancing where unaffected partitions keep
flowing during a rebalance. Unset means the librdkafka default (`range,roundrobin`).
Cooperative and eager strategies cannot mix within one group, and librdkafka offers no API for
custom group assignors.

## Manual partition assignment

`KafkaTopic::partitions` switches the subscription from the group protocol (`subscribe`) to
manual assignment (`assign`): the consumer takes exactly the named partitions of the topic -
no group membership, no rebalancing. It covers consuming exactly these partitions: static
pinning, inspection and replay readers, one-consumer-per-partition deployments.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_topics.rs:assign"
```

A group is optional and changes only offset handling. With one named, the consumer commits
into the group without joining it: `Commit::Tracked` stores positions exactly as on a normal
subscription, and `StartOffset::Committed` resumes from them. Without one, commits are off and
the start offset must be explicit (`Earliest` or `Latest`); `Commit::Tracked` and
`StartOffset::Committed` are clear startup errors, and acks are advisory (librdkafka insists
on a `group.id` even for `assign()`, so a group-less reader runs under the inert
`ruststream.standalone` placeholder - it never joins or commits). Manual assignment
does not combine with `and_topic`/`pattern` (it names exact partitions of one topic) or with
`Commit::Transactional`, and the in-process test broker rejects it (it does not simulate
partitions).

Manual assignment composes with keyed worker lanes: under the default `LaneKey::Partition`
each assigned partition lanes independently, so `partitions([0, 2, 5])` +
`workers(n, by_key)` processes every assigned partition in order on its lane. Size `n` against
the partition list - fewer lanes than partitions means partitions share lanes (ordering still
holds), more means idle lanes.

## Repositioning a subscription

Kafka keeps the log, so a subscription can be moved through it: replay from an earlier point,
skip a poison run, or start a projection from the beginning on every boot. Positions are
`KafkaPosition` values, built with its constructors:

| Position | Applies to | Resumes at |
|---|---|---|
| `KafkaPosition::earliest()` | every assigned partition | the earliest retained offset |
| `KafkaPosition::latest()` | every assigned partition | the end of the log (only new records) |
| `KafkaPosition::offset(partition, n)` | one partition | the absolute offset `n` |
| `KafkaPosition::timestamp(millis)` | every assigned partition | the first record at or after that time (its end when there is none) |

A delivery also carries its own position (`Positioned::position`), which is the pinned
topic-partition-offset form: seeking to it redelivers exactly that record and the ordered
suffix behind it.

There are two ways to use them. The `start_at(..)` clause opens the subscription at a position
on every startup, whatever the group committed before - unlike `StartOffset`, which only
applies when the group has no committed offset for the partition:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_seek.rs:start_at"
```

A running subscription moves through its own per-delivery context. The subscription mints the
seeker when it opens and every delivery carries it, so the `SeekHandle` key hands the handler a
live handle by construction - as a `Ctx(seeker): Ctx<SeekHandle>` parameter, or off a declared
`Context<'_, KafkaContext>` with `ctx.context(SeekHandle)`. Its sibling key `Position` reports
where the delivery being handled sits, which is what "replay this record" takes:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_seek.rs:handler"
```

A batch handler gets `KafkaBatchContext` instead - the same `SeekHandle` key, and nothing
per-delivery, because a page spans many records and no single position describes it. Where to
seek then rides the elements:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_seek.rs:batch"
```

The scope and the bookkeeping:

- **A seek moves this consumer instance, not the group.** It repositions the partitions this
  member currently holds; other members keep reading where they are, and nothing is committed
  on anyone's behalf. A position naming a partition this consumer does not hold is an error,
  not a silent no-op.
- **A rebalance discards a seek.** The reposition lives in the assignment it was applied to.
  When a member joins or leaves, a session times out, or topic metadata changes, the partitions
  are revoked and whoever gets them next - this instance included - resumes from the group's
  committed offsets. Repositioning is an operational action on a running consumer, not durable
  state; a position that must survive restarts belongs in `start_at(..)`, which reapplies it
  every time.
- **The offsets follow the read position.** Under `Commit::Tracked` the contiguous-offset
  watermark resets to the seek target (and so does librdkafka's own offset store), so a later
  commit can never advance past records the seek replayed but nobody handled. Deliveries pulled
  before the seek settle into nothing: they describe a position the subscription no longer
  reads from. In an exactly-once pipeline the same rule applies to the transaction: a window
  open when the seek lands is aborted rather than committed, so `send_offsets_to_transaction`
  never carries the group past the replayed range, and the replayed deliveries are processed
  into a fresh window.

## Keyed worker lanes

Kafka partitions by the native record key, and this crate surfaces it through
`IncomingMessage::partition_key` (with the `Partitioned` capability mirroring it), so keyed
lanes keep per-key ordering end to end:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_keys.rs:consumer"
```

`KafkaTopic::lane_key` picks what drives the lanes. The default, `LaneKey::Partition`, lanes
by the source partition - Kafka's native ordering unit, so everything one partition delivers
(keyless included) processes in order on one lane, and concurrency comes from consuming
several partitions:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_keys.rs:partition_lanes"
```

`LaneKey::RecordKey` opts into finer, per-record-key lanes (the first example above):
deliveries sharing a record key stay ordered while different keys of one partition process
concurrently. Keyless deliveries then carry no lane key and rotate across lanes, losing even
their partition order.

## Retries and dead-lettering

Without a policy, `nack(true)` keeps Kafka's native meaning: the offset stays unsettled and
redelivers on the next fetch of the partition. `KafkaTopic::retry` gives it an immediate one:

- `Retry::Topic("orders.retry")` republishes the message to the retry topic with an attempt
  counter riding in the `kafka-retry-count` header, then settles the original
  (republish-first: a crash between the steps duplicates, never loses).
- `Retry::SeekBack` seeks the partition back and re-consumes the message in place; everything
  after it on that partition replays too, and the attempt count survives only within the
  session.
- `Retry::Drop` treats `nack(true)` like the drop path.

`max_deliveries(n)` is the poison cap (the original delivery counts as one): once the next
retry would exceed it, the drop path runs instead. `dead_letter("orders.dlq")` routes the drop
path - `nack(false)` included - to a dead-letter topic, stamped with `kafka-dlq-source-topic` /
`-partition` / `-offset` headers, then settles; without it the drop path just settles. Retry
and dead-letter topics are your infrastructure: the crate only publishes to them. The
in-process test broker does not run these policies (`nack(true)` re-enqueues in place there).

The usual pipeline puts the retry topic on the same subscription with `and_topic`, so retried
copies come back to the same handler:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_retries.rs:retry_topic"
```

`Retry::SeekBack` trades throughput for strict partition order - nothing overtakes a failed
message while it retries:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_retries.rs:seek_back"
```

A dead-letter consumer is an ordinary subscription; the `kafka-dlq-source-*` headers carry the
origin of the failed delivery:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_retries.rs:dead_letter"
```

## Batches

A handler that takes a slice consumes whole pages; nothing in the attribute says so, the
signature does. Batching is native here: the subscriber implements the core `BatchSubscriber`
capability directly, and a page is one delivery plus everything librdkafka has already fetched -
no added waiting, no crate-imposed knobs:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_batches.rs:handler"
```

Page size is bounded by librdkafka's own fetch-queue limits, reached through the descriptor's
config passthrough:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_batches.rs:size"
```

Batch handlers mount with `include`, like every other handler:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_batches.rs:app"
```

### How batch settlement maps onto Kafka

A batch handler settles its page either uniformly (one `HandlerOutcome` for every element) or
per element (`Vec<HandlerOutcome>`, entry `i` settling element `i`):

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_batches.rs:selective"
```

Kafka commits one position per partition, not per message, so the outcomes map as follows
(everything below assumes `Commit::Tracked`; under `Commit::Auto` every settlement is an
advisory no-op and none of it applies):

- **Uniform `Ack`** - the page settles, the position advances.
- **Per-element with `Ack`s only** - also exact: acks may land out of order across concurrent
  pages, the position always advances to just below the lowest unsettled delivery.
- **Per-element with a `retry()` in the middle** - the runtime does settle each element
  individually, but the committed position stops in front of the first retried element and
  stays there until that offset redelivers (the next fetch of the partition: a rebalance or a
  restart). When it does, the acked tail behind it replays too - at-least-once duplicates, not
  loss. Selective ack therefore works "up to the first nack" as far as the committed position
  is concerned; use a retry policy (retry topics) when a poison element must not hold the page
  hostage.
- **Per-element with `retry_after(..)`** - Kafka has no native delayed redelivery, so the
  runtime's deferred-republish fallback runs: with `retry_via(broker.retry_publisher())`
  configured on the scope, the element settles immediately (the position moves past it) and a
  copy republishes to the topic's tail after the delay, carrying an incremented
  `x-ruststream-retry-count` header - ordering is not preserved, and the copy is at-most-once
  across the delay window (a crash before the timer fires loses it). Without `retry_via` the
  delay is dropped with a warning and the element degrades to a plain `retry()` hole as above.
- **A too-short result vector** - the unmatched remainder of the page is retried (an extra
  redelivery beats a silently lost message) and the mismatch is logged.

### Concurrency

`workers(n)` on a batch registration keeps up to `n` pages in flight at once; the tracked
position stays correct under out-of-order acks by construction. `by_key` does not apply to
batches - a keyed policy behaves like a plain pool of the same size. Per-key ordering is a
single-message-handler feature (`workers(n, by_key)`, see the keyed lanes example), where it
composes with Kafka's native record-key partitioning end to end.

The in-process test broker batches natively the same way (a page drains what is enqueued), so a
batch handler mounts on either without changing.

## Consume errors

The subscriber stream forwards consumer errors as stream items, except the ones librdkafka is
already retrying by itself - today exactly `UnknownTopicOrPartition`, a subscribed topic that
does not exist yet. Such an episode surfaces as one warning when it starts - the monitoring
signal to act on - with debug lines for the repeats and the recovery, so a topic that appears
late (broker auto-creation, provisioning races) recovers on its own without flooding the
dispatch error log, while a topic that never appears leaves the warning standing.

## Raw configuration passthrough

`KafkaTopic::config(key, value)` reaches any librdkafka consumer property this crate does not
surface as a typed option (`fetch.min.bytes`, `session.timeout.ms`, `isolation.level`, ...). It
is applied last, so it wins over the typed options; overriding the keys a commit mode relies on
(`enable.auto.offset.store`) changes that mode's behavior accordingly.
