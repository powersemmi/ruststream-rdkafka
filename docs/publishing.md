# Publishing

The outgoing message name is the destination topic. Publishing comes in two halves: a
`KafkaPublish` **policy** (pure declaration - queue timeout, transactional id - constructible
anywhere, with no publish surface) and the **live** `KafkaPublisher` the runtime pairs from it
against the connected broker. Every publisher on this page follows that split, so a handler
never holds a publisher that is not connected yet. A plain live publisher rides the broker's
shared producer (a transactional one gets its own, fenced by its id), and each publish awaits
the cluster's delivery report, so an `Ok` means Kafka accepted the record.

The prelude exports the policies under their own names - `KafkaPublish`,
`KafkaTransactionalPublish`, `KafkaPartitionedPublish`, `KafkaEosPublish`. The unprefixed concept
names belong to the core (`Publish` is its slot capability trait), and two broker preludes can be
globbed side by side without a name clash.

Where a policy is named:

- `b.include(handler)` alone - a `publish("dest")` handler replies through the broker's default
  policy, `KafkaPublish::default()`.
- `b.include(handler).publisher(policy)` - the handler's reply publisher, or the publisher its
  `Out<..>` parameter receives.
- `b.after_startup(policy, hook)` - a scope-level hook that runs once with the live publisher,
  after the subscriptions open.
- `connected.publisher(policy)` - outside the runtime, straight off a broker you connected
  yourself (see the `kafka_producer` example).

One publisher stands outside the split. `broker.retry_publisher()` is minted from
the *unconnected* broker for builder-time wiring that takes a live publisher rather than a
policy - today `retry_via`, the deferred republish behind `retry_after` that Kafka needs because
it has no native delayed redelivery (see
[batch settlement](topics.md#how-batch-settlement-maps-onto-kafka)). It resolves the connection
at startup; used before `connect` it reports `KafkaError::NotConnected`, and after the broker
shuts down `KafkaError::Closed` - never a silent success.

## The publish builder

Every publisher starts a publish the same way, through the blanket `PublishExt`:
`message(&value)`, then `to(..)` for the destination, `with_headers(..)` for the headers,
`with_codec(..)` for a non-default codec, and `publish()` to send. A handler's `Out` parameter, a
publisher held in application state and a handle taken off the connected broker all publish
through those calls; only the codec `message(..)` uses differs. There is no bytes entry point:
an already-encoded payload is a `#[derive(Outgoing, Serialized)]` newtype, which carries its
bytes through the same call with no codec in the way.

This crate's per-message arguments travel in the publish's headers position, and the publisher
turns them into native record fields rather than wire headers: the record key and the explicit
partition below. A placement rule that is not per-message goes on the publisher instead, as a
`PublishTransform` (`RoundRobin` below).

An `Out` parameter names a capability rather than a publisher type, and this crate declares
`PartitionLanes`, the capability of handing out one transactional publisher per source partition.
A handler writes `Out(lanes): Out<impl PartitionLanes>` instead of naming the concrete
`TransactionalPartitions` that the `per_partition()` policy pairs into (see
[transaction scopes and worker pools](#transaction-scopes-and-worker-pools)).

That capability is a router, not a publisher: it hands out a publisher rather than sending a
message, so what a lane publishes leaves through that publisher and lands in the broker's publish
log rather than in the slot's test record (`tb.out::<Marker>()`) - the same boundary a settled
owned transaction's buffer has. In tests, assert on the publish log for lane traffic; the slot
record covers handlers that publish through the slot itself.

## Record keys

The partition-key header becomes the record's native key on publish (and is not duplicated as a
wire header); consuming through this crate surfaces it back under the same name. Kafka routes
every message for one key to one partition, which is what keeps per-key ordering:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_producer.rs:producer"
```

Without the header, the configured partitioner picks a partition (the librdkafka default is a
hash for keyed records and round-robin-ish distribution for keyless ones).

## Explicit partitions and round-robin distribution

The partition header (`kafka-partition`, an ASCII decimal) pins a record to an exact
partition: the publisher consumes the header (it never hits the wire) and sets the record's
partition explicitly. Precedence on publish: an explicit partition wins over the record key,
which wins over the configured partitioner. A malformed value fails the publish with a clear
error, and a partition that does not exist fails delivery - no silent fallback either way:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_producer.rs:partition"
```

On top of it, `RoundRobin` distributes publishing-handler replies evenly: librdkafka has no
round-robin partitioner (only the random/consistent/hash families), and keyless distribution
may batch-stick to one partition, which turns long, near-constant per-message processing into
one hot consumer and idle peers. The transform stamps every keyless, unpinned reply with the
next partition of the cycle:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_distribution.rs:round_robin"
```

The count is explicit and must match the destination topic: a smaller count just leaves the
tail partitions idle, a larger one fails publishes to the missing partitions. The header
mechanism also serves any other placement policy through a user-written `PublishTransform`.

## Delivery guarantees

Durability is the producer's `acks` setting, and everything else librdkafka offers
(`enable.idempotence`, `message.timeout.ms`, compression, ...) is one `producer_config` away:

- `KafkaBroker::producer_config(key, value)` - producer-only properties.
- `KafkaBroker::config(key, value)` - client-wide properties (consumers and the producer).

For at-least-once end to end, pair an idempotent producer
(`producer_config("enable.idempotence", "true")`) with `Commit::Tracked` on the consuming side
(see [Topics and groups](topics.md)).

## Transactions

`KafkaPublish::default().transactional_id("orders-svc-1")` is the transactional policy; it pairs
into a `KafkaTransactionalPublisher`, which adds the core `TransactionalPublisher` capability:
publishes between `begin_transaction` and `commit` become visible atomically (readers on Kafka's
default `read_committed` isolation see all of them or none), and `abort` discards them
broker-side. The id fences zombies, so it must be stable and unique per concurrent producer -
name distinct policies for concurrent transactional flows. Outside an open transaction the
handle publishes like a plain one; `commit` or `abort` with no open transaction is a
`NoTransaction` error, and a second `begin_transaction` is `TransactionBusy`. The transactional
producer is created and initialized when the policy pairs (that initialization is what fences
earlier producers holding the id), so the handle is fenced from the moment it exists;
`transaction_timeout` bounds the control calls, while Kafka's own `transaction.timeout.ms` is
one `producer_config` away. Tying consumed offsets into the producer transaction (full
consume-transform-produce exactly-once) is the
[exactly-once pipeline](#exactly-once-pipelines) below.

One atomic fan-out per call, committing at the end and aborting on any failure:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:fanout"
```

The handler receives the live publisher as an injected `Out` parameter - the runtime pairs it
once, right after the subscription opens - and settles by the outcome: an abort left nothing
visible, so a retry redelivers and reruns the whole fan-out:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:handler"
```

The id is picked at the include site, one per concurrent producer:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:id"
```

### Transaction scopes and worker pools

Two Kafka facts shape everything here: one producer runs one transaction at a time, and one
transactional id belongs to one live producer (initializing a second fences the first). A
`workers(n, by_key)` pool therefore cannot share a single transactional publisher - a second
`begin_transaction` while one is open is a `TransactionBusy` error: silently merging two lanes'
messages into one transaction would commit one flow's records with the other's.

The scope that composes with a worker pool is the source partition. Under the default
`LaneKey::Partition` lanes a partition's deliveries process serially on one lane, so the
`per_partition()` policy - pairing into `TransactionalPartitions`, a publisher per partition
with ids `"{base}-p{partition}"` - gives every lane an independent transaction with no
coordination. The id set follows the topic's partitions rather than the worker count, so
changing `workers(n)` neither changes the ids nor weakens zombie fencing (the same scheme Kafka
Streams uses for its per-task producers):

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:partitions"
```

The include site names the base id:
`.publisher(KafkaPublish::default().transactional_id("billing-svc-1").per_partition())`. Each
partition's publisher is created and initialized on its first delivery, so `for_partition` is
async and reports the initialization failure rather than hiding it.

This does not compose with `LaneKey::RecordKey` pools: record-key lanes spread
one partition across lanes, two lanes would collide on its publisher, and a per-lane id scheme
would tie fencing identity to a runtime knob (`n`) instead of a Kafka-native unit. Sharing one
id across a pool is the exactly-once pipeline below.

### Exactly-once pipelines

`KafkaEosPublish` declares the full consume-transform-produce shape (KIP-447), pairing into the
live `EosPipeline`: one transactional producer shared by every lane, committing the consumed
offsets inside the transaction
(`send_offsets_to_transaction`), so source positions move atomically with the published
records. A crash or an aborted window rewinds both - handlers reprocess (at-least-once on the
handler side, as always), but the output topic never sees a duplicate.

Three places name one id:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:eos"
```

A publishing handler needs no manual pairing at all: mount it with the pipeline's reply
publisher, and every reply joins the window paired with its delivery's consumed offset -

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:eos_wiring"
```

`KafkaEosPublish::replies()` is a plain `TypedPublisher` over the policy (the explicit spelling
is `TypedPublisher::new(policy).transform(EosReplies)`), so codecs and further transforms
compose as usual; `replies_with(codec)` names a non-default codec. For manual publishes,
`EosPipeline::publish` takes the delivery's coordinates explicitly instead of reading them off
the relay header, and a handler gets its own from a `Ctx<Source>` extractor parameter, like
every other `KafkaContext` field key. Reaching the pipeline through an `Out` slot is not a
route yet: an `Out` parameter names a capability, never a publisher type, and this crate
declares no capability for the pipeline's explicit form.

The subscription's `Commit::Transactional("enrich-svc-1")` switches its consumer's own
committing off (the pipeline owns the offsets) and registers its watermark with the pipeline;
`KafkaEosPublish::new("enrich-svc-1")` wires the producer side, and the pipeline itself exists
only by pairing that policy against the connected broker. Publishes join the pipeline's open
window; every `commit_interval` (100ms by default,
the Kafka Streams EOS default) the window closes: the pipeline waits for its participants to
settle, adds the settled positions and the consumer's group metadata to the transaction, and
commits. The group metadata is what fences a stale consumer server-side, so a rebalance
mid-window makes the commit fail instead of committing offsets the consumer no longer owns.

On any failure - a failed publish, a commit error, or a settle stall (a handler hanging or
`retry()`-ing past the publisher's transaction timeout) - the window aborts and the consumers
seek back to the last committed offsets, so the whole window redelivers promptly and
republishes into a fresh transaction. Records published into an aborted window were never
visible to `read_committed` readers (librdkafka's default here).

Practical notes:

- One pipeline id per service instance, exactly like any transactional id: it is the fencing
  unit. Kafka Streams' EOSv2 uses the same one-producer-per-process scheme.
- End-to-end latency is at least the commit interval: records become visible at the window
  commit, not at publish.
- `retry()` from a participant stalls its window until the transaction deadline, then aborts
  it; prefer `drop()`/dead-lettering for poison messages in EOS handlers.
- The `retry_after` deferred-republish fallback (`retry_via`, see
  [batch settlement](topics.md#how-batch-settlement-maps-onto-kafka)) does not apply to EOS
  replies: a delayed copy would break the offset-record pairing.
- The reply publisher pairs only with `Commit::Transactional` subscriptions naming this
  pipeline's id; a reply from any other subscription fails with a clear error instead of
  silently downgrading the guarantee.
- Works best over the default `LaneKey::Partition` lanes, where each partition settles in
  order behind its lane head.

## Back-pressure and shutdown

A publish waits indefinitely for space when librdkafka's local queue is full - the natural
back-pressure behavior. `KafkaPublish::queue_timeout` bounds that wait instead, failing the
publish with a queue-full error.

`ConnectedKafkaBroker::shutdown` flushes in-flight publishes and reports an error when they do
not make it out within `KafkaBroker::flush_timeout` (30 seconds unless configured); it consumes
the connected broker and returns the `ClosedKafkaBroker` witness, whose `unflushed_records()`
counts what librdkafka still held. Publishers paired before the shutdown stay usable as values
but report `KafkaError::Closed` on every publish.
