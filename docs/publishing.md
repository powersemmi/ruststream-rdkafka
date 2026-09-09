# Publishing

An outgoing message's name is the destination topic. `KafkaPublish` declares the queue timeout
and the transactional id, and you name it where you register the handler. At startup the policy
instantiates `KafkaPublisher` on the connected broker.

A plain publisher sends through the broker's shared producer. A transactional one gets a producer
of its own, fenced by its id. Every publish awaits the cluster's delivery report, so an `Ok` means
Kafka accepted the record.

Publishing has two vocabularies, and they belong in different files. A handler body imports
`ruststream::prelude` and bounds its injected slot with the **capability** it needs
(`Out<impl Publisher>`, `Out<impl TransactionalPublisher>`), so it names no broker type at all. A
mount site imports `ruststream_rdkafka::prelude`, which adds the core one plus the **policies**
under their concept names: `Publish`, `TransactionalPublish`, `PartitionedPublish`, `EosPublish`.
An include site therefore reads the same on every broker.

Where a policy is named:

- `b.include(handler)` alone - a returned reply is published through the broker's default policy,
  `KafkaPublish::default()`.
- `b.include(handler).out(Reply, policy)` - the handler's reply publisher.
- `b.include(handler).out(marker, policy).build()` - the publisher an `Out<..>` parameter
  receives, named by the slot's marker (`DefaultSlot` when the parameter declares none).
- `b.after_startup(policy, hook)` - a scope-level hook that runs once with the live publisher,
  after every subscription is open.
- `connected.publisher(policy)` - outside the runtime, on a broker you connected yourself (see
  the `kafka_producer` example).

One publisher does not come from a policy. `broker.retry_publisher()` comes from the
*unconnected* broker, for the one wiring that takes a live publisher instead: `retry_via`, the
deferred republish standing in for the delayed redelivery Kafka does not have (see
[batch settlement](topics.md#how-batch-settlement-maps-onto-kafka)). It resolves the connection
at startup. Before `connect` it returns `KafkaError::NotConnected`, and after the broker shuts
down `KafkaError::Closed`.

## The publish builder

Every publisher starts a publish the same way, through the blanket `PublishExt`:
`message(&value)`, then `to(..)` for the destination, `with_headers(..)` for the headers,
`with_codec(..)` for another codec, and `publish()` to send. A handler's `Out` parameter, a
publisher held in application state and a handle created from the connected broker all publish
through those calls; only the codec `message(..)` uses differs. An already-encoded payload is a
`#[derive(Outgoing, Serialized)]` newtype, and the same call sends it with no encoding step.

You pass this crate's per-message arguments in the publish's headers position, and the publisher
turns them into the record's own fields rather than into Kafka headers: the record key and the
explicit partition, both below. A placement rule that is not per-message is a `PublishTransform`
step of the mount site's chain instead (`RoundRobin` below).

An `Out` parameter names a capability, not a publisher type. This crate declares one of its own,
`PartitionLanes`: it hands out one transactional publisher per source partition. A handler writes
`Out(lanes): Out<impl PartitionLanes>`, and the `per_partition()` policy constructs the concrete
`TransactionalPartitions` behind it (see
[transaction scopes and worker pools](#transaction-scopes-and-worker-pools)).

A lane publishes through the publisher the slot handed out, so its messages go to the broker's
publish log and not to the slot's test record (`tb.out::<Marker>()`). In tests, assert on the
publish log for lane traffic; the slot record covers handlers that publish through the slot
itself.

## Record keys

The partition-key header becomes the record's native key on publish, and is not repeated as a
Kafka header. Consuming through this crate reports the key back under the same header name. Kafka
routes records that share a key to one partition, which is what keeps per-key ordering:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_producer.rs:producer"
```

Without the header, the configured partitioner picks the partition.

## Explicit partitions and round-robin distribution

The partition header (`kafka-partition`, an ASCII decimal) pins a record to one exact partition:
the publisher reads the header, sets the record's partition, and does not send the header itself.
An explicit partition wins over the record key, and the record key wins over the configured
partitioner. A value that is not a decimal index makes the publish return an error, and a
partition the topic does not have makes it return a delivery error:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_producer.rs:partition"
```

`RoundRobin` uses that same header to spread replies evenly. No librdkafka partitioner places
records round-robin one at a time, and keyless records may stick to one partition for a whole
batch. With long, near-constant per-message work that means one hot consumer and idle peers. The
transform sets the next partition of the cycle on every reply that has neither a key nor a
partition of its own:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_distribution.rs:round_robin"
```

The count is explicit and must match the destination topic's partition count: a smaller one
leaves the tail partitions idle, a larger one makes publishes to the missing partitions return an
error. You can write your own `PublishTransform` over the same header for any other placement
rule.

## Delivery guarantees

Durability is the producer's `acks` setting. You set every other librdkafka property
(`enable.idempotence`, `message.timeout.ms`, compression) on the broker yourself:

- `KafkaBroker::producer_config(key, value)` - producer-only properties.
- `KafkaBroker::config(key, value)` - client-wide properties (consumers and the producer).

For at-least-once end to end, combine an idempotent producer
(`producer_config("enable.idempotence", "true")`) with `Commit::Tracked` on the consuming side
(see [Topics and groups](topics.md)).

## Transactions

`KafkaPublish::default().transactional_id("orders-svc-1")` constructs
`KafkaTransactionalPublisher`, which adds the core `TransactionalPublisher` capability. Publishes
between `begin_transaction` and `commit` become visible atomically: a reader on Kafka's default
`read_committed` isolation sees all of them or none. `abort` discards them broker-side.

Outside an open transaction the handle publishes like a plain one. `commit` or `abort` with no
open transaction returns a `NoTransaction` error, and a second `begin_transaction` returns
`TransactionBusy`.

One atomic fan-out per call, committing at the end and aborting on the first error:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:fanout"
```

The handler takes the live publisher as an injected `Out` parameter, and the policy instantiates
it once the subscription opens. An abort leaves nothing visible, so the handler asks for a
redelivery and reruns the whole fan-out:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:handler"
```

The include site names the id, one per concurrent producer:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:id"
```

The policy creates the transactional producer and initializes its transactions when it constructs
the publisher. That initialization fences earlier producers holding the id, so the handle is
fenced from the moment it exists. The id must be stable and unique per concurrent producer: name
a distinct policy for each concurrent transactional flow. `transaction_timeout` bounds the
control calls, and you set Kafka's own `transaction.timeout.ms` through `producer_config`.

Tying consumed offsets into the producer transaction, the full consume-transform-produce shape,
is the [exactly-once pipeline](#exactly-once-pipelines) below.

### Transaction scopes and worker pools

Two Kafka facts shape everything here: one producer runs one transaction at a time, and one
transactional id belongs to one live producer (initializing a second fences the first). A
`workers(n, by_key)` pool therefore cannot share one transactional publisher: merging two lanes'
messages into one transaction would commit one flow's records with the other's.

The scope that composes with a worker pool is the source partition. Under the default
`LaneKey::Partition` lanes a partition's deliveries process serially on one lane. The
`per_partition()` policy constructs `TransactionalPartitions`, one publisher per partition with
the ids `"{base}-p{partition}"`, so every lane runs an independent transaction with no
coordination. The id set follows the topic's partitions rather than the worker count, so changing
`workers(n)` changes neither the ids nor the fencing:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:partitions"
```

The include site names the base id:
`.out(DefaultSlot, Publish::default().transactional_id("billing-svc-1").per_partition())`.
`TransactionalPartitions` creates and initializes a partition's publisher on its first use, which
is why `for_partition` is async and can return the initialization error.

The per-partition scope does not compose with record-key lanes (`LaneKey::RecordKey`): they
spread one partition across lanes, so two lanes would collide on that partition's publisher.
Sharing one id across a whole pool is the exactly-once pipeline below.

### Exactly-once pipelines

`KafkaEosPublish` covers the full consume-transform-produce shape (KIP-447) and constructs the
live `EosPipeline`. One transactional producer serves every lane, and it commits the consumed
offsets inside its own transaction (`send_offsets_to_transaction`), so source positions move
atomically with the published records. A crash or an aborted window rewinds both: handlers
reprocess the deliveries, and the output topic never sees a duplicate.

`Commit::Transactional("enrich-svc-1")` on the subscription switches its consumer's own
committing off and registers its watermark with the pipeline of that id.
`KafkaEosPublish::new("enrich-svc-1")` is the producer side, and the pipeline itself exists only
once that policy instantiates it on the connected broker.

Three places name one id:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:eos"
```

A publishing handler needs no explicit publish. Name the pipeline as the mount site's policy, add
the `EosReplies` transform after it, and every reply joins the open window together with its
delivery's consumed offset:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:eos_wiring"
```

`EosReplies` copies the delivery's source coordinates onto the reply, and that is what the
pipeline matches the two by. It is an ordinary step of the mount site's chain, so you can put
`.codec(..)` and further `.transform(..)` steps around it as usual. Leave it off and publishing
the first reply returns an error about the missing coordinates instead of sending it outside the
window.

For a publish you write yourself, `EosPipeline::publish` takes the delivery's coordinates as an
argument, and a handler reads its own from a `Ctx<Source>` parameter like any other
`KafkaContext` field. A handler cannot take the pipeline as an `Out` slot: a slot's bound names a
capability, and this crate declares none for the pipeline's explicit form.

Publishes join the pipeline's open window. Every `commit_interval` (100ms by default, the Kafka
Streams exactly-once default) the window closes: the pipeline waits for its participants to
settle, adds the settled positions and the consumer's group metadata to the transaction, and
commits. The group metadata fences a stale consumer server-side, so a rebalance in mid-window
makes the commit return an error instead of committing offsets the consumer no longer owns.

Three things abort the window: a publish error, a commit error, or a settle stall (a handler
hanging or asking for a retry past the publisher's transaction timeout). The consumers then seek
back to the last committed offsets, so the whole window redelivers promptly and republishes into
a fresh transaction. Records published into an aborted window were never visible to
`read_committed` readers, librdkafka's default here.

Practical notes:

- One pipeline id per service instance, exactly like any transactional id: it is the fencing
  unit.
- End-to-end latency is at least the commit interval: records become visible at the window
  commit, not at publish.
- A `retry()` from a participant stalls its window until the transaction deadline and then aborts
  it, so prefer `drop()` and dead-lettering for poison messages in EOS handlers.
- The `retry_after` deferred-republish fallback (`retry_via`, see
  [batch settlement](topics.md#how-batch-settlement-maps-onto-kafka)) does not apply to EOS
  replies: a delayed copy would break the offset-record pairing.
- The reply path works only for subscriptions in `Commit::Transactional` mode naming this
  pipeline's id; a reply from any other subscription returns an error.
- Works best over the default `LaneKey::Partition` lanes, where each partition settles in order
  behind its lane head.

## Back-pressure and shutdown

When librdkafka's local queue is full, a publish waits for space indefinitely, which is the
natural back-pressure behavior. `KafkaPublish::queue_timeout` bounds that wait, and the publish
then returns a queue-full error.

`ConnectedKafkaBroker::shutdown` flushes in-flight publishes and returns an error when they are
not delivered within `KafkaBroker::flush_timeout` (30 seconds unless you set it). It consumes the
connected broker and returns the `ClosedKafkaBroker` witness, whose `unflushed_records()` counts
what librdkafka still held. Publishers created before the shutdown stay usable as values, and
every publish through them returns `KafkaError::Closed`.
