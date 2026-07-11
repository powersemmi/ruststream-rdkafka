# Publishing

The outgoing message name is the destination topic: `KafkaBroker::publisher()` hands out one
shared producer handle, and each publish awaits the cluster's delivery report, so an `Ok` means
Kafka accepted the record.

## Record keys

The partition-key header becomes the record's native key on publish (and is not duplicated as a
wire header); consuming through this crate surfaces it back under the same name. Kafka routes
every message for one key to one partition, which is what keeps per-key ordering:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_producer.rs:producer"
```

Without the header, the configured partitioner picks a partition (the librdkafka default is a
hash for keyed records and round-robin-ish distribution for keyless ones).

## Delivery guarantees

Durability is the producer's `acks` setting, and everything else librdkafka offers
(`enable.idempotence`, `message.timeout.ms`, compression, ...) is one `producer_config` away:

- `KafkaBroker::producer_config(key, value)` - producer-only properties.
- `KafkaBroker::config(key, value)` - client-wide properties (consumers and the producer).

For at-least-once end to end, pair an idempotent producer
(`producer_config("enable.idempotence", "true")`) with `Commit::Tracked` on the consuming side
(see [Topics and groups](topics.md)).

## Transactions

`broker.publisher().transactional_id("orders-svc-1")` upgrades the handle to the core
`TransactionalPublisher` capability: publishes between `begin_transaction` and `commit` become
visible atomically (readers on Kafka's default `read_committed` isolation see all of them or
none), and `abort` discards them broker-side. The id fences zombies, so it must be stable and
unique per concurrent producer - create several publishers with distinct ids for concurrent
transactional flows. Outside an open transaction the handle publishes like a plain one, and
calling the transaction methods without an id is a clear error. The transactional producer is
created and initialized on first use from the broker's resolved producer configuration;
`transaction_timeout` bounds the control calls, while Kafka's own `transaction.timeout.ms` is
one `producer_config` away. Tying consumed offsets into the producer transaction (full
consume-transform-produce exactly-once) is the
[exactly-once pipeline](#exactly-once-pipelines) below.

The usual shape is a use-case object in the application state that owns the transactional
publisher and runs one atomic fan-out per call:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:state"
```

Handlers receive it through `State` and settle by the outcome - an abort left nothing visible,
so a retry redelivers and reruns the whole fan-out:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:handler"
```

The id is picked where the publisher is created, one per concurrent producer:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:id"
```

### Transaction scopes and worker pools

Two Kafka facts shape everything here: one producer runs one transaction at a time, and one
transactional id belongs to one live producer (initializing a second fences the first). A
`workers(n, by_key)` pool therefore cannot share a single transactional publisher - a second
`begin_transaction` while one is open is a `TransactionBusy` error, deliberately: silently
merging two lanes' messages into one transaction would commit one flow's records with the
other's.

The scope that composes with a worker pool is the source partition. Under the default
`LaneKey::Partition` lanes a partition's deliveries process serially on one lane, so
`TransactionalPartitions` - a publisher per partition, ids `"{base}-p{partition}"` - gives
every lane an independent transaction with no coordination. The id set follows the topic's
partitions rather than the worker count, so changing `workers(n)` neither changes the ids nor
weakens zombie fencing (the same scheme Kafka Streams uses for its per-task producers):

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:partitions"
```

This deliberately does not compose with `LaneKey::RecordKey` pools: record-key lanes spread
one partition across lanes, two lanes would collide on its publisher, and a per-lane id scheme
would tie fencing identity to a runtime knob (`n`) instead of a Kafka-native unit. Sharing one
id across a pool is the exactly-once pipeline below.

### Exactly-once pipelines

`EosPipeline` is the full consume-transform-produce shape (KIP-447): one transactional
producer shared by every lane, committing the consumed offsets inside the transaction
(`send_offsets_to_transaction`), so source positions move atomically with the published
records. A crash or an aborted window rewinds both - handlers reprocess (at-least-once on the
handler side, as always), but the output topic never sees a duplicate.

Three places name one id:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_transactions.rs:eos"
```

The subscription's `Commit::Transactional("enrich-svc-1")` switches its consumer's own
committing off (the pipeline owns the offsets) and registers its watermark with the pipeline;
`EosPipeline::new(broker.publisher().transactional_id("enrich-svc-1"))` wires the producer
side. Publishes join the pipeline's open window; every `commit_interval` (100ms by default,
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
- Works best over the default `LaneKey::Partition` lanes, where each partition settles in
  order behind its lane head.

## Back-pressure and shutdown

A publish waits indefinitely for space when librdkafka's local queue is full - the natural
back-pressure behavior. `KafkaPublisher::queue_timeout` bounds that wait instead, failing the
publish with a queue-full error.

`Broker::shutdown` flushes in-flight publishes and reports an error when they do not make it
out within `KafkaBroker::flush_timeout` (30 seconds unless configured).
