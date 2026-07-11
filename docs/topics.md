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

Negative settlement under `Tracked`:

- `nack(false)` (drop) settles the offset so the position can move past it.
- `nack(true)` (requeue) leaves the offset unsettled: the committed position stays below it, so
  Kafka redelivers from there when the partition is next fetched (a rebalance or a restart).
  The unsettled offset also blocks the watermark, keeping every later ack uncommitted until
  then - precise, but worth knowing when a handler nacks in a loop. Retry topics, seek-back
  redelivery, and dead-letter routing are planned descriptor options.

## Partition assignment

`KafkaTopic::assignment` picks how the group balances partitions across members (librdkafka's
`partition.assignment.strategy`): `Assignment::Range`, `Assignment::RoundRobin`, or
`Assignment::CooperativeSticky` for incremental rebalancing where unaffected partitions keep
flowing during a rebalance. Unset means the librdkafka default (`range,roundrobin`).
Cooperative and eager strategies cannot mix within one group, and librdkafka offers no API for
custom group assignors.

## Keyed worker lanes

Kafka partitions by the native record key, and this crate surfaces it through
`IncomingMessage::partition_key` (with the `Partitioned` capability mirroring it), so keyed
lanes keep per-key ordering end to end:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_keys.rs:consumer"
```

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
