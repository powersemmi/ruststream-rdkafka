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

## Back-pressure and shutdown

A publish waits indefinitely for space when librdkafka's local queue is full - the natural
back-pressure behavior. `KafkaPublisher::queue_timeout` bounds that wait instead, failing the
publish with a queue-full error.

`Broker::shutdown` flushes in-flight publishes and reports an error when they do not make it
out within `KafkaBroker::flush_timeout` (30 seconds unless configured).
