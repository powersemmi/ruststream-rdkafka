<h1 align="center">ruststream-rdkafka</h1>

<p align="center">
  <i>The Apache Kafka broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: consumer groups, precise tracked commits, native record keys, and an in-process test broker.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-rdkafka/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-rdkafka/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-rdkafka"><img src="https://img.shields.io/crates/v/ruststream-rdkafka.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-rdkafka"><img src="https://img.shields.io/crates/dr/ruststream-rdkafka" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-rdkafka"><img src="https://img.shields.io/docsrs/ruststream-rdkafka" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-rdkafka/">Documentation</a></b>
</p>

---

`ruststream-rdkafka` implements the RustStream broker contract on top of
[rdkafka](https://docs.rs/rdkafka) / librdkafka: `#[subscriber]` handlers consume topics through
consumer groups, publishers await Kafka's delivery reports, and the whole service composes with
the synchronous `#[ruststream::app]` builder because the broker's constructor is pure
configuration - the runtime climbs the lifecycle ladder around it.

## Features

- **Consumer groups as descriptors** - `KafkaTopic::new("orders").group("workers")` describes
  one subscription; the bare-string `#[subscriber("orders")]` form rides on the broker's
  `default_group`. `KafkaTopic::pattern` subscribes by regex instead, and `.partitions([0, 1])`
  assigns partitions by hand, without group membership or rebalancing.
- **Three commit modes** - librdkafka auto-commit (`Commit::Auto`, the default); precise
  per-message acknowledgement over a contiguous watermark (`Commit::Tracked`), correct under
  concurrent handler lanes; and `Commit::Transactional`, where the consumer stops committing
  and the exactly-once pipeline moves the positions inside the producer transaction.
- **Native record keys** - the partition-key header becomes the record's Kafka key, so per-key
  ordering works end to end, including `workers(n, by_key)` lanes.
- **Native batches** - a slice parameter consumes whole batches, and the subscriber implements
  the core's `BatchSubscriber` directly rather than buffering client-side: a batch is one
  delivery plus everything librdkafka has already fetched, cut off at the `batch(nonzero!(n))`
  the mount site names, with no added waiting.
- **Retries and dead-lettering** - without a policy `nack(true)` keeps Kafka's native meaning
  (the offset stays unsettled and redelivers on the next fetch); `Retry::Topic` republishes to
  a retry topic with an attempt counter, `Retry::SeekBack` re-consumes in place,
  `max_deliveries` caps the attempts, and `dead_letter` routes the drop path to a topic
  stamped with the origin coordinates.
- **librdkafka delegation** - unset options mean librdkafka defaults; raw `config(key, value)`
  passthroughs on the broker, the producer, and the descriptor reach every property not
  surfaced as a typed option.
- **Typed lifecycle** - synchronous `new` records configuration, `connect` probes the cluster and
  hands back the connected broker, `shutdown` consumes it into a closed witness carrying the flush
  result; subscribing before connecting or publishing after shutdown does not compile.
- **Policies, then live publishers** - `KafkaPublish` and its transactional, per-partition, and
  exactly-once transitions are pure declaration, named at the mount site with
  `.out(marker, policy)`; the runtime pairs each into a live publisher once the broker is
  connected, so a handler never sees a not-connected one.
- **Repositionable subscriptions** - a handler moves its own subscription over the partitions
  this consumer holds (earliest, latest, an absolute offset, a timestamp, or the delivery's own
  position) by reading the `SeekHandle` key off its context, with the tracked watermark and the
  exactly-once offsets following the seek; `start_at(..)` applies a position on every startup.
- **Confluent Schema Registry** - the `schema-registry` feature transcodes framed deliveries on
  the way in and frames publishes by the subject's registered flavor on the way out, as
  middleware on the async edges, so handlers and codecs stay on plain JSON. `avro` and
  `protobuf` add those two flavors.
- **In-process test broker** - the `testing` feature ships `KafkaTestBroker` for application
  tests with the core `TestApp` harness, no cluster required.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-rdkafka = "0.7"
serde = { version = "1", features = ["derive"] }
```

The crate builds librdkafka from source by default (a C toolchain is the only requirement).
Cargo features: `json` (on by default), `msgpack`, and `cbor` forward the core's codecs;
`schema-registry`, `avro`, and `protobuf` cover Confluent framing; `testing` ships the
in-process broker; `ssl` / `ssl-vendored` for TLS and `zstd` for compression map 1:1 onto
rdkafka's. SASL PLAIN/SCRAM/OAUTHBEARER need no feature - librdkafka implements them
built-in; other backends (gssapi, dynamic linking, ...) can be enabled by depending on
`rdkafka` directly, since cargo features are additive across the dependency graph.

## Write a service

```rust
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize)]
struct Confirmation {
    id: u64,
}

// `Commit::Tracked` turns every ack into a precise per-message acknowledgement; the `publish`
// clause sends the returned value to the `confirmations` topic.
#[subscriber(
    KafkaTopic::new("orders").commit(Commit::Tracked),
    publish("confirmations")
)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
        |b| {
            b.include(confirm);
        },
    )
}
```

`#[ruststream::app]` generates `main`, so `cargo run -- run` starts the service and
`cargo run -- asyncapi gen` prints its AsyncAPI document.

The reply above rides the broker's default publish policy, so the mount site names no
publisher. Everything else is named there: `.out(Reply, policy)` for the reply slot,
`.out(DefaultSlot, policy)` (or your own marker) for an `Out<..>` handler parameter. A policy
holds no connection - `Publish::default().transactional_id("orders-svc-1")`,
`EosPublish::new("enrich-svc-1")` - which is why it can be written next to the `include`; the
runtime pairs it into a live publisher after the broker connects.

Those policy names come from this crate's prelude, which aliases `KafkaPublish` and its
transitions to the uniform ones, so mount sites read the same on every broker. A handler body
imports `ruststream::prelude::*` instead and bounds its slot with a capability
(`Out<impl Publisher>`, `Out<impl TransactionalPublisher>`): it names no broker type, and
mounts unchanged against the in-process test broker.

Full compiling examples: `examples/kafka_quickstart.rs` and `examples/kafka_topics.rs`.

## Test it

`KafkaTestBroker` stands in for the cluster in-process, and the core `TestApp` harness drives
the service through the same dispatch path production uses. Include sites do not change: the
real `KafkaPublish` policy pairs against the test broker too.

```toml
[dev-dependencies]
ruststream-rdkafka = { version = "0.7", features = ["testing"] }
```

```rust
use ruststream::testing::TestApp;
use ruststream_rdkafka::testing::KafkaTestBroker;

// The harness seeds one payload type and reads back the other, so both derive `Serialize`,
// `Deserialize`, and `PartialEq` here.
let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .with_broker(KafkaTestBroker::new(), |b| {
        b.include(confirm);
    });
let tb = TestApp::start(app).await?;

// `publish` returns once the handlers have settled, so the assertions read finished state.
tb.broker::<KafkaTestBroker>()
    .publish("orders", &Order { id: 42 })
    .await?;

tb.broker::<KafkaTestBroker>()
    .subscriber("orders")
    .assert_called_once()
    .with(&Order { id: 42 })
    .settled(HandlerOutcome::ack());

tb.broker::<KafkaTestBroker>()
    .published::<Confirmation>("confirmations")
    .assert_called_once()
    .with(&Confirmation { id: 42 });
```

The transport retains what it routes, so `Ctx<Position>`, `Ctx<SeekHandle>` and `start_at(..)`
work here as well. Consumer groups, real partitions, committed offsets, rebalancing, and
everything transactional are cluster behavior: exercise those against a live Kafka. Full
example: `examples/kafka_testing.rs`.

## Scaffold a service

```bash
cargo generate --git https://github.com/powersemmi/ruststream-rdkafka templates/kafka-topic --name my-service
```

The starter wires one broker with a default consumer group, a tracked-commit subscriber with a
retry and dead-letter pipeline plus a published reply, and the `#[ruststream::app]` entry point.

## Documentation

- [Topics and groups](https://powersemmi.github.io/ruststream-rdkafka/latest/topics/) -
  descriptors, start offsets, commit modes, keyed lanes, retries, batches.
- [Publishing](https://powersemmi.github.io/ruststream-rdkafka/latest/publishing/) - policies,
  record keys, transactions, exactly-once pipelines.
- [Schema Registry](https://powersemmi.github.io/ruststream-rdkafka/latest/schema-registry/) -
  Confluent framing, Avro and Protobuf transcoding.
- [Testing](https://powersemmi.github.io/ruststream-rdkafka/latest/testing/) - the in-process
  broker and the live-cluster suites.
- API reference: <https://docs.rs/ruststream-rdkafka>

## Contributing

```bash
just check          # fmt, clippy, and feature checks
just test           # the suite; the live-cluster tests skip without a broker
just test-brokers   # the same suite against a Kafka container
```

## License

Apache-2.0.
