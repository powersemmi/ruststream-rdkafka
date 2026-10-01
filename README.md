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

`ruststream-rdkafka` connects a RustStream service to Apache Kafka over
[`rdkafka`](https://docs.rs/rdkafka), the librdkafka binding. Handlers, routing, codecs and
middleware come from the framework; this crate is the transport.

## Features

- **Consumer groups as descriptors:** one topic, a set of topics or a regex, or partitions
  assigned by hand.
- **Three commit modes:** librdkafka auto-commit, precise per-message acknowledgement, and
  commits inside a producer transaction.
- **Record keys and placement:** the partition key becomes the Kafka key, and a publish can pin a
  record to a partition.
- **Native batches** cut from what librdkafka has already fetched.
- **Retry caps and dead-letter topics** declared where the handler is mounted.
- **Transactions and exactly-once pipelines** as publish policies.
- **Repositioning:** a handler moves its subscription to an offset or a timestamp.
- **Confluent Schema Registry** framing, with Avro and Protobuf, behind features.
- **AsyncAPI** with the specification's `kafka` binding, behind the `asyncapi` feature.
- **Tests without a cluster:** handlers run against an in-process Kafka.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-rdkafka = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-rdkafka = { version = "0.7", features = ["testing"] }
```

The crate builds librdkafka from source, so a C toolchain is required. Optional features:
`asyncapi`, `schema-registry`, `avro`, `protobuf`, `ssl`, `ssl-vendored` and `zstd`.

## Write a service

```rust
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Confirmation {
    id: u64,
}

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

`#[ruststream::app]` generates `main`, so the binary understands `run` and `asyncapi gen`.

Scaffold a fresh project from the template:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-rdkafka templates/kafka-topic --name my-service
```

## Test it

`TestApp` runs the service's own app with `KafkaBroker` in process, with no cluster.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(app()).await?;

tb.broker::<KafkaBroker>()
    .message(&Order { id: 42 })
    .to("orders")
    .publish()
    .await?;

tb.broker::<KafkaBroker>()
    .subscriber("orders")
    .assert_called_once()
    .with(&Order { id: 42 })
    .settled(HandlerOutcome::ack());

tb.broker::<KafkaBroker>()
    .published::<Confirmation>("confirmations")
    .assert_called_once()
    .with(&Confirmation { id: 42 });
```

## Documentation

- [Subscribing](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing) - descriptors, start offsets, commit modes, keyed lanes,
  retries, batches, repositioning.
- [Publishing](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#publishing) - policies, record keys, transactions, exactly-once pipelines.
- [`schema_registry`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/schema_registry/index.html) - Confluent framing, with
  [`avro`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/avro/index.html) and [`protobuf`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/protobuf/index.html) beside it.
- [`testing`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/testing/index.html) - the in-process broker and what it does not simulate.
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.88**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
