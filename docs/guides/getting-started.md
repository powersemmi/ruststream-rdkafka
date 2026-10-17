---
sidebar_position: 1
description: "Install ruststream-rdkafka, start a local Kafka, run a first subscriber against it and test it without a cluster."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-rdkafka`, starts a local Kafka and runs a first service against it.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-rdkafka = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-rdkafka = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the default codec, and `logging` prints the service's log to the terminal.
`ruststream-rdkafka` is the Kafka broker.
Its `testing` feature adds the in-process Kafka that tests run on.

The broker crate has these features:

| Feature | What it gives |
|---|---|
| `json` *(default)*, `msgpack`, `cbor` | the codec of the same name |
| `schema-registry` | the Confluent Schema Registry client and its wire format |
| `avro` | Avro over the Schema Registry |
| `protobuf` | Protobuf messages, with the Confluent envelope |
| `asyncapi` | the Kafka bindings in the generated AsyncAPI document |
| `testing` | the in-process Kafka behind `TestApp` |
| `ssl` | TLS through the system OpenSSL |
| `ssl-vendored` | TLS through an OpenSSL built from source |
| `zstd` | the `zstd` compression codec |

## System requirements

The crate builds librdkafka, the C client of Kafka, from source.
The build needs a C compiler and `make`; librdkafka configures itself with its own script.
On Debian and Ubuntu, `build-essential` provides both.
The `ssl` feature also needs the OpenSSL headers (`libssl-dev`), and `ssl-vendored` needs `perl` to build OpenSSL.
A CMake build or a system librdkafka is a feature of [rust-rdkafka](https://docs.rs/rdkafka): add `rdkafka` to your own dependencies with the feature you need, and the build picks it up.

## A local Kafka

One Kafka node in KRaft mode is enough to develop against:

```yaml title="compose.yaml"
services:
  kafka:
    image: apache/kafka:4.0.0
    ports:
      - "9092:9092"
```

Start it and create the topic the service reads:

```bash
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic orders
```

The broker listens on `localhost:9092`.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` reads JSON orders from the `orders` topic.
It acknowledges an order with an amount and drops one without.
[`KafkaBroker::new`](api:ruststream_rdkafka::KafkaBroker::new) takes the bootstrap servers and only records them.
The connection opens when the service starts.
Kafka reads a topic only inside a consumer group.
[`default_group`](api:ruststream_rdkafka::KafkaBroker::default_group) is the group of every subscription that names none.
`#[ruststream::app]` turns `app` into `main`; in the manual form, [`run_main`](api:ruststream::runtime::cli::run_main) does the same.

Run the service:

```bash
cargo run -- run
```

Publish an order from another terminal with the console producer that ships in the image:

```bash
echo '{"id":1,"amount_cents":1250}' | docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders
```

The service logs `order taken` with the order's id.
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops it.

`cargo generate` scaffolds the same kind of service with a router and a reply:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-rdkafka templates/kafka-topic --name my-service
```

## The first test

The example file also holds two tests.
Each starts the same `app` with `TestApp::start`, publishes an order and checks how `take_order` settled it.
`KafkaBroker` connects to an in-process Kafka there, so the tests need no cluster:

```bash
cargo test
```

[Testing a Kafka service](testing.md) walks through these tests and what the in-process Kafka models.

## Coming from rust-rdkafka

A service on rust-rdkafka writes its own consumer loop and producer.
Here is where each part goes in RustStream:

| rust-rdkafka | RustStream |
|---|---|
| `ClientConfig::new().set("group.id", ..)` | [`default_group`](api:ruststream_rdkafka::KafkaBroker::default_group), or [`KafkaTopic::group`](api:ruststream_rdkafka::KafkaTopic::group) per subscription |
| `ClientConfig::set` for any other property | [`KafkaBroker::config`](api:ruststream_rdkafka::KafkaBroker::config), [`producer_config`](api:ruststream_rdkafka::KafkaBroker::producer_config), [`KafkaTopic::config`](api:ruststream_rdkafka::KafkaTopic::config) |
| `StreamConsumer::subscribe` and a `recv` loop | a `#[subscriber]` handler |
| deserializing `message.payload()` | the handler's parameter type, decoded by the codec |
| `store_offset` or `commit_message` after the work | [`Commit::Tracked`](api:ruststream_rdkafka::Commit::Tracked) and the handler's outcome ([Commits and delivery guarantees](commits.md)) |
| `FutureProducer::send` | a reply or an `Out` parameter, sent through a publish policy |
| `seek` and `assign` | `start_at` at the mount site, or the seek handle in the handler ([Replay and seeking](replay.md)) |
| `send_offsets_to_transaction` | [`EosPublish`](api:ruststream_rdkafka::KafkaEosPublish) ([Transactions and exactly-once](exactly-once.md)) |
