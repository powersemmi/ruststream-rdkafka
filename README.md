<h1 align="center">ruststream-rdkafka</h1>

<p align="center">
  <i>The Apache Kafka broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: consumer groups, precise tracked commits, native record keys, and an in-process test broker.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-rdkafka/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-rdkafka/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-rdkafka"><img src="https://img.shields.io/crates/v/ruststream-rdkafka.svg" alt="crates.io"></a>
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
the synchronous `#[ruststream::app]` builder thanks to the lazy startup contract.

## Features

- **Consumer groups as descriptors** - `KafkaTopic::new("orders").group("workers")` describes
  one subscription; the bare-string `#[subscriber("orders")]` form rides on the broker's
  `default_group`.
- **Two commit modes** - librdkafka auto-commit (`Commit::Auto`, the default) or precise
  per-message acknowledgement over a contiguous watermark (`Commit::Tracked`), correct under
  concurrent handler lanes.
- **Native record keys** - the partition-key header becomes the record's Kafka key, so per-key
  ordering works end to end, including `workers(n, by_key)` lanes.
- **librdkafka delegation** - unset options mean librdkafka defaults; raw `config(key, value)`
  passthroughs on the broker, the producer, and the descriptor reach every property not
  surfaced as a typed option.
- **Fail-fast lifecycle** - synchronous `new`, idempotent `connect` with a cluster-reachability
  probe, and a `shutdown` that flushes in-flight publishes.
- **In-process test broker** - the `testing` feature ships `KafkaTestBroker` for application
  tests with the core `TestApp` harness, no cluster required.

## Install

```toml
[dependencies]
ruststream = { version = "0.5", features = ["macros", "json"] }
ruststream-rdkafka = "0.5"
serde = { version = "1", features = ["derive"] }
```

The crate builds librdkafka from source by default (a C toolchain is the only requirement).
Optional cargo features map 1:1 onto rdkafka's: `ssl` / `ssl-vendored` for TLS and `zstd` for
compression. SASL PLAIN/SCRAM/OAUTHBEARER need no feature - librdkafka implements them
built-in; other backends (gssapi, dynamic linking, ...) can be enabled by depending on
`rdkafka` directly, since cargo features are additive across the dependency graph.

## Scaffold a service

```bash
cargo generate --git https://github.com/powersemmi/ruststream-rdkafka templates/kafka-topic --name my-service
```

## License

Apache-2.0.
