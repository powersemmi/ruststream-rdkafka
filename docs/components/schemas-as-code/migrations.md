---
sidebar_position: 3
title: Migrations
description: "cargo kafka-schema keeps every subject this service owns as numbered migrations in the repository and synchronizes them with the registry."
---

# Migrations

`cargo kafka-schema` keeps every subject this service owns as numbered migrations in the repository and synchronizes them with the registry.

```text
schemas/
  kafka-schema.lock            # written by the tool, committed
  orders-value/
    subject.toml               # format = "avro", compatibility = "BACKWARD_TRANSITIVE"
    0001_create_order.avsc
    0002_add_item.avsc
  payments-value/
    subject.toml               # format = "protobuf", source = "proto/payments.proto"
    0001_init.proto
  inventory-value/
    subject.toml               # owner = "external": pulled, never registered by this service
    0001.avsc
```

```sh
cargo install ruststream-rdkafka-schema-cli

cargo kafka-schema migrate add orders-value add_item
cargo kafka-schema migrate add orders-key create_order_key --record com.acme.orders.OrderKey
cargo kafka-schema migrate import inventory-value
cargo kafka-schema migrate status
cargo kafka-schema migrate run [--dry-run]
cargo kafka-schema prepare [--check]
cargo kafka-schema check
```

A directory is a subject, and a migration is the full schema of one subject version.
Migrations are numbered per subject, so two branches that change one subject conflict in git.
`subject.toml` holds the format, the compatibility level, the owner, the record the subject declares, the `.proto` source of a Protobuf subject, and the references to other subjects.
The lock records each migration's checksum and, once registered, its registry version and schema id.
The lock is what the [`Schema` derive](./index.md) reads offline, and a new migration rebuilds the crates that derive from it.

`migrate add` writes the next migration from the type's own schema.
It builds the crate's tests with `RUSTSTREAM_SCHEMA_EXPORT` set, so the derive emits no checks and a changed type exports before its migration exists.
`migrate run` registers the pending migrations in reference order.
For each, it sets the declared level, asks the registry's verbose compatibility check against every version the level names, prints the registry's messages, registers, and records the version and id in the lock.

The rules:
- Compatibility is checked twice: locally for Avro under the subject's level, and by the registry before registering.
- A Protobuf field number that disappears must appear in `reserved`, a rule the registry does not enforce.
- A JSON Schema migration is validated against its draft's metaschema, and an owned JSON subject's content model must suit its level: closed at `BACKWARD`, open at `FORWARD`.
- Drift is an error: a registry version no migration describes, a registered migration missing from the registry, or versions in another order. The fix is `migrate import` or a registry cleanup.
- A registered migration is immutable; a change is a new migration.
- Migrations only go forward: a registry version cannot be undone without breaking producers.

`RUSTSTREAM_SCHEMA_REGISTRY` names the registry, with credentials in the URL or in their own variables; the tool never prints them.
Without it, every command but `migrate import` and `migrate run` works from the migrations and the lock.

A CI pipeline:

```yaml
- run: cargo kafka-schema check          # offline: exports, checksums, local compatibility
- run: cargo build --locked              # offline: the derive reads the committed lock
- run: cargo kafka-schema prepare --check
  env: { RUSTSTREAM_SCHEMA_REGISTRY: ${{ secrets.REGISTRY_URL }} }
- run: cargo kafka-schema migrate run    # the deploy job, before the producers roll out
```

## Protobuf

`prost-build` generates the types from the `.proto`, so the type equals its source by construction.
[`schema::build::check_proto`](api:ruststream_rdkafka::schema::build::check_proto) in the build script fails the build when the compiled `.proto` differs from the subject's newest migration:

```rust
// build.rs
use ruststream_rdkafka::schema::build::check_proto;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(&["proto/payments.proto"], &["proto"])?;
    check_proto("payments-value")?;
    Ok(())
}
```
