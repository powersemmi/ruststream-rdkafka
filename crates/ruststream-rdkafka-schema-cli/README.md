# cargo kafka-schema

Schema migrations for a Confluent Schema Registry, kept in the repository of the service that
owns them. The repository is the source of truth for the subjects the service owns; the tool
synchronizes them with the registry. The `Schema` derive of
[`ruststream-rdkafka`](https://crates.io/crates/ruststream-rdkafka) (feature `schema-check`)
checks the Rust types against the same migrations when the crate compiles.

```sh
cargo install ruststream-rdkafka-schema-cli
```

## The migrations directory

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
    0002.avsc
```

- A directory is a subject. A migration is the full schema of one subject version, numbered
  `NNNN_<description>` with no gaps.
- `subject.toml` holds `format` (`avro`, `json`, `protobuf`), `compatibility` (a Confluent
  level), `owner` (`service` by default, or `external`), `record` (the record the subject
  declares), `source` (the `.proto` a Protobuf subject compiles) and `[[references]]` (`name`,
  `subject`) for schemas that import other subjects.
- `kafka-schema.lock` records, per migration, the SHA-256 of its normalized content (LF line
  endings, minified JSON with sorted keys), and once registered its registry version and schema
  id; per subject, the registry's version list as of the last sync.
- `schemas/` sits beside the crate's `Cargo.toml`; `RUSTSTREAM_SCHEMA_DIR` moves it.

## Commands

Run them in the crate's directory, or name the crate with `--manifest-path`.

```sh
cargo kafka-schema migrate add orders-value add_item
cargo kafka-schema migrate add orders-key create_order_key --record com.acme.orders.OrderKey
cargo kafka-schema migrate add payments-value init --source proto/payments.proto
cargo kafka-schema migrate import inventory-value
cargo kafka-schema migrate status
cargo kafka-schema migrate run [--dry-run]
cargo kafka-schema prepare [--check]
cargo kafka-schema check
```

- `migrate add` writes the next migration from the crate's current schema. For Avro and JSON
  Schema it runs the crate's tests with `RUSTSTREAM_SCHEMA_EXPORT` set: the hidden test the
  `Schema` derive emits per type writes the type's own schema (`AvroSchema::get_schema()`, or the
  `schemars` schema). For Protobuf it snapshots the subject's `source`. The first migration of a
  subject names its record with `--record` (or its `.proto` with `--source`), and may set
  `--compatibility`. Nothing is written when the schema equals the newest migration.
- `migrate import` pulls every registry version of a subject as `NNNN.<ext>` migrations: an
  external subject, or a baseline of a subject registered before its migrations existed
  (`--owner service`).
- `migrate status` lists, per subject, its migrations with their registry versions, the pending
  ones, and drift.
- `migrate run` registers every pending migration of the owned subjects, in reference order: it
  sets the declared compatibility level, asks the registry's compatibility check against every
  version the level names, prints the registry's messages, registers, and records the version
  and id in the lock after each registration. `--dry-run` asks the questions and changes nothing.
- `prepare` refreshes the lock from the registry, or offline from the migrations alone.
  `--check` writes nothing and fails when the lock is stale.
- `check` is the CI gate: exported schemas equal their newest migrations, a Protobuf `source`
  equals its newest migration, the lock's checksums hold, and the migration line passes the local
  checks. With a registry it also fails on drift and asks the registry about each pending
  migration.

## The rules

- **Compatibility is checked twice.** Locally, an Avro migration is checked against the earlier
  ones under the subject's level with `apache-avro`'s schema resolution (an undeclared level
  reads as `BACKWARD`, the registry's default). Before registering, the registry is asked through
  `/compatibility/subjects/{subject}/versions?verbose=true`, which covers every version a
  `_TRANSITIVE` level names, and its messages are printed as they come.
- **Protobuf field numbers stay reserved.** A field number that disappears from a message must
  appear in its `reserved` list. The registry accepts moving `item = 2` to `item = 3` under
  `BACKWARD`, and old records then lose the field silently.
- **JSON Schema is validated, and its content model matches the level.** A JSON Schema migration
  must be valid under its draft's metaschema (drafts 04, 06, 07, 2019-09 and 2020-12). At a
  `BACKWARD` level `migrate add` refuses an open object, because adding a property to it later is
  `PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL`; at a `FORWARD` level it refuses a closed one. The fix is
  `#[serde(deny_unknown_fields)]` on the type, or its removal. JSON Schema compatibility is asked
  of the registry.
- **Drift is an error.** The registry holding a version no migration describes, a registered
  migration missing from the registry, and versions in another order than the migrations each
  fail `prepare`, `status`, `run` and an online `check`. The fix is `migrate import` or a registry
  cleanup; the tool never overwrites the registry.
- **A registered migration is immutable.** Its checksum is fixed in the lock; a change, or its
  deletion, is an error. A change is described with a new migration.
- **Migrations only go forward.** A registry version cannot be undone without breaking producers
  or making records unreadable.

## Online and offline

`RUSTSTREAM_SCHEMA_REGISTRY` (or `--registry`) names the registry. Credentials ride its user part
(`https://user:pass@registry:8081`) or come from `--registry-user` and `--registry-password`
(`RUSTSTREAM_SCHEMA_REGISTRY_USER`, `RUSTSTREAM_SCHEMA_REGISTRY_PASSWORD`), or a bearer token
from `--registry-token` (`RUSTSTREAM_SCHEMA_REGISTRY_TOKEN`). The tool never prints them, its
`--help` included. Without a URL, or with `RUSTSTREAM_SCHEMA_OFFLINE=true`, every command but
`migrate import` and `migrate run` works from the migrations and the lock.

A CI pipeline:

```yaml
- run: cargo kafka-schema check          # offline: exports, checksums, local compatibility
- run: cargo build --locked              # offline: the derive reads the committed lock
- run: cargo kafka-schema prepare --check
  env: { RUSTSTREAM_SCHEMA_REGISTRY: ${{ secrets.REGISTRY_URL }} }   # when CI has credentials
- run: cargo kafka-schema migrate run    # the deploy job, before the producers roll out
```

## Protobuf in the build script

`prost-build` generates the message types from the `.proto`, so the build script checks the
source against the subject's newest migration:

```rust,ignore
// build.rs
use ruststream_rdkafka::schema::build::check_proto;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(&["proto/payments.proto"], &["proto"])?;
    check_proto("payments-value")?;
    Ok(())
}
```

## License

Apache-2.0.
