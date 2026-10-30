---
sidebar_position: 1
title: Schema derive
description: "#[derive(Schema)] checks a message type against the newest migration of every subject in schemas/ that declares its record, when the crate compiles."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Schema derive

`#[derive(Schema)]` checks a message type against the newest migration of every subject in `schemas/` that declares its record, when the crate compiles.

The repository holds the schemas this service owns, one directory per subject, one file per subject version.
[`cargo kafka-schema`](./migrations.md) writes those files and keeps them in step with the registry.
The derive comes with the `schema-check` feature as [`schema::Schema`](api:ruststream_rdkafka::schema::Schema).

The type is bound to its migrations by its record's full name: `#[avro(namespace = ..)]` plus the type's name, `com.acme.orders.Order`.
The derive finds every subject whose newest migration declares that record and checks the type against each of them.
A nested record without a subject of its own is checked through every subject that holds it.
A type no migration declares carries no check.

For a subject this service owns, the type equals the newest migration.
Field names, kinds, nullability (an `Option` against `["null", T]`), nested record names, `#[avro(default = ..)]` defaults and the whole canonical form are compared.
A field with `#[avro(with = ..)]` or `#[serde(flatten)]` is checked for its presence only.
For an external subject (`owner = "external"`) the type is a reader of every pulled version under Avro's resolution rules: `int` promotes to `long`, `float` or `double`, `string` and `bytes` convert, and a field the writer lacks needs a default.

An enum whose variants each wrap one model, the core's `#[derive(InMessages)]` set, derives `Schema` too.
It maps each variant to its model's record full name, and each model is checked by its own derive.

The check is constants, uncalled code and trait bounds.
A release binary holds no schema text, and the delivery and publish paths are the same as without it.
Schema ids stay resolved at startup by [`SchemaPrefetch`](../schema-registry/schema-prefetch.md).

Switches, read when the crate compiles:
- `RUSTSTREAM_SCHEMA_DIR` names another directory than `schemas/` beside `Cargo.toml`.
- `RUSTSTREAM_SCHEMA_REGISTRY=<url>` also compares the lock with the registry's version list, once per subject per compiler process: drift fails the build, and a migration not registered yet is a warning.
- `RUSTSTREAM_SCHEMA_OFFLINE=true` reads the migrations and the lock only. It is the setting for rust-analyzer (`rust-analyzer.cargo.extraEnv`).
- `RUSTSTREAM_SCHEMA_EXPORT=<dir>` emits no checks: `cargo kafka-schema` sets it to export a changed type before its migration exists.

Recursive Avro records are checked by `cargo kafka-schema check` only, and a named type referenced twice is compared in its expanded form.

## Example

`Order` and its nested `Item` equal the newest migration of `orders-value`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kafka_schema_check.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kafka_schema_check.rs#app"
```

</TabItem>
</Tabs>

A mismatch is a compile error at the field that names the subject and the migration:

```text
error[E0277]: `i32` cannot carry the Avro type `long` declared at
              `Order__id__in__orders_value__0002_add_item`
```

A field the record lacks, or a field the record declares without a default that the type lacks, is an error at the field with the record, the subject and the migration.
A nested record that differs fails a constant comparison that names it.
