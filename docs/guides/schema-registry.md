---
sidebar_position: 7
description: "Read and write Avro and JSON Schema records under the Confluent envelope, with every schema checked against the Schema Registry at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Schema Registry: Avro and JSON Schema

This guide reads and writes Avro and JSON Schema records under the Confluent envelope, with every schema checked against the Schema Registry at startup.

A registry-backed record starts with the Confluent envelope: a zero byte, then the four-byte id of the schema it was written with.
The schemas themselves live in the registry.
In RustStream the schema belongs to the codec, so handlers take and return plain serde structs.

```toml title="Cargo.toml"
[dependencies]
ruststream-rdkafka = { version = "0.7", features = ["avro"] }
apache-avro = { version = "0.22", features = ["derive"] }
```

The `avro` feature adds the Avro codec on top of `schema-registry`, which brings the registry client and the envelope.
`apache-avro` provides the `AvroSchema` derive; use the version the crate depends on.

## The models

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#models"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#models"
```

</TabItem>
</Tabs>

`AvroSchema` derives each struct's Avro schema.
`currency` arrived in the second version of the invoice schema.
`#[avro(default)]` is the value a record written without it reads as.

## The handler

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#handler"
```

</TabItem>
</Tabs>

`issue_receipt` reads an invoice and answers with a receipt on the `receipts` topic.
Nothing in it knows about Avro or the envelope.

## The registry, the prefetch and the codec

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#service"
```

</TabItem>
</Tabs>

[`SchemaRegistry::new`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::new) records the registry's URL and sends nothing yet.
Every clone of it shares one cache, so a schema is fetched once per process.
[`SchemaPrefetch`](api:ruststream_rdkafka::schema_registry::SchemaPrefetch) does the network work, and [`schema_prefetch`](api:ruststream_rdkafka::KafkaBroker::schema_prefetch) attaches it to the broker.
When the broker connects, the prefetch resolves every subject the codecs publish under.
When a record arrives, it fetches the writer schema the envelope names, before the codec runs.
So the codec itself never waits on the network.

[`AvroCodec::registry`](api:ruststream_rdkafka::avro::AvroCodec::registry) builds the Avro codec from the prefetch.
[`register`](api:ruststream_rdkafka::avro::AvroCodec::register) names the subject a type's values are framed under: receipts go out under `receipts-value`.
Decoding needs no registration, since each record names its own writer schema.

`with_broker_codec` decodes every handler of the scope with the Avro codec.
A reply is encoded with the codec named at its mount site, and with the default JSON codec when none is named.
So `out_reply(KafkaPublish::default()).codec(avro)` names the Avro codec for the receipts.

## Reading an older writer schema

[`resolve_onto`](api:ruststream_rdkafka::avro::AvroCodec::resolve_onto) makes Avro resolve every record onto the reader schema, here `Invoice::get_schema()`.
A record written under the first version of the schema has no `currency`.
Resolution fills it from the field's default, so the handler reads `"EUR"`.
A reader schema applies to every record its codec decodes, so a codec with one serves one type.
A second type with its own reader schema gets its own codec, in its own router.

## Registering subjects and a missing subject

The service registers no schemas: whoever owns a topic registers its subject, for example in a deploy step.
[`register_avro`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register_avro) registers a type's Avro schema under a subject, [`register_json`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register_json) its JSON Schema, and [`register`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register) a schema given as text.

When a subject a codec publishes under is not in the registry, the broker does not connect, and the error names the subject.
[`MissingSubject`](api:ruststream_rdkafka::schema_registry::MissingSubject) chooses what happens instead:

| Policy | What it does |
|---|---|
| `Refuse` *(default)* | the service does not start |
| `AutoRegister` | registers the type's own schema under the subject, and logs a warning |
| `PublishUnframed` | publishes the bare datum with no envelope, and logs a warning |

Set it with [`on_missing_subject`](api:ruststream_rdkafka::schema_registry::SchemaPrefetch::on_missing_subject) before the codecs are built from the prefetch.

## Compatibility at startup

At connect, each registered type's schema is checked the way registering it would be checked.
The subject's compatibility level decides which versions it is checked against: the latest one, or every version under a `_TRANSITIVE` level.
A model that drifted from its subject stops the service, and the registry's account of the difference is in the error.
[`check_compatibility(false)`](api:ruststream_rdkafka::schema_registry::SchemaPrefetch::check_compatibility) turns the check off, for a subject whose level is `NONE`.

## JSON Schema under the envelope

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#dispute"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#dispute"
```

</TabItem>
</Tabs>

[`SchemaFramed`](api:ruststream_rdkafka::schema_registry::SchemaFramed) wraps the core's JSON codec in the envelope.
Decoding steps over the envelope and reads the JSON document inside it.
Encoding frames a value with the id of the subject that [`register`](api:ruststream_rdkafka::schema_registry::SchemaFramed::register) names for its type, as with Avro.
In the service above, a router gives the `disputes` subscription this codec, since the most specific codec wins.
`JsonSchema` derives the type's JSON Schema, which `register_json` registers.

## A pinned local schema

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#archive"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#archive"
```

</TabItem>
</Tabs>

[`AvroCodec::local`](api:ruststream_rdkafka::avro::AvroCodec::local) holds one schema and uses it for every record.
The topic carries the bare Avro datum: no envelope, no registry and no network on the path.
This archive service therefore runs, and is tested, without a registry.

## The transcode path

A service can also keep plain serde models on the default JSON codec and convert at the broker's edges.
[`KafkaBroker::schema_registry`](api:ruststream_rdkafka::KafkaBroker::schema_registry) converts every framed delivery to JSON before the codec runs.
[`SchemaFrame`](api:ruststream_rdkafka::schema_registry::SchemaFrame), added with `RustStream::publish_layer`, frames each outgoing JSON payload in the format its destination subject holds: Avro, JSON Schema or Protobuf.
A topic with no subject in the registry is published as it is.
This path costs a JSON conversion per message, and it does not resolve records onto a reader schema.
A broker uses one path or the other: with `schema_registry` attached, a codec would be handed JSON.

## Bringing another registry crate

[`SchemaRegistry::with_client`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::with_client) takes a [`RegistryClient`](api:ruststream_rdkafka::schema_registry::RegistryClient) of your own, such as one over another HTTP stack or another client crate.
The codecs, the prefetch and the transcode path then reach the registry through it.

A crate that does the whole schema work itself, such as `schema_registry_converter`, works on bytes instead.
The handler takes the record's bytes through a `#[derive(Deserialized)]` type and returns the encoded reply through a `#[derive(Serialized)]` type.
Only bytes cross between that crate and RustStream, so its own `apache-avro` version does not have to match this crate's.
