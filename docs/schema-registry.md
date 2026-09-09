# Schema Registry

Payloads on registry-backed topics carry the Confluent wire format: a zero magic byte, a
big-endian 4-byte schema id, then the encoded datum. The `schema-registry` feature converts
payloads between that format and plain JSON as **middleware on the async edges** - the
subscription's delivery path on the way in, the app's publish pipeline on the way out. Handlers,
codecs and the rest of the runtime stay on plain JSON, the default codec.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_schema_registry.rs:wiring"
```

## The client

`SchemaRegistry::new` records the URL and sends no request until the first lookup. You can set
basic or bearer authentication on the client, and HTTPS goes through rustls. Every clone of the
client reads and writes one cache, so a schema id or a subject is fetched over the network once
per process.

Every registry request carries a deadline, ten seconds by default, which you can change with
`request_timeout`. A delivery or a publish waits on the lookup, so the deadline is what bounds a
registry that accepts the connection and then goes silent. The request returns an error when the
deadline expires, and both edges treat it like any other registry error.

## Consuming: transcode on the way in

`KafkaBroker::schema_registry(sr)` puts the client on the consume edge: every subscription of
that broker converts a framed delivery to plain JSON before the payload reaches the codec. A
JSON Schema payload keeps its bytes and loses the envelope. An Avro or Protobuf datum converts
through the registry schema its envelope names. Handlers are then ordinary subscribers on the
default codec:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_schema_registry.rs:handler"
```

A payload without the envelope passes through untouched, so a topic that mixes framed and plain
records keeps working. A delivery the middleware cannot convert passes through un-transcoded
with a warning: the schema lookup returned an error, or the payload's format feature is off. The
subscriber's decode failure policy then decides what happens to that delivery.

## Publishing: frame on the way out

`SchemaFrame` is the publish middleware; you add it app-wide with `RustStream::publish_layer`.
For every publish that goes through the app's pipeline it resolves the destination topic's
subject and frames the plain-JSON payload in that subject's **registered flavor**: a JSON Schema
subject keeps its bytes under the envelope, an Avro or Protobuf subject converts. The registry
decides a topic's wire format, and no publisher declares it.

The subject follows Confluent's `TopicName` strategy by default, `{topic}-value`. You can change
the mapping with `subject_strategy`, or pin one topic's subject with `subject(topic, subject)`.
The `RecordName` and `TopicRecordName` strategies name the subject after the record type, which
the publish path does not pass, so the subject comes out empty or ending in a dash. Either one
needs the subject pinned per topic.

A topic whose subject the registry does not know publishes untouched, so one app serves
registry-backed and plain topics without configuration. `SchemaFrame` remembers the unregistered
subject and logs it once; a subject registered afterwards takes effect after a restart or an
explicit `warm`.

A publish that cannot be framed returns an error: the subject lookup returned an error, or the
subject's schema rejects the payload. A publishing handler then nacks its delivery for a retry,
so no mis-framed record reaches the topic.

A subject resolves **lazily, on the first publish** to its topic, so a service whose subjects
already exist in the registry needs no startup step. A producer that owns its schemas registers
them at startup, from the message type itself:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_schema_registry.rs:types"
```

The reply type declares the destination: `#[outgoing(name = "confirmations")]` names the topic,
and the subscriber writes a bare `publish` clause.

`register` posts a definition you wrote yourself, and an identical schema keeps the id it already
has. `register_json::<T>` derives the JSON Schema from the type through schemars, which the crate
re-exports. `warm` registers nothing: it resolves an existing subject and caches it, for
deployments where producers must not create schemas (`auto.register.schemas` off).

Every publisher the runtime pairs publishes through the app's pipeline, so replies, transactional
publishers, the per-partition publishers and the exactly-once pipeline are framed with nothing to
configure at the include site. A publisher you pair off a connected broker yourself is outside
the pipeline and publishes exactly what it is given.

## Formats

- **JSON** (`schema-registry` alone, on the default `json` codec): the envelope goes on and comes
  off, the document itself is untouched. The document is not checked against the registered
  schema, so the handler type is the effective contract.
- **Avro** (`avro` feature): datums convert through the registry schema on both edges.
  `register_avro::<T>` derives the schema from the type, and the `AvroSchema` derive is
  re-exported:

    ```rust
    --8<-- "crates/ruststream-rdkafka/examples/kafka_avro.rs:wiring"
    ```
- **Protobuf** (`protobuf` feature): messages convert to JSON and back through descriptors
  compiled from the registry's `.proto` source. The well-known types are available; registry
  schema references beyond them are not resolved. Message indexes are read and written on both
  edges, so nested and multi-message schemas work. An outgoing message uses the schema's first
  top-level message; you can name another per topic with
  `SchemaFrame::message("topic", "pkg.Message")`:

    ```rust
    --8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf.rs:wiring"
    ```

An Avro or Protobuf message is converted through JSON on each edge. That is the cost of one
handler model: the same struct and the same codec for every wire format.
