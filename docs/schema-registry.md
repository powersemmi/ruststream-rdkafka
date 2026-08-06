# Schema Registry

Kafka deployments standardized on Confluent Schema Registry frame their payloads with the
Confluent wire format - a zero magic byte, a big-endian 4-byte schema id, then the encoded
datum - and keep the schemas themselves in the registry. The `schema-registry` cargo feature
integrates all of it as **middleware on the async edges** - the subscription's delivery path
on the way in, the app's publish pipeline on the way out - so handlers, codecs, and the whole
runtime stay on plain JSON (the default codec).

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_schema_registry.rs:wiring"
```

## The client

`SchemaRegistry` constructs synchronously and does no I/O until the first lookup; clones share
one schema cache, so an id or a subject resolves over the network once per process. Basic and
bearer authentication are builder options, TLS comes via rustls.

## Consuming: transcode on the way in

`KafkaBroker::schema_registry(sr)` makes every subscription transcode Confluent-framed
deliveries to plain JSON while still on the async consume path: the JSON Schema flavor loses
its envelope, Avro and Protobuf datums (with their features enabled) convert through the
registry schema the envelope references. Handlers are ordinary subscribers on the default
codec:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_schema_registry.rs:handler"
```

Non-framed payloads pass through untouched, so mixed topics keep working. A registry outage
or a framed payload whose format feature is off passes through un-transcoded with a warning,
and the handler's decode failure policy decides the delivery's fate - a broken registry never
stalls the consumer.

## Publishing: frame on the way out

`SchemaFrame` is publish middleware - the publish-side counterpart of the core's consume
layers - added app-wide with `RustStream::publish_layer`. For every publish flowing through
the app's pipeline it resolves the destination topic's subject and frames the plain-JSON
payload by the subject's **registered flavor**: a JSON Schema subject keeps its bytes under
the envelope, an Avro or Protobuf subject transcodes (with the matching feature). Nothing is
declared per publisher; the registry is the source of truth for what each topic speaks.

A topic whose subject the registry does not know publishes untouched - mixed registry/plain
topologies need no configuration. The miss is cached and logged once per subject, so plain
topics pay no per-publish round-trip (a subject registered later is picked up after a restart
or an explicit `warm`). A registry outage or a payload that does not fit the schema fails the
publish - a publishing handler nacks and retries rather than putting a mis-framed record on
the topic.

The subject comes from the Confluent `TopicName` strategy (`{topic}-value`) by default;
`subject_strategy` changes the mapping and `subject(topic, subject)` pins one explicitly (the
`RecordName` strategies need the record's name, which the publish path does not know).

Subjects resolve **lazily on the async publish path** - when the subject already exists in the
registry there is no startup ceremony at all. Producers that own their schemas register them
once at startup:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_schema_registry.rs:types"
```

`register` takes a raw definition (idempotent registry-side), `register_json::<T>` derives a
JSON Schema from the type via schemars (re-exported), and `warm` only resolves an existing
subject - for deployments where producers must not create schemas, which is Confluent's own
production guidance (`auto.register.schemas` off).

Publishing handlers, reply publishers, the partition-scoped transactional publishers, and the
EOS pipeline all compose unchanged: they publish through the app's pipeline, where the layer
sits. A publisher paired straight off a connected broker outside the runtime bypasses the
pipeline and publishes exactly what it is given.

## Formats

- **JSON** (this feature, works with the default `json` codec alone): envelope on and off,
  documents untouched. Documents are not validated against the registered schema; the handler
  type's shape is the effective contract.
- **Avro** (`avro` feature): datum to JSON on consume, JSON to datum on publish, both against
  the registry schema; `register_avro::<T>` derives the schema from the type (the `AvroSchema`
  derive is re-exported):

    ```rust
    --8<-- "crates/ruststream-rdkafka/examples/kafka_avro.rs:wiring"
    ```
- **Protobuf** (`protobuf` feature): messages to JSON and back through descriptors compiled
  from the registry's `.proto` source (well-known types available; schema references beyond
  them are not resolved), message-indexes handled on both sides - nested and multi-message
  schemas included. Outgoing messages default to the schema's first top-level message; pin
  another per topic with `SchemaFrame::message("topic", "pkg.Message")`:

    ```rust
    --8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf.rs:wiring"
    ```

The transcoding trade-off: one JSON hop per message on registry topics buys a
single uniform handler model - the same struct, the same codec, any wire format.
