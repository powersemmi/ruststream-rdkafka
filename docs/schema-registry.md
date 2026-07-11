# Schema Registry

Kafka deployments standardized on Confluent Schema Registry frame their payloads with the
Confluent wire format - a zero magic byte, a big-endian 4-byte schema id, then the encoded
datum - and keep the schemas themselves in the registry. The `schema-registry` cargo feature
integrates all of it as **broker middleware**: handlers, codecs, and the whole runtime stay on
plain JSON (the default codec), and the transcoding happens on the broker's own async paths.

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

`publisher.schema_format(SchemaFormat::Json | Avro | Protobuf)` frames every publish: the
plain-JSON payload the codec produced is transcoded to the wire format against the destination
subject's schema. The subject comes from the Confluent `TopicName` strategy (`{topic}-value`)
by default; `subject_strategy` changes the mapping and `schema_subject` pins one explicitly
(the `RecordName` strategies need the record's name, which the publisher does not know).

Subjects resolve **lazily on the async publish path** - when the subject already exists in the
registry there is no startup ceremony at all. Producers that own their schemas register them
once at startup:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_schema_registry.rs:types"
```

`register` takes a raw definition (idempotent registry-side), `register_json::<T>` derives a
JSON Schema from the type via schemars (re-exported), and `warm` only resolves an existing
subject - for deployments where producers must not create schemas, which is Confluent's own
production guidance (`auto.register.schemas` off). Publishing under a subject the registry
does not know is a clear error naming the fix.

Reply publishers (`TypedPublisher::new(publisher.schema_format(..))`), the partition-scoped
transactional publishers, and the EOS pipeline all compose unchanged: framing happens inside
the publish itself.

## Formats

- **JSON** (this feature, works with the default `json` codec alone): envelope on and off,
  documents untouched. Documents are not validated against the registered schema; the handler
  type's shape is the effective contract.
- **Avro** (`avro` feature): datum to JSON on consume, JSON to datum on publish, both against
  the registry schema; `register_avro::<T>` derives the schema from the type.
- **Protobuf** (`protobuf` feature): messages to JSON and back through descriptors compiled
  from the registry's `.proto` source (well-known types available; schema references beyond
  them are not resolved), message-indexes handled on both sides.

The transcoding trade-off is deliberate: one JSON hop per message on registry topics buys a
single uniform handler model - the same struct, the same codec, any wire format.
