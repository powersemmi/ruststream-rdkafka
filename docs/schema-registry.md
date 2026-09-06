# Schema Registry

Kafka deployments standardized on Confluent Schema Registry frame their payloads with the
Confluent wire format - a zero magic byte, a big-endian 4-byte schema id, then the encoded
datum - and keep the schemas themselves in the registry. The `schema-registry` cargo feature
covers both halves, and there are three ways to consume them. **Reach for the codec first.**

**The codec** puts the schema where a serializer belongs. `AvroCodec` holds the schema, handlers
stay ordinary functions over ordinary structs, and nothing about the wire appears in a signature -
which is what a codec is for, and what makes this the default choice. Avro fits the position
exactly, being a schema-driven format with a serde front end; a JSON payload under the envelope is
the core's own `JsonCodec` inside `SchemaFramed`.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_avro_codec.rs:handler"
```

**The byte lanes** put the wire form in the handler's signature instead: a delivery arrives as an
`IncomingFrame` - the schema id and the datum, byte for byte as they came off the topic - and the
handler decides what to make of it. Reach for this when the handler must see the wire itself: a
topic carrying more than one schema, a router dispatching on the id, a service forwarding frames
it never decodes. It is also the only path for Protobuf, whose `prost` messages are not serde
types and so cannot ride a codec at all.

**The transcode** converts at the broker's edges, so handlers keep plain serde models on the
default codec and never see the wire. It is the compatibility path: the right choice for a service
that must not carry generated types or Avro-derived models, at the cost of a JSON hop per message
and of losing schema resolution, since a JSON handler has no reader schema to resolve onto.

None of the three mix on one broker. `KafkaBroker::schema_registry(sr)` attaches the transcode to
every subscription that broker opens, so a codec or a frame-reading handler on it would be handed
JSON; the codec and the lanes take `KafkaBroker::schema_prefetch(..)` instead, which resolves
schemas without touching a payload.

## The codec

The schema source is part of the codec, and there are two.

`AvroCodec::local(schema)` pins one schema: a bare datum on the wire, no envelope, no registry,
and no I/O anywhere on the path - a fixed-schema topic, and every unit test.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_avro_codec.rs:local"
```

`AvroCodec::registry(&prefetch, subject)` speaks the Confluent wire format: encoding frames with
the id its subject holds, and decoding reads each delivery with the writer schema that delivery's
envelope names - so a producer still on an older version stays readable.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_avro_codec.rs:wiring"
```

`SchemaPrefetch` is the async half, and it exists because a `Codec` is synchronous on both ends
while a registry lookup is not. That meeting cannot be arranged inside `encode` or `decode`:
blocking a runtime worker from a sync function is not an option, and guessing a schema is
corruption. So the lookups move to the two places that are already async and already know when
they must happen - the broker's `connect`, for the subjects the codecs publish under, and the
delivery path, for the writer schema an arriving envelope names. A subject that does not exist
therefore fails startup rather than the first publish, and an id the prefetch could not resolve
becomes a decode failure the subscription's failure policy settles, never a silent guess.

### Schema evolution

Reading a datum with its writer schema recovers what the writer wrote, and no more. A field the
writer never had is filled from a *reader* schema's default, which is Avro's own schema
resolution: `resolve_onto(schema)` names the schema this consumer expects. Without it a model
carrying a field the writer lacks fails to deserialize, which is the honest outcome - the value is
genuinely not on the wire.

### JSON under the envelope

`SchemaFramed::new(&prefetch, subject, JsonCodec)` is the JSON registry codec. The envelope is
separable here because a JSON document is self-describing: the id says which schema it claims to
conform to, and the document parses without it. An Avro datum cannot be read without the schema
its id names, which is why `AvroCodec` owns its envelope rather than riding this wrapper - the
line between them is that property, not the two formats.

There is no local JSON codec, because there is nothing for it to do: a local JSON Schema would be
a schema the codec never consults, so the local JSON case is the core's own `JsonCodec`.

The wrapper frames and does not validate. A registry's JSON Schema is a compatibility contract the
registry enforces between versions, and checking every message against it would mean carrying a
JSON Schema validator and paying it per delivery - which is why Confluent's own serializer makes
that optional too. Validation belongs to the inner codec, and the inner codec is named right at
the call site: pass one that validates instead of a plain one.

### What the client remembers

`SchemaCachePolicy` follows a property of the registry rather than a preference: **a schema id is
immutable**. An id is assigned per distinct schema definition, globally and by content, so a new
version of a subject mints a new id and leaves the old one resolving to the old schema for ever,
while a subject's *latest version* moves whenever someone registers one.

So the two halves need opposite treatment. Id-keyed entries need a bound and no expiry - a TTL
over them could only cause a refetch returning identical bytes - and the bound matters because a
consumer meets one id per writer version, which is small in a healthy topology and unbounded in a
broken one. Subject-keyed entries need an expiry and no bound. Confluent's own clients scope their
`latest.cache.ttl.sec` to the latest-version caches for the same reason. `SchemaCachePolicy::Disabled`
turns the cache off entirely - a real configuration rather than a zero TTL in disguise, with the
consequence that the synchronous codecs, which read the cache and cannot await a miss, cannot work
under it.

## The client

`SchemaRegistry` constructs synchronously and does no I/O until the first lookup; clones share
one schema cache, so an id or a subject resolves over the network once per process. Basic and
bearer authentication are builder options, TLS comes via rustls.

## The byte lanes

The envelope is a lane type on both ends: `IncomingFrame` arrives through the core's
`Deserialized` lane, `OutgoingFrame` leaves through `Serialized`. What rides the lane is the
envelope rather than the message model, and that is forced rather than chosen. Resolving a schema
id is a registry conversation and therefore `async`, while `Deserialized::from_payload` is a sync
associated function with no context to reach a registry from; and for Avro the model type is a
serde type, which the core's lanes are reserved against (`MessageWire`, `ReplyShape` and `Input`
are blanket-implemented for every `Serialize` / `DeserializeOwned` value - which is why
`#[wire(prost)]` works for a `prost` message and an equivalent `#[wire(avro)]` cannot exist). So
the wire form rides the lane, the value's conversion is one call, and no decode hides an I/O stall
or reaches for a process-wide registry singleton.

Reading resolves the writer schema the envelope names onto the reading type's own, which is what
makes a producer still on an older version of the subject readable. Publishing resolves its
subject once, at startup, so the publish itself does no I/O and a subject that is missing or
incompatible fails the app's startup rather than its first message:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_avro_lanes.rs:app"
```

`Subject::register` publishes the type's own schema and takes the id back; `Subject::resolve`
looks the id up without registering, for deployments where producers must not create schemas
(Confluent's own guidance, with `auto.register.schemas` off); `Subject::pinned` takes an id the
service already knows, for a pinned deployment, a replay tool, or a test with no registry in
front of it.

The Protobuf half is the same shape with one difference: reading needs no registry at all. The
envelope's message-index path only says which message of the schema was written, and the reading
type has already decided which one it reads, so `protobuf::decode_framed` is a plain synchronous
call. Only the publish side resolves anything.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_lanes_testing.rs:handler"
```

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

## Formats on the transcoding path

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

The transcoding trade-off: one JSON hop per message on registry topics buys a single uniform
handler model - the same struct, the same codec, any wire format - and gives up what the JSON
document cannot carry. Avro types JSON has no shape for do not survive it, and a datum written
under an older version of the subject is decoded, not resolved: the handler sees the writer's
fields, with no reader schema to fill in what the producer never wrote. Reach for it when a
service must keep plain serde models on a registry-backed topic; reach for the lanes otherwise.

## Testing a lane handler

A lane handler is an ordinary handler, so `TestApp` and the in-process `KafkaTestBroker` drive it
with no cluster - and, on the Protobuf side, with no registry either, since reading needs none.
An `OutgoingFrame` is a publish value like any other, so the injection is the ordinary typed one
and the frame's own bytes go on the topic untouched:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_lanes_testing.rs:testapp"
```

Without the `macros` feature the same handler is a `Handle` impl over the same two axes; the
mount names the subscription and the reply's destination, and nothing in either knows a codec
exists:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_lanes_manual.rs:handler"
```

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_lanes_manual.rs:mount"
```
