# Schema Registry

Kafka deployments standardized on Confluent Schema Registry frame their payloads with the
Confluent wire format - a zero magic byte, a big-endian 4-byte schema id, then the encoded
datum - and keep the schemas themselves in the registry. The `schema-registry` cargo feature
covers both halves, and there are two ways to consume them. **Reach for the codec first.**

**The codec** puts the schema where a serializer belongs. `AvroCodec` holds the schema, the handler
takes the model and returns the model, and nothing about the wire appears in a signature - which is
what a codec is for, and what makes it the one way to read and write an Avro or JSON Schema payload
here. Avro fits the position exactly, being a schema-driven format with a serde front end; a JSON
payload under the envelope is the core's own `JsonCodec` inside `SchemaFramed`.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_avro_codec.rs:handler"
```

**The transcode** converts at the broker's edges, so handlers keep plain serde models on the
default codec and never see the wire. It is the compatibility path: the right choice for a service
that must not carry generated types or Avro-derived models, at the cost of a JSON hop per message
and of losing schema resolution, since a JSON handler has no reader schema to resolve onto.

The two do not mix on one broker. `KafkaBroker::schema_registry(registry)` attaches the transcode to
every subscription that broker opens, so a codec on it would be handed JSON; the codec takes
`KafkaBroker::schema_prefetch(..)` instead, which resolves schemas without touching a payload.

Protobuf is not a third choice. A `prost` message is not a serde type, so it can never reach the
codec position; it serializes itself instead, and the generated type is what the handler takes and
returns. That path is [Protobuf](#protobuf) below.

## What each format reaches

The paths do not cover the three formats evenly. One blank is forced by the type system and will
never close; the others are gaps, and this table marks which is which rather than leaving a reader
to guess.

| | Avro | JSON | Protobuf |
| --- | --- | --- | --- |
| Codec | `AvroCodec::local`, `AvroCodec::registry` | `SchemaFramed<JsonCodec>` | forced blank, see below |
| Handler over the message itself | the codec | the codec | `#[wire(..)]` + `ProtobufFrame` |
| Transcode | yes | yes | yes |
| Schema read off the type | `AvroSchema` | `schemars::JsonSchema` | **no** |
| Subject registered from the type | `register_avro::<T>` | `register_json::<T>` | **no** |
| Schema references (`import`) | not applicable | not applicable | **not resolved** |
| Subjects resolved at `connect` | codec | codec | forced blank |
| `MissingSubject` | codec | codec | forced blank |
| `check_compatibility` at startup | codec | codec | forced blank |
| Writer schema resolved per delivery | codec, which needs it | not needed | not needed |
| Shared id and subject cache | every path | every path | every path |

**The forced blank.** Protobuf can never be a codec. `Codec::encode<T: Serialize>` is the gate, and
a `prost` message is not a serde type, so it cannot reach the codec position at all - the envelope
is its only home, and that is a property of the format rather than an unfinished corner. Every
Protobuf row that reads "forced blank" is the same fact one step removed: `SchemaPrefetch` warms
what a codec registered, so with no codec there is nothing for it to warm.

What the blank costs is smaller than it looks, because the row below it is filled. The *outcome* a
codec buys - a handler over ordinary types, with nothing about the wire in its signature - Protobuf
reaches by another route, described under [Protobuf](#protobuf). What it does not reach is the
prefetch's machinery, which hangs off the codec position itself.

**Why the prefetch rows read "codec" and not "Avro".** Nothing in that machinery is Avro-only.
`MissingSubject`, the connect-time subject resolution and the startup compatibility check all live
on `SchemaPrefetch` and apply to whatever a codec registered, so JSON under `SchemaFramed` gets
them today exactly as Avro does - including `AutoRegister`, which puts a `schemars`-derived JSON
Schema back.

**Protobuf answers the same question where the publish happens, not with a policy.** The framing
resolves the destination topic's subject, and a destination the registry does not describe is
passed through by the app-wide layer and refused by the mount-site policy. That difference is
deliberate and explained under [Setting it once for a whole app](#setting-it-once-for-a-whole-app),
so there is no `MissingSubject` to add here.

**The two real gaps, both Protobuf, both one piece of work.** A Protobuf type cannot hand over its
own schema, so a service writes its `.proto` twice - once as the file `prost-build` compiles and
once as a string literal to register - with nothing tying the copies together, which is why nothing
here registers a Protobuf schema from a type. And nothing here resolves registry schema references,
so a `.proto` that imports anything the compiled pool does not already carry is out of reach: the
well-known `google/protobuf/*` types resolve, `confluent/*` (which the registry itself treats as
ambient) does not, and any import of your own needs the `references` field this crate never writes
or reads.

## The codec

The schema source is part of the codec, and there are two.

`AvroCodec::local(schema)` pins one schema: a bare datum on the wire, no envelope, no registry,
and no I/O anywhere on the path - a fixed-schema topic, and every unit test.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_avro_codec.rs:local"
```

`AvroCodec::registry(&prefetch)` speaks the Confluent wire format, and `register::<T>(subject)`
says what it publishes: encoding frames each value with the id of its type's subject, and decoding
reads every delivery with the writer schema that delivery's envelope names - so a producer still on
an older version stays readable.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_avro_codec.rs:wiring"
```

`SchemaPrefetch` is the async half, and it exists because a `Codec` is synchronous on both ends
while a registry lookup is not. That meeting cannot be arranged inside `encode` or `decode`:
blocking a runtime worker from a sync function is not an option, and guessing a schema is
corruption. So the lookups move to the two places that are already async and already know when
they must happen - the broker's `connect`, for the subjects the codecs publish under, and the
delivery path, for the writer schema an arriving envelope names. A subject that does not exist is
therefore settled while the app is starting rather than at its first publish, and an id the
prefetch could not resolve becomes a decode failure the subscription's failure policy settles,
never a silent guess.

### Registration is the publish side only

**One codec, as many message types as a router mounts through it.** Registration says which subject
a type's values are framed under, and decoding needs none of it: the writer schema comes off the
envelope's id, so a subscription carrying five types decodes all five through a codec that was told
about none of them. That asymmetry is the reason one codec serves a whole scope; it is also why
`register` lives on the publish half of the story and nothing on the consume half mirrors it.

The subject is looked up per publish by **the name serde gives the value's type**. `TypeId` would be
the obvious key and is unavailable - it needs `T: 'static`, and `Codec::encode` bounds `T` by
`Serialize` alone - while every derived `Serialize` hands the name to `serialize_struct` before
touching a field, so a probing serializer reads it and stops there. It is the right key on merit
too: it is the same name `AvroSchema` writes into the Avro record, which is what registration reads
when it has a type but no value. `std::any::type_name` is deliberately not used, since the standard
library promises neither uniqueness nor stability for it.

Two registered types with the same serde name would make a publish ambiguous, so that is **rejected
at registration**, before the app runs. Give one of them a distinct `#[serde(rename = "..")]`.

The cost that remains: a type you forgot to register is a **runtime error at its first publish**,
naming the type and listing the ones the codec does carry. Making it a compile error would mean the
codec's type carrying its message list, which is the type-parameter-per-mount-site shape this design
exists to avoid.

### A reader schema is tuning, not a requirement

`resolve_onto(schema)` turns on Avro's own resolution, so a field the writer never had is filled
from a reader schema's default. It applies to **every** delivery this codec decodes - decode has a
type but no value, so there is no name to key a per-type reader schema by - which makes it a setting
for a codec that reads one type. A codec serving several uses `#[serde(default)]` on the Rust side
instead, which covers the same ground per field.

### When the subject is gone

A subject can be deleted from a registry while a producer is running, so the reaction is a policy
rather than a fixed answer: `SchemaPrefetch::on_missing_subject`, an enum whose default is
**`Refuse`**. Creating subjects in someone else's registry as a side effect of starting up is worse
than not starting. `AutoRegister` puts the type's own schema back and warns, and `PublishUnframed`
writes the bare datum and warns. Each warning names the subject, the flavour and the schema,
because a bare "schema missing" tells an operator nothing.

The vocabulary is Confluent's on purpose. Their serializers cover this ground with three settings,
and two of them describe this crate's normal path rather than the policy: encoding always writes
with the schema the framed id names, which is `use.latest.version`, and `check_compatibility` is
`latest.compatibility.strict`. What is left for the enum is the question those two answer between
them - when the subject is not there at all, does the producer create it (`AutoRegister`, their
`auto.register.schemas=true`), refuse (`Refuse`, their `auto.register.schemas=false` with
`use.latest.version=true`), or go without (`PublishUnframed`, which has no Confluent counterpart).
It is an enum rather than three booleans because the booleans are not independent -
`use.latest.version` means nothing while auto-registration is on - and a combination that means
nothing is what an enum keeps unrepresentable.

`check_compatibility` is on by default, as it is at Confluent, and it closes the gap the earlier
subject-on-the-type design could not: at `connect` each registered type's schema is checked against
the version its subject already holds, and a model that has drifted stops the app with the
registry's own account of the difference - down to the field - rather than surfacing as a consumer
that cannot read what was written.

The two deletions differ, and this was checked against a live registry rather than assumed. A
**soft** delete hides the subject - its `versions/latest` answers 404 - while `GET /schemas/ids/{id}`
still returns the schema, so **consumers keep working** and only the producer is stuck. A
**permanent** delete removes the id too, and then nothing decodes a record naming it: no policy here
helps a consumer, because the schema is gone for everyone. Re-registering afterwards mints a *new*
id, so records already on the topic stay unreadable.

This is where the codec path and `SchemaFrame` part company, and deliberately. The transcoding
layer treats a subject the registry does not know as "this topic is not registry-backed" and
publishes untouched, because there the condition is genuinely ambiguous - it resolves subjects for
every topic an app publishes to, most of which are not registry-backed at all. On the codec path
the ambiguity is gone: writing `register::<Order>("orders-value")` *declares* the topic
registry-backed, so an absent subject is an anomaly rather than a plain topic, and the default says
so.

### JSON takes the same shape

`SchemaFramed::new(&prefetch, JsonCodec).register::<Order>("orders-value")` is the same builder over
the same name-keyed map, for the same reason. Registration captures the type's JSON Schema through
`schemars`, so `AutoRegister` has something to put back.

### Naming the registry once

The registry is named twice in an app and never at a mount site: once when the prefetch is built,
and once when it is attached to the broker. Every codec is minted from that one prefetch, so what
varies per mount is the subject - which is what actually differs per mount.

The scoping is the core's own codec cascade, and it already does what schemas need: a codec set
for a broker scope covers every handler in it, and a router mounted inside overrides it for the
handlers it carries. Most specific wins, exactly as for any other codec.

One codec now serves a whole scope, so the override is for the settings that cannot be shared. The
reader schema is the clearest: it applies to every delivery its codec decodes, so a codec carrying
one can only serve a single reading type, and the handler that wants Avro's resolution gets its own
codec in its own router while the scope's keeps serving the rest. A subtree publishing to a
different registry, or under a different `MissingSubject` policy, is minted the same way.

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_avro_codec.rs:cascade"
```

There is no app-wide level, because the core scopes codecs at the broker and the router, and a
registry is a per-cluster thing in any case. What the core does not offer is a *partial* override -
"take the scope's codec and change only one setting" - since a codec is an opaque value to it. The
prefetch minting a second codec is that override, and it costs one line.

### Schema evolution

Reading a datum with its writer schema recovers what the writer wrote, and no more. A field the
writer never had is filled from a *reader* schema's default, which is Avro's own schema
resolution: `resolve_onto(schema)` names the schema this consumer expects. Without it a model
carrying a field the writer lacks fails to deserialize, which is the honest outcome - the value is
genuinely not on the wire.

### JSON under the envelope

`SchemaFramed::new(&prefetch, JsonCodec)` is the JSON registry codec. The envelope is
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

`SchemaRegistry::new` records the URL and sends no request until the first lookup. You can set
basic or bearer authentication on the client, and HTTPS goes through rustls. Every clone of the
client reads and writes one cache, so a schema id or a subject is fetched over the network once
per process.

Every registry request carries a deadline, ten seconds by default, which you can change with
`request_timeout`. A delivery or a publish waits on the lookup, so the deadline is what bounds a
registry that accepts the connection and then goes silent. The request returns an error when the
deadline expires, and both edges treat it like any other registry error.

## Protobuf

A generated message arrives as itself and leaves as itself. The handler is an ordinary function
over ordinary types, exactly as it is on the Avro codec:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf_plain.rs:handler"
```

The types carry the wire, because a `prost` message is not a serde type and so can never reach the
codec position. They ride the core's byte lanes instead - selected by the type and reserved for
types that are *not* serde types, which is why `#[wire(prost)]` works for a `prost` message and an
equivalent `#[wire(avro)]` cannot exist:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf_plain.rs:types"
```

The two halves are asymmetric, and knowing why is what makes the shape read as one thing rather
than two. **Reading needs no registry**, so it can happen in the type: Protobuf's compatibility
model is the wire format's own - fields are tag-addressed, a field the reader does not know is
kept as an unknown field, one the writer never wrote takes its default - so a delivery decodes
against the reader's own generated type, and everything the envelope puts in front of the message
(the magic byte, the id, the message-index path) parses without asking anyone. **Writing needs one
number**, the subject's schema id, and a value cannot fetch it: `Serialized::wire_bytes` is
synchronous and has only `&self` to work with. The publish path can, because it knows the
destination topic, which is exactly what names the subject.

So the type splits its wire paths - `prost` writes the message, this crate reads the envelope -
and the reply's own publisher puts the id and the index path on the way out:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf_plain.rs:wiring"
```

The handler returns its reply and nothing else: no slot parameter, no `publish().await`, no error
branch in the body. The reply type carries no destination of its own either, because the address
comes from the `publish("confirmations")` clause; `#[derive(Serialized)]` and the encode half of
`#[wire(..)]` are all it needs. The registry is named once, at the mount site, in the policy.

`KafkaPublish::framed(&registry)` works wherever a publish policy is named, an `Out` slot
included, so a handler that publishes several messages frames them all the same way.

### Setting it once for a whole app

`ProtobufFrame` is the same framing as a `publish_layer`, for an app that would otherwise repeat
the policy at every mount site:

<!-- inline-rust: one line of wiring; the compiled example shows the mount-site form, which is the one to reach for first -->
```rust
RustStream::new(info).publish_layer(ProtobufFrame::new(registry))
```

It frames a publish only when the destination topic's subject is registered **and holds a Protobuf
schema**, and passes everything else through untouched - a topic with no subject, and a topic whose
subject is Avro or JSON. That is what lets one app carry an Avro codec, a JSON codec and Protobuf
at once with the layer installed app-wide, and it is why the layer is lenient where the policy is
not: the layer sees every topic the app publishes to and most of them are not Protobuf, while
naming the policy at a mount site *declares* that one destination registry-backed, so a missing
subject there is an anomaly and fails the publish. That is the same argument `MissingSubject`'s
`Refuse` default makes on the codec path.

One thing the layer cannot do: **it never sees a `publish(..)` reply.** The core routes a
byte-for-byte reply straight to its paired publisher, deliberately, so a value that owns its bytes
leaves exactly as it wrote them - which is why the reply form takes the policy and the layer covers
publishes leaving through slots and through publishers a mount site never named. The two compose
rather than conflict: whichever runs first frames the payload, and the other finds an envelope
already there and leaves it alone.

A payload that already carries an envelope passes through on both paths, so a message that came in
framed and goes out again is not framed twice. The check is exact rather than a guess, because a
bare `prost` message opens with a field tag whose field number is at least 1, so its first byte is
never the zero magic byte.

Neither is a companion to `SchemaFrame`: that layer's contract is "this payload is a JSON document,
transcode it to the subject's flavor", these say "this payload is already the datum, put the
envelope on".

### Which message of the schema

The envelope's index path says which message was written, so it has to be the one the publishing
type actually is. Both the policy and the layer take the schema's first top-level message by
default, which is the common case and the one Confluent optimises to a single zero byte, so a
single-message `.proto` needs nothing. Anything else pins it with `.message(topic, "pkg.Message")`,
the same call `SchemaFrame` already takes.

The subject is resolved on the first publish to each destination and cached from then on. It
cannot happen at startup: a policy could do I/O when it pairs with the connected broker, but the
destination topic is not known there - it comes from the mount's `publish(..)` clause, or from the
call site of a slot publish, neither of which a policy is handed. So a missing subject surfaces as
a failed publish, which the handler's failure policy settles.

## Consuming: transcode on the way in

`KafkaBroker::schema_registry(registry)` puts the client on the consume edge: every subscription
of that broker converts a framed delivery to plain JSON before the payload reaches the codec. A
JSON Schema payload keeps its bytes and loses the envelope. An Avro or Protobuf datum converts
through the registry schema its envelope names, with that format's feature enabled. Handlers are
then ordinary subscribers on the default codec:

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

## Formats on the transcoding path

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

The transcoding trade-off: one JSON hop per message on registry topics buys a single uniform
handler model - the same struct, the same codec, any wire format - and gives up what the JSON
document cannot carry. Avro types JSON has no shape for do not survive it, and a datum written
under an older version of the subject is decoded, not resolved: the handler sees the writer's
fields, with no reader schema to fill in what the producer never wrote. Reach for it when a
service must keep plain serde models on a registry-backed topic; reach for the codec otherwise.

## Testing a Protobuf handler

A Protobuf handler is an ordinary handler, so `TestApp` and the in-process `KafkaTestBroker` drive
it with no cluster and no registry either, since reading needs none. The seeded record carries the
envelope a registry-backed producer writes, and the handler never sees it:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf_testing.rs:testapp"
```

Framing the reply is the half that needs the registry, so in process the reply leaves bare and the
test asserts on the message rather than on the envelope. What the mount names against a cluster is
`KafkaPublish::framed(&registry)`.

Without the `macros` feature the same handler is a `Handle` impl over the same two axes - the
generated message in, the generated message out - and the mount names the subscription and the
reply's destination:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf_testing.rs:manual"
```

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_protobuf_testing.rs:mount"
```
