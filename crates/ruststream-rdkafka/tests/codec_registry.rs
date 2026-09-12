//! Live end-to-end for the registry-backed codecs, against a real Confluent Schema Registry and
//! a real broker.
//!
//! Both schema sources are exercised, for both formats that have one:
//!
//! - Avro `local`: one pinned schema, a bare datum on the wire, no registry in the path.
//! - Avro `registry`: the Confluent envelope, the writer schema resolved per delivery, and a
//!   datum written under an older version read by a consumer that has moved on - which is the
//!   case a fixed-schema decoder cannot serve and the reason the registry variant exists.
//! - JSON `registry`: the same envelope as a wrapper over the core's own JSON codec.
//! - JSON `local`: deliberately absent. A local JSON Schema adds nothing to the core's
//!   `JsonCodec` - a JSON document is self-describing and the codec would hold a schema it never
//!   consults - so the local JSON case *is* the core codec, and the crate's other suites already
//!   cover it. Wrapping it for symmetry would ship a type whose only content is a pass-through.

#![cfg(feature = "avro")]

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use apache_avro::AvroSchema;
use ruststream::codec::{Codec, JsonCodec};
use ruststream::prelude::*;
use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream::{Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Subscriber};
use ruststream_rdkafka::avro::AvroCodec;
use ruststream_rdkafka::{
    ConnectedKafkaBroker, KafkaBroker, KafkaPublish, KafkaTopic, MissingSubject, SchemaFramed,
    SchemaPrefetch, SchemaRegistry, StartOffset,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

/// What a producer wrote before `note` existed.
#[derive(Debug, Serialize, Deserialize, AvroSchema)]
#[serde(rename = "CodecOrder")]
struct OrderV1 {
    id: i64,
    item: String,
}

/// What this service's handler reads: the same record one version on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AvroSchema)]
#[serde(rename = "CodecOrder")]
struct OrderV2 {
    id: i64,
    item: String,
    #[avro(default = r#""none""#)]
    note: String,
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize, ruststream_rdkafka::schema_registry::JsonSchema,
)]
struct JsonOrder {
    id: i64,
    item: String,
}

#[derive(Clone)]
struct Probe<T> {
    seen: Arc<Mutex<Vec<T>>>,
    done: Arc<Notify>,
}

impl<T> Probe<T> {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            done: Arc::new(Notify::new()),
        }
    }

    fn record(&self, value: T) {
        self.seen.lock().expect("probe mutex poisoned").push(value);
        self.done.notify_waiters();
    }
}

#[derive(FromRef)]
struct AvroApp {
    probe: Probe<OrderV2>,
}

#[derive(FromRef)]
struct JsonApp {
    probe: Probe<JsonOrder>,
}

// The handler is an ordinary handler over an ordinary struct: the codec put the schema in the
// pipeline, so nothing about Avro reaches this signature.
#[subscriber(
    KafkaTopic::new(std::env::var("CODEC_AVRO_TOPIC").expect("topic env"))
        .group(std::env::var("CODEC_AVRO_GROUP").expect("group env"))
        .start(StartOffset::Earliest)
)]
async fn take_order(order: &OrderV2, State(probe): State<Probe<OrderV2>>) -> HandlerOutcome {
    probe.record(order.clone());
    HandlerOutcome::ack()
}

#[subscriber(
    KafkaTopic::new(std::env::var("CODEC_JSON_TOPIC").expect("topic env"))
        .group(std::env::var("CODEC_JSON_GROUP").expect("group env"))
        .start(StartOffset::Earliest)
)]
async fn take_json(order: &JsonOrder, State(probe): State<Probe<JsonOrder>>) -> HandlerOutcome {
    probe.record(order.clone());
    HandlerOutcome::ack()
}

fn unique(base: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{base}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

fn live() -> Option<(String, String)> {
    Some((
        std::env::var("SCHEMA_REGISTRY_TEST_URL").ok()?,
        std::env::var("KAFKA_TEST_URL").ok()?,
    ))
}

/// Publishes `payload` to `topic` through a broker of its own, before the service starts.
async fn seed(kafka: &str, topic: &str, payload: &[u8]) {
    let broker = KafkaBroker::new([kafka.to_owned()])
        .connect()
        .await
        .expect("connect seed");
    broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(topic, payload), None)
        .await
        .expect("seed");
    broker.shutdown().await.expect("seed shutdown");
}

/// Reads one payload off `topic` from the earliest offset.
async fn first_payload(broker: &ConnectedKafkaBroker, topic: &str) -> Vec<u8> {
    use futures::StreamExt;

    let mut subscriber = broker
        .subscribe_with(
            KafkaTopic::new(topic)
                .group(unique("codec-scan"))
                .start(StartOffset::Earliest),
        )
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());
    let found = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let msg = stream
            .next()
            .await
            .expect("stream has next")
            .expect("delivery ok");
        let payload = IncomingMessage::payload(&msg).to_vec();
        msg.ack().await.expect("ack");
        payload
    })
    .await
    .expect("a message within the timeout");
    drop(stream);
    found
}

/// The Avro registry codec, end to end and across a real schema evolution: a datum written under
/// version 1 of a subject, read by a service whose model is version 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_avro_registry_codec_reads_an_older_writer() {
    let Some((registry, kafka)) = live() else {
        return;
    };
    let topic = unique("codec-avro");
    unsafe {
        std::env::set_var("CODEC_AVRO_TOPIC", &topic);
        std::env::set_var("CODEC_AVRO_GROUP", unique("codec-avro-group"));
    }
    let marker = i64::from(std::process::id()) * 1000 + 11;

    // One subject, two versions. The second is accepted because adding a field with a default is
    // a backward compatible change.
    let subject = unique("codec-avro-orders");
    let sr = SchemaRegistry::new(&registry);
    sr.register_avro::<OrderV1>(&subject)
        .await
        .expect("register v1");
    sr.register_avro::<OrderV2>(&subject)
        .await
        .expect("register v2");

    // The producer still writes version 1, so its codec is pinned to that schema and frames with
    // the id the registry gave it.
    let writer_prefetch = SchemaPrefetch::new(SchemaRegistry::new(&registry));
    let writer_codec = AvroCodec::registry(&writer_prefetch).register::<OrderV1>(&subject);
    let writer_broker = KafkaBroker::new([kafka.clone()])
        .schema_prefetch(writer_prefetch)
        .connect()
        .await
        .expect("connect writer");
    // `connect` resolved the subject, so encoding is a pure computation from here.
    let framed = writer_codec
        .encode(&OrderV1 {
            id: marker,
            item: "anvil".to_owned(),
        })
        .expect("encode");
    writer_broker.shutdown().await.expect("writer shutdown");
    seed(&kafka, &topic, &framed).await;

    // The consumer's model is version 2, and its codec resolves whatever writer schema arrives
    // onto that - which is what fills the field the producer never wrote.
    let probe = Probe::<OrderV2>::new();
    let app_probe = probe.clone();
    let prefetch = SchemaPrefetch::new(SchemaRegistry::new(&registry));
    let codec = AvroCodec::registry(&prefetch)
        .register::<OrderV2>(&subject)
        .resolve_onto(OrderV2::get_schema())
        .expect("reader schema");
    let broker = KafkaBroker::new([kafka]).schema_prefetch(prefetch);
    let app = RustStream::new(AppInfo::new("codec-avro", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(AvroApp { probe: app_probe }))
        .with_broker_codec(broker, codec, |b| {
            b.include(take_order);
        });

    let done = Arc::clone(&probe.done);
    App::run_until(app, async move {
        tokio::time::timeout(std::time::Duration::from_secs(30), done.notified())
            .await
            .expect("the delivery arrives within the timeout");
    })
    .await
    .expect("run");

    assert_eq!(
        probe.seen.lock().expect("probe mutex poisoned").as_slice(),
        [OrderV2 {
            id: marker,
            item: "anvil".to_owned(),
            // Never written by the producer: Avro's resolution took it from the reader schema.
            note: "none".to_owned(),
        }],
    );
}

/// The Avro local codec: one pinned schema, a bare datum on the wire, and no registry at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_avro_local_codec_puts_a_bare_datum_on_the_wire() {
    let Some((_, kafka)) = live() else {
        return;
    };
    let topic = unique("codec-avro-local");
    let codec = AvroCodec::local(OrderV1::get_schema()).expect("local");
    let payload = codec
        .encode(&OrderV1 {
            id: 7,
            item: "anvil".to_owned(),
        })
        .expect("encode");
    seed(&kafka, &topic, &payload).await;

    let broker = KafkaBroker::new([kafka])
        .connect()
        .await
        .expect("connect reader");
    let on_the_wire = first_payload(&broker, &topic).await;
    broker.shutdown().await.expect("shutdown");

    assert_eq!(on_the_wire, &payload[..], "byte for byte, no envelope");
    assert_ne!(on_the_wire[0], 0, "a bare datum carries no magic byte");
    let back: OrderV1 = codec.decode(&on_the_wire).expect("decode");
    assert_eq!(back.id, 7);
}

/// The JSON registry codec: the envelope as a wrapper over the core's own JSON codec, read back
/// by a consumer whose registry client starts cold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_json_registry_codec_round_trips_through_the_envelope() {
    let Some((registry, kafka)) = live() else {
        return;
    };
    let topic = unique("codec-json");
    unsafe {
        std::env::set_var("CODEC_JSON_TOPIC", &topic);
        std::env::set_var("CODEC_JSON_GROUP", unique("codec-json-group"));
    }
    let marker = i64::from(std::process::id()) * 1000 + 13;

    let subject = unique("codec-json-orders");
    let sr = SchemaRegistry::new(&registry);
    let id = sr
        .register_json::<JsonOrderSchema>(&subject)
        .await
        .expect("register");

    let writer_prefetch = SchemaPrefetch::new(SchemaRegistry::new(&registry));
    let writer_codec =
        SchemaFramed::new(&writer_prefetch, JsonCodec).register::<JsonOrder>(&subject);
    let writer_broker = KafkaBroker::new([kafka.clone()])
        .schema_prefetch(writer_prefetch)
        .connect()
        .await
        .expect("connect writer");
    let framed = writer_codec
        .encode(&JsonOrder {
            id: marker,
            item: "anvil".to_owned(),
        })
        .expect("encode");
    writer_broker.shutdown().await.expect("writer shutdown");

    // The wire really carries the envelope, and the document inside it is plain JSON - which is
    // the whole reason the envelope is separable here and not for Avro.
    let (wire_id, datum) =
        ruststream_rdkafka::schema_registry::parse_envelope(&framed).expect("framed");
    assert_eq!(wire_id, id);
    assert_eq!(
        serde_json::from_slice::<JsonOrder>(datum).expect("plain json inside"),
        JsonOrder {
            id: marker,
            item: "anvil".to_owned(),
        },
    );

    seed(&kafka, &topic, &framed).await;

    let probe = Probe::<JsonOrder>::new();
    let app_probe = probe.clone();
    // A cold client: nothing warm in its cache when the app starts.
    let prefetch = SchemaPrefetch::new(SchemaRegistry::new(&registry));
    let codec = SchemaFramed::new(&prefetch, JsonCodec).register::<JsonOrder>(&subject);
    let broker = KafkaBroker::new([kafka]).schema_prefetch(prefetch);
    let app = RustStream::new(AppInfo::new("codec-json", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(JsonApp { probe: app_probe }))
        .with_broker_codec(broker, codec, |b| {
            b.include(take_json);
        });

    let done = Arc::clone(&probe.done);
    App::run_until(app, async move {
        tokio::time::timeout(std::time::Duration::from_secs(30), done.notified())
            .await
            .expect("the delivery arrives within the timeout");
    })
    .await
    .expect("run");

    assert_eq!(
        probe.seen.lock().expect("probe mutex poisoned").as_slice(),
        [JsonOrder {
            id: marker,
            item: "anvil".to_owned(),
        }],
    );
}

/// The shape registered under the JSON subject, via schemars.
#[derive(Serialize, Deserialize, ruststream_rdkafka::schema_registry::JsonSchema)]
struct JsonOrderSchema {
    id: i64,
    item: String,
}

/// The missing-subject policy, against a registry the subject is really deleted from, driven
/// through the broker's own `connect` - which is where the policy runs.
///
/// The two deletions differ and the behaviour hangs on it: a soft delete hides the subject while
/// `GET /schemas/ids/{id}` still answers, so consumers keep working and only the producer is
/// stuck; a permanent delete takes the id too, and then no policy here helps a consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_missing_subject_policies_do_what_they_say() {
    let Some((registry, kafka)) = live() else {
        return;
    };
    let subject = unique("codec-missing");
    let sr = SchemaRegistry::new(&registry);
    sr.register_avro::<OrderV1>(&subject)
        .await
        .expect("register");

    // Soft delete: the subject is gone for a producer resolving it.
    reqwest::Client::new()
        .delete(format!("{registry}/subjects/{subject}"))
        .send()
        .await
        .expect("soft delete")
        .error_for_status()
        .expect("deleted");

    // Refuse, the default: connect stops and the message names the subject.
    let refusing = SchemaPrefetch::new(SchemaRegistry::new(&registry));
    let _refused = AvroCodec::registry(&refusing).register::<OrderV1>(&subject);
    let err = KafkaBroker::new([kafka.clone()])
        .schema_prefetch(refusing)
        .connect()
        .await
        .expect_err("the subject is gone, so the app must not come up");
    assert!(err.to_string().contains(&subject), "{err}");

    // AutoRegister: the schema the type carries goes back under the same subject, at connect.
    let repairing = SchemaPrefetch::new(SchemaRegistry::new(&registry))
        .on_missing_subject(MissingSubject::AutoRegister);
    let codec = AvroCodec::registry(&repairing).register::<OrderV1>(&subject);
    let broker = KafkaBroker::new([kafka])
        .schema_prefetch(repairing)
        .connect()
        .await
        .expect("the policy repaired the subject");

    let restored = SchemaRegistry::new(&registry)
        .warm(&subject)
        .await
        .expect("the subject is back");

    // And the codec frames with the restored id.
    let framed = codec
        .encode(&OrderV1 {
            id: 1,
            item: "anvil".to_owned(),
        })
        .expect("encode");
    let (id, _) = ruststream_rdkafka::schema_registry::parse_envelope(&framed).expect("framed");
    assert_eq!(id, restored.id());
    broker.shutdown().await.expect("shutdown");
}

/// The same record name with an incompatible field type: what a model looks like after someone
/// changed it without touching the registry.
#[derive(Debug, Serialize, Deserialize, AvroSchema)]
#[serde(rename = "CodecOrder")]
struct Drifted {
    id: String,
    item: String,
}

/// `latest.compatibility.strict`: a model that has drifted from its subject stops the app at
/// connect, with the registry's own account of the difference in the error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_a_drifted_model_is_caught_at_connect() {
    let Some((registry, kafka)) = live() else {
        return;
    };
    let subject = unique("codec-drift");
    SchemaRegistry::new(&registry)
        .register_avro::<OrderV1>(&subject)
        .await
        .expect("register the agreed schema");

    let prefetch = SchemaPrefetch::new(SchemaRegistry::new(&registry));
    let _codec = AvroCodec::registry(&prefetch).register::<Drifted>(&subject);
    let err = KafkaBroker::new([kafka.clone()])
        .schema_prefetch(prefetch)
        .connect()
        .await
        .expect_err("the drifted model must not reach a topic");
    assert!(err.to_string().contains(&subject), "{err}");
    assert!(
        err.to_string().contains("not compatible"),
        "the registry's own account travels in the error: {err}",
    );

    // Turned off, the same wiring comes up: a registry set to NONE, or a client that cannot
    // answer the question, must not be blocked by this check.
    let lenient = SchemaPrefetch::new(SchemaRegistry::new(&registry)).check_compatibility(false);
    let _lenient_codec = AvroCodec::registry(&lenient).register::<Drifted>(&subject);
    KafkaBroker::new([kafka])
        .schema_prefetch(lenient)
        .connect()
        .await
        .expect("the check is what refused, and it is off")
        .shutdown()
        .await
        .expect("shutdown");
}
