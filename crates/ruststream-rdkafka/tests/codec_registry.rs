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
    ConnectedKafkaBroker, KafkaBroker, KafkaPublish, KafkaTopic, SchemaFramed, SchemaPrefetch,
    SchemaRegistry, StartOffset,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        .publish(OutgoingMessage::new(topic, payload))
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
    let writer_codec = AvroCodec::registry(&writer_prefetch, &subject);
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
    let codec = AvroCodec::registry(&prefetch, &subject)
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
    let writer_codec = SchemaFramed::new(&writer_prefetch, &subject, JsonCodec);
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
    let codec = SchemaFramed::new(&prefetch, &subject, JsonCodec);
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
