//! Live end-to-end for Avro on the byte lanes: a datum written under one registered schema is
//! read by a handler whose type declares a later one, and the reply leaves under the Confluent
//! envelope of its own subject.
//!
//! The schema evolution is the point. A fixed-schema decoder can read back what it wrote; only a
//! reader that resolves the writer schema the envelope names onto its own can read a datum a
//! producer wrote before the field existed, which is what a registry is for.

#![cfg(feature = "avro")]

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use apache_avro::AvroSchema;
use ruststream::prelude::*;
use ruststream::runtime::{App, AppInfo, Reply, RustStream};
use ruststream::{Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Subscriber};
use ruststream_rdkafka::avro;
use ruststream_rdkafka::{
    ConnectedKafkaBroker, IncomingFrame, KafkaBroker, KafkaPublish, KafkaTopic, OutgoingFrame,
    SchemaRegistry, StartOffset,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

/// The reply topic, fixed because the macro's `publish(..)` takes a string literal; runs share it
/// and pick their own messages out by the marker id they seeded.
const REPLY_TOPIC: &str = "avro-lane-confirmations-placeholder";

/// What a producer wrote before `note` existed.
#[derive(Debug, Serialize, Deserialize, AvroSchema)]
#[serde(rename = "AvroLaneOrder")]
struct OrderV1 {
    id: i64,
    item: String,
}

/// What this service's handler reads: the same record with a field added, carrying the schema
/// default that makes the older datum readable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AvroSchema)]
#[serde(rename = "AvroLaneOrder")]
struct OrderV2 {
    id: i64,
    item: String,
    #[avro(default = r#""none""#)]
    note: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, AvroSchema)]
#[serde(rename = "AvroLaneConfirmation")]
struct Confirmation {
    id: i64,
    note: String,
}

/// Everything the handler needs, resolved once at startup: the registry it reads writer schemas
/// through, and the subject its replies are framed under.
#[derive(Clone)]
struct Wiring {
    registry: SchemaRegistry,
    confirmations: avro::Subject<Confirmation>,
    seen: Arc<Mutex<Vec<OrderV2>>>,
    done: Arc<Notify>,
}

#[derive(FromRef)]
struct LaneApp {
    wiring: Wiring,
}

#[subscriber(
    KafkaTopic::new(std::env::var("AVRO_LANE_TRIGGER").expect("trigger env"))
        .group(std::env::var("AVRO_LANE_GROUP").expect("group env"))
        .start(StartOffset::Earliest),
    publish("avro-lane-confirmations-placeholder")
)]
async fn confirm(
    frame: &IncomingFrame<'_>,
    State(wiring): State<Wiring>,
) -> Result<OutgoingFrame, HandlerOutcome> {
    let order: OrderV2 = avro::decode_framed(&wiring.registry, frame)
        .await
        .map_err(|_| HandlerOutcome::drop())?;
    let confirmation = Confirmation {
        id: order.id,
        note: order.note.clone(),
    };
    wiring.seen.lock().expect("seen mutex poisoned").push(order);
    wiring.done.notify_waiters();
    wiring
        .confirmations
        .frame(&confirmation)
        .map_err(|_| HandlerOutcome::drop())
}

fn unique(base: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{base}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// Scans `topic` from the earliest offset until `pick` accepts a payload, returning it.
async fn scan_topic(
    broker: &ConnectedKafkaBroker,
    topic: &str,
    pick: impl Fn(&[u8]) -> bool + Send + Sync,
) -> Vec<u8> {
    use futures::StreamExt;

    let mut subscriber = broker
        .subscribe_with(
            KafkaTopic::new(topic)
                .group(unique("avro-lane-scan"))
                .start(StartOffset::Earliest),
        )
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());
    let found = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let msg = stream
                .next()
                .await
                .expect("stream has next")
                .expect("delivery ok");
            let payload = IncomingMessage::payload(&msg).to_vec();
            msg.ack().await.expect("ack");
            if pick(&payload) {
                return payload;
            }
        }
    })
    .await
    .expect("marker message within the timeout");
    drop(stream);
    found
}

/// Registers one subject twice - the schema the producer writes with, then the one the handler
/// reads with - and puts a datum written under the first onto `trigger`. The registry accepts
/// the second version because adding a field with a default is a backward compatible change,
/// which is exactly the case schema resolution exists for.
async fn seed_an_older_writers_datum(
    registry: &str,
    kafka: &str,
    trigger: &str,
    marker: i64,
) -> avro::Subject<Confirmation> {
    let sr = SchemaRegistry::new(registry);
    let orders = unique("avro-lane-orders");
    let v1 = avro::Subject::<OrderV1>::register(&sr, &orders)
        .await
        .expect("register v1");
    let v2 = avro::Subject::<OrderV2>::register(&sr, &orders)
        .await
        .expect("register v2");
    assert_ne!(
        v1.schema_id(),
        v2.schema_id(),
        "the two versions must be distinct schemas, or the run proves nothing",
    );

    let seeded = v1
        .frame(&OrderV1 {
            id: marker,
            item: "anvil".to_owned(),
        })
        .expect("frame v1");
    assert_eq!(seeded.schema_id(), v1.schema_id());
    let mut buf = BytesMut::new();
    let payload = seeded.wire_bytes(&mut buf).expect("infallible").to_vec();

    let seed_broker = KafkaBroker::new([kafka.to_owned()])
        .connect()
        .await
        .expect("connect seed");
    seed_broker
        .publisher(KafkaPublish::default())
        .publish(OutgoingMessage::new(trigger, payload.as_slice()))
        .await
        .expect("seed trigger");
    seed_broker.shutdown().await.expect("seed shutdown");

    avro::Subject::<Confirmation>::register(&sr, &unique("avro-lane-confirmations"))
        .await
        .expect("register confirmations")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_avro_lanes_resolve_a_real_schema_evolution() {
    let Some(registry) = std::env::var("SCHEMA_REGISTRY_TEST_URL").ok() else {
        return;
    };
    let Some(kafka) = std::env::var("KAFKA_TEST_URL").ok() else {
        return;
    };
    let trigger = unique("avro-lane-trigger");
    unsafe {
        std::env::set_var("AVRO_LANE_TRIGGER", &trigger);
        std::env::set_var("AVRO_LANE_GROUP", unique("avro-lane-group"));
    }
    let marker = i64::from(std::process::id()) * 1000 + 3;
    let confirmations = seed_an_older_writers_datum(&registry, &kafka, &trigger, marker).await;

    // No `schema_registry` on the broker: the lane path reads the wire itself, so nothing
    // transcodes the delivery on the way in.
    let wiring = Wiring {
        registry: SchemaRegistry::new(&registry),
        confirmations,
        seen: Arc::new(Mutex::new(Vec::new())),
        done: Arc::new(Notify::new()),
    };
    let app_wiring = wiring.clone();
    let app = RustStream::new(AppInfo::new("avro-lane", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(LaneApp { wiring: app_wiring }))
        .with_broker(KafkaBroker::new([kafka.clone()]), |b| {
            b.include(confirm).out(Reply, KafkaPublish::default());
        });

    let done = Arc::clone(&wiring.done);
    let registry_for_wait = registry.clone();
    let wait = async move {
        tokio::time::timeout(std::time::Duration::from_secs(30), done.notified())
            .await
            .expect("the delivery arrives within the timeout");

        // A cold client, with nothing warm in its cache, must resolve the reply's id from the
        // registry and read the datum back.
        let cold = SchemaRegistry::new(&registry_for_wait);
        let raw = KafkaBroker::new([kafka])
            .connect()
            .await
            .expect("connect raw");
        let reply = scan_topic(&raw, REPLY_TOPIC, |payload| {
            IncomingFrame::from_payload(payload).is_ok_and(|frame| {
                frame.schema_id() == confirmations.schema_id()
                    && avro::decode::<Confirmation>(frame.datum())
                        .is_ok_and(|confirmation| confirmation.id == marker)
            })
        })
        .await;
        let frame = IncomingFrame::from_payload(&reply).expect("the reply carries the envelope");
        let confirmation: Confirmation = avro::decode_framed(&cold, &frame)
            .await
            .expect("the cold client resolves the reply's schema");
        assert_eq!(
            confirmation,
            Confirmation {
                id: marker,
                note: "none".to_owned(),
            },
        );
        raw.shutdown().await.expect("raw shutdown");
    };
    App::run_until(app, wait).await.expect("run");

    let seen = wiring.seen.lock().expect("seen mutex poisoned").clone();
    assert_eq!(
        seen,
        vec![OrderV2 {
            id: marker,
            item: "anvil".to_owned(),
            // The producer never wrote this field; the reader schema's default filled it in,
            // which is the resolution a fixed-schema decoder cannot perform.
            note: "none".to_owned(),
        }],
    );
}
