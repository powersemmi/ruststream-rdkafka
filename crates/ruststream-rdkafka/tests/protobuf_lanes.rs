//! Live end-to-end for Protobuf on the byte lanes: a generated message is framed behind the
//! message-index path its subject's schema declares, read back off the wire with no registry in
//! the read path, and - the interop assertion that matters - decoded by the transcoding
//! consumer, which resolves the index path through the registry's own compiled descriptor.
//!
//! The index path is the only thing this crate computes on the publish side, so a run that only
//! read its own frames back would prove nothing about the wire format.

#![cfg(feature = "protobuf")]

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ruststream::prelude::*;
use ruststream::runtime::{App, AppInfo, Reply, RustStream};
use ruststream::{Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Subscriber};
use ruststream_rdkafka::schema_registry::RegistrySubject;
use ruststream_rdkafka::{
    ConnectedKafkaBroker, IncomingFrame, KafkaBroker, KafkaPublish, KafkaTopic, OutgoingFrame,
    SchemaRegistry, SchemaType, StartOffset, protobuf,
};
use serde::Deserialize;
use tokio::sync::Notify;

/// The reply topic, fixed because the macro's `publish(..)` takes a string literal.
const REPLY_TOPIC: &str = "proto-lane-confirmations-placeholder";

/// `Order` is deliberately not the first message of the file: the compact single-zero index path
/// would hide a wrong one.
const ORDERS_PROTO: &str = r#"
syntax = "proto3";
package rslane;

message Ignored {
  string noise = 1;
}

message Order {
  int64 id = 1;
  string item = 2;
}
"#;

const CONFIRMATIONS_PROTO: &str = r#"
syntax = "proto3";
package rslane;

message Confirmation {
  int64 id = 1;
  string item = 2;
}
"#;

/// What `prost-build` would emit for `rslane.Order`.
#[derive(Clone, PartialEq, prost::Message)]
struct Order {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    item: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Confirmation {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    item: String,
}

// The subject and the Protobuf message name are facts about this type, so they live on it and
// every mount site names the type instead.
impl RegistrySubject for Confirmation {
    const SUBJECT: &'static str = "proto-lane-confirmations-placeholder-value";
    const MESSAGE: &'static str = "rslane.Confirmation";
}

/// What the transcoding consumer sees: the same shape as plain JSON.
#[derive(Debug, PartialEq, Deserialize)]
struct ConfirmationJson {
    id: i64,
    item: String,
}

#[derive(Clone)]
struct Wiring {
    confirmations: protobuf::Subject<Confirmation>,
    seen: Arc<Mutex<Vec<Order>>>,
    done: Arc<Notify>,
}

#[derive(FromRef)]
struct LaneApp {
    wiring: Wiring,
}

#[subscriber(
    KafkaTopic::new(std::env::var("PROTO_LANE_TRIGGER").expect("trigger env"))
        .group(std::env::var("PROTO_LANE_GROUP").expect("group env"))
        .start(StartOffset::Earliest),
    publish("proto-lane-confirmations-placeholder")
)]
async fn confirm(
    frame: &IncomingFrame<'_>,
    State(wiring): State<Wiring>,
) -> Result<OutgoingFrame, HandlerOutcome> {
    // No registry on this path at all: the index path only says which message of the schema was
    // written, and this handler has already decided which one it reads.
    let order: Order = protobuf::decode_framed(frame).map_err(|_| HandlerOutcome::drop())?;
    let confirmation = Confirmation {
        id: order.id,
        item: order.item.clone(),
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
                .group(unique("proto-lane-scan"))
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

/// Registers both subjects, puts one framed `rslane.Order` onto `trigger`, and returns the
/// subject the handler's replies are framed under.
async fn seed_a_framed_order(
    registry: &str,
    kafka: &str,
    trigger: &str,
    marker: i64,
) -> protobuf::Subject<Confirmation> {
    let sr = SchemaRegistry::new(registry);
    let orders_subject = unique("proto-lane-orders");
    sr.register(&orders_subject, SchemaType::Protobuf, ORDERS_PROTO)
        .await
        .expect("register orders");
    // The reply subject is the fixed reply topic's default name, so the transcoding consumer
    // resolves it without being told.
    let confirmations_subject = format!("{REPLY_TOPIC}-value");
    sr.register(
        &confirmations_subject,
        SchemaType::Protobuf,
        CONFIRMATIONS_PROTO,
    )
    .await
    .expect("register confirmations");

    let orders = protobuf::Subject::<Order>::resolve(&sr, &orders_subject, "rslane.Order")
        .await
        .expect("resolve orders");
    let seeded = orders
        .frame(&Order {
            id: marker,
            item: "anvil".to_owned(),
        })
        .expect("frame");
    assert_ne!(
        seeded.datum()[0],
        0,
        "Order is the second message of its schema, so the index path is a real one",
    );
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

    // Off the type's own declaration: the subject and the message name are written once, on
    // `Confirmation`, and this site names the type.
    assert_eq!(Confirmation::SUBJECT, confirmations_subject);
    protobuf::Subject::<Confirmation>::resolve_declared(&sr)
        .await
        .expect("resolve confirmations")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_protobuf_lanes_frame_what_the_registry_can_read() {
    let Some(registry) = std::env::var("SCHEMA_REGISTRY_TEST_URL").ok() else {
        return;
    };
    let Some(kafka) = std::env::var("KAFKA_TEST_URL").ok() else {
        return;
    };
    let trigger = unique("proto-lane-trigger");
    unsafe {
        std::env::set_var("PROTO_LANE_TRIGGER", &trigger);
        std::env::set_var("PROTO_LANE_GROUP", unique("proto-lane-group"));
    }
    let marker = i64::from(std::process::id()) * 1000 + 5;
    let confirmations = seed_a_framed_order(&registry, &kafka, &trigger, marker).await;

    let wiring = Wiring {
        confirmations,
        seen: Arc::new(Mutex::new(Vec::new())),
        done: Arc::new(Notify::new()),
    };
    let app_wiring = wiring.clone();
    let app = RustStream::new(AppInfo::new("proto-lane", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(LaneApp { wiring: app_wiring }))
        .with_broker(KafkaBroker::new([kafka.clone()]), |b| {
            b.include(confirm).out(Reply, KafkaPublish::default());
        });

    let done = Arc::clone(&wiring.done);
    let reply_id = wiring.confirmations.schema_id();
    let wait = async move {
        tokio::time::timeout(std::time::Duration::from_secs(30), done.notified())
            .await
            .expect("the delivery arrives within the timeout");

        // The interop assertion: a transcoding consumer resolves the reply's schema id and its
        // message-index path through the registry's own compiled descriptor. It only reaches the
        // right message if the path this crate wrote addresses the one it claims to.
        let transcoding = KafkaBroker::new([kafka])
            .schema_registry(SchemaRegistry::new(&registry))
            .connect()
            .await
            .expect("connect transcoding");
        let json = scan_topic(&transcoding, REPLY_TOPIC, |payload| {
            serde_json::from_slice::<ConfirmationJson>(payload)
                .is_ok_and(|confirmation| confirmation.id == marker)
        })
        .await;
        let confirmation: ConfirmationJson = serde_json::from_slice(&json).expect("json");
        assert_eq!(
            confirmation,
            ConfirmationJson {
                id: marker,
                item: "anvil".to_owned(),
            },
        );
        transcoding.shutdown().await.expect("transcoding shutdown");
    };
    App::run_until(app, wait).await.expect("run");

    let seen = wiring.seen.lock().expect("seen mutex poisoned").clone();
    assert_eq!(
        seen,
        vec![Order {
            id: marker,
            item: "anvil".to_owned(),
        }],
    );
    assert_ne!(reply_id, 0, "the reply framed under a resolved schema id");
}

/// The same shape in process, with no cluster and no registry: what a service's own unit tests
/// look like, and the only lane coverage that runs where a broker is not available.
#[cfg(feature = "testing")]
mod in_process {
    use ruststream::prelude::*;
    use ruststream::runtime::{AppInfo, RustStream};
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::testing::KafkaTestBroker;
    use ruststream_rdkafka::{IncomingFrame, KafkaTopic, OutgoingFrame, protobuf};

    use super::{Confirmation, Order};

    /// The ids a registry would have assigned. Reading needs none of this; the publish side
    /// needs one number, so the test names it instead of standing a registry up.
    const ORDERS_SCHEMA_ID: u32 = 3;
    const CONFIRMATIONS_SCHEMA_ID: u32 = 7;

    #[derive(Clone)]
    struct Wiring {
        confirmations: protobuf::Subject<Confirmation>,
    }

    #[derive(FromRef)]
    struct Orders {
        wiring: Wiring,
    }

    #[subscriber(
        KafkaTopic::new("in-process-orders"),
        publish("in-process-confirmations")
    )]
    async fn confirm_in_process(
        frame: &IncomingFrame<'_>,
        State(wiring): State<Wiring>,
    ) -> Result<OutgoingFrame, HandlerOutcome> {
        let order: Order = protobuf::decode_framed(frame).map_err(|_| HandlerOutcome::drop())?;
        wiring
            .confirmations
            .frame(&Confirmation {
                id: order.id,
                item: order.item,
            })
            .map_err(|_| HandlerOutcome::drop())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lane_handler_round_trips_on_the_test_broker() {
        let wiring = Wiring {
            confirmations: protobuf::Subject::pinned(CONFIRMATIONS_SCHEMA_ID, &[0]),
        };
        let app = RustStream::new(AppInfo::new("proto-lane", "0.0.0"))
            .on_startup(async move |()| Ok::<_, std::io::Error>(Orders { wiring }))
            .with_broker(KafkaTestBroker::new(), |b| {
                b.include(confirm_in_process);
            });
        let tb = TestApp::start(app).await.expect("start");

        // An `OutgoingFrame` publishes like any other typed value, and carries its own bytes, so
        // the injection puts the exact wire form on the topic.
        let seeded = protobuf::Subject::<Order>::pinned(ORDERS_SCHEMA_ID, &[0])
            .frame(&Order {
                id: 42,
                item: "anvil".to_owned(),
            })
            .expect("frame");
        tb.message(&seeded)
            .to("in-process-orders")
            .publish()
            .await
            .expect("publish drives the handler to quiescence");

        let published = tb
            .broker::<KafkaTestBroker>()
            .published::<()>("in-process-confirmations")
            .assert_called_once();
        let reply =
            IncomingFrame::from_payload(published.messages()[0].payload()).expect("framed reply");
        assert_eq!(reply.schema_id(), CONFIRMATIONS_SCHEMA_ID);
        assert_eq!(
            protobuf::decode_framed::<Confirmation>(&reply).expect("decode"),
            Confirmation {
                id: 42,
                item: "anvil".to_owned(),
            },
        );

        tb.shutdown().await.expect("shutdown");
    }
}
