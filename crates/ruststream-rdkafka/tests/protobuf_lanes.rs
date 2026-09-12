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

    protobuf::Subject::<Confirmation>::resolve(&sr, &confirmations_subject, "rslane.Confirmation")
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

/// The same wire with the manual calls gone: a generated type carries the two lane derives and
/// splits its wire paths - `prost` writes the message, this crate reads the envelope - and
/// `ProtobufFrame` puts the id and the message-index path on outgoing publishes. The handler is
/// then an ordinary function over ordinary types, which is what the Avro codec already gave Avro.
///
/// The reply leaves through an `Out` slot rather than a `publish(..)` clause, because the core
/// routes a byte-for-byte *reply* around the app's publish pipeline by design (`RawReplyRoute`:
/// "the pipeline travels along for shape, though a byte-for-byte reply never runs it"), while a
/// slot publishes through it. Nothing in the handler serializes anything either way.
mod plain_handler {
    use super::{ConfirmationJson, scan_topic, unique};
    use std::sync::Arc;

    use ruststream::prelude::*;
    use ruststream::runtime::{App, AppInfo, DefaultSlot, RustStream};
    use ruststream::{Broker, ConnectedBroker, OutgoingMessage};
    use ruststream_rdkafka::{
        KafkaBroker, KafkaPublish, KafkaTopic, ProtobufFrame, SchemaRegistry, SchemaType,
        StartOffset, protobuf,
    };
    use tokio::sync::Notify;

    /// The reply topic, fixed because the slot's destination is named in the handler body.
    const REPLY_TOPIC: &str = "proto-plain-confirmations-placeholder";

    /// Neither message is declared first: a compact single-zero path would hide a wrong one on
    /// either side.
    const ORDERS_PROTO: &str = r#"
syntax = "proto3";
package rsplain;

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
package rsplain;

message AlsoIgnored {
  string noise = 1;
}

message Confirmation {
  int64 id = 1;
  string item = 2;
}
"#;

    /// What `prost-build` emits, plus the two lane derives. The encode half is the core's own
    /// `prost` path, so the value writes a bare message and the layer frames it; the decode half
    /// is this crate's, so a delivery arrives past its envelope.
    #[derive(Clone, PartialEq, prost::Message, Deserialized, Serialized)]
    #[wire(encode = ::prost::Message::encode, decode = protobuf::decode_confluent)]
    struct PlainOrder {
        #[prost(int64, tag = "1")]
        id: i64,
        #[prost(string, tag = "2")]
        item: String,
    }

    #[derive(Clone, PartialEq, prost::Message, Deserialized, Serialized, Outgoing)]
    #[wire(encode = ::prost::Message::encode, decode = protobuf::decode_confluent)]
    struct PlainConfirmation {
        #[prost(int64, tag = "1")]
        id: i64,
        #[prost(string, tag = "2")]
        item: String,
    }

    #[derive(Clone)]
    struct Signal(Arc<Notify>);

    #[derive(FromRef)]
    struct PlainApp {
        signal: Signal,
    }

    // The whole point: no envelope in the signature, no `decode_framed`, no `Subject::frame`.
    #[subscriber(
        KafkaTopic::new(std::env::var("PROTO_PLAIN_TRIGGER").expect("trigger env"))
            .group(std::env::var("PROTO_PLAIN_GROUP").expect("group env"))
            .start(StartOffset::Earliest)
    )]
    async fn confirm(
        order: &PlainOrder,
        State(signal): State<Signal>,
        Out(out): Out<impl Publisher>,
    ) -> HandlerOutcome {
        let sent = out
            .message(&PlainConfirmation {
                id: order.id,
                item: order.item.clone(),
            })
            .to(REPLY_TOPIC)
            .publish()
            .await;
        signal.0.notify_waiters();
        if sent.is_err() {
            return HandlerOutcome::retry();
        }
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_a_plain_protobuf_handler_reads_and_writes_the_confluent_wire() {
        let Some(registry) = std::env::var("SCHEMA_REGISTRY_TEST_URL").ok() else {
            return;
        };
        let Some(kafka) = std::env::var("KAFKA_TEST_URL").ok() else {
            return;
        };
        let trigger = unique("proto-plain-trigger");
        unsafe {
            std::env::set_var("PROTO_PLAIN_TRIGGER", &trigger);
            std::env::set_var("PROTO_PLAIN_GROUP", unique("proto-plain-group"));
        }
        let marker = i64::from(std::process::id()) * 1000 + 7;

        let sr = SchemaRegistry::new(&registry);
        let orders_subject = unique("proto-plain-orders");
        sr.register(&orders_subject, SchemaType::Protobuf, ORDERS_PROTO)
            .await
            .expect("register orders");
        // The reply subject under the layer's default `{topic}-value` naming: nothing pins it,
        // so the layer resolving it is part of what this asserts.
        sr.register(
            &format!("{REPLY_TOPIC}-value"),
            SchemaType::Protobuf,
            CONFIRMATIONS_PROTO,
        )
        .await
        .expect("register confirmations");

        // The producer stays the explicit path, which is what a service outside this app is.
        let orders =
            protobuf::Subject::<PlainOrder>::resolve(&sr, &orders_subject, "rsplain.Order")
                .await
                .expect("resolve orders");
        let seeded = orders
            .frame(&PlainOrder {
                id: marker,
                item: "anvil".to_owned(),
            })
            .expect("frame");
        assert_ne!(
            seeded.datum()[0],
            0,
            "Order is the second message of its schema, so the path is a real one",
        );
        let mut buf = BytesMut::new();
        let payload = seeded.wire_bytes(&mut buf).expect("infallible").to_vec();
        let seed_broker = KafkaBroker::new([kafka.clone()])
            .connect()
            .await
            .expect("connect seed");
        seed_broker
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(trigger.as_str(), payload.as_slice()))
            .await
            .expect("seed trigger");
        seed_broker.shutdown().await.expect("seed shutdown");

        let signal = Signal(Arc::new(Notify::new()));
        let app_signal = signal.clone();
        let app = RustStream::new(AppInfo::new("proto-plain", "0.0.0"))
            // `Confirmation` is the second message of its schema, so the reply needs the pin.
            .publish_layer(
                ProtobufFrame::new(sr.clone()).message(REPLY_TOPIC, "rsplain.Confirmation"),
            )
            .on_startup(async move |()| Ok::<_, std::io::Error>(PlainApp { signal: app_signal }))
            .with_broker(KafkaBroker::new([kafka.clone()]), |b| {
                b.include(confirm)
                    .out(DefaultSlot, KafkaPublish::default())
                    .build();
            });

        let notified = Arc::clone(&signal.0);
        let kafka_for_wait = kafka.clone();
        let registry_for_wait = registry.clone();
        let wait = async move {
            tokio::time::timeout(std::time::Duration::from_secs(30), notified.notified())
                .await
                .expect("the delivery reaches the plain handler within the timeout");

            // The reply as it lies on the topic, read by a transcoding consumer through the
            // registry's own compiled descriptor: it only reaches `rsplain.Confirmation` if the
            // layer's envelope and its message-index path are both right.
            let transcoding = KafkaBroker::new([kafka_for_wait])
                .schema_registry(SchemaRegistry::new(&registry_for_wait))
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
    }

    /// The same shape in process, with the registry mocked: what a service's own unit test looks
    /// like, and the only coverage of this path that runs where a cluster is not available.
    #[cfg(feature = "testing")]
    mod in_process {
        use ruststream::prelude::*;
        use ruststream::runtime::{AppInfo, DefaultSlot, RustStream};
        use ruststream::testing::TestApp;
        use ruststream_rdkafka::testing::KafkaTestBroker;
        use ruststream_rdkafka::{
            IncomingFrame, KafkaPublish, ProtobufFrame, SchemaRegistry, SchemaType, protobuf,
        };
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        use super::{CONFIRMATIONS_PROTO, PlainConfirmation, PlainOrder};

        const CONFIRMATIONS_ID: u32 = 21;

        #[subscriber("in-process-plain-orders")]
        async fn confirm_in_process(
            order: &PlainOrder,
            Out(out): Out<impl Publisher>,
        ) -> HandlerOutcome {
            let sent = out
                .message(&PlainConfirmation {
                    id: order.id,
                    item: order.item.clone(),
                })
                .to("in-process-plain-confirmations")
                .publish()
                .await;
            if sent.is_err() {
                return HandlerOutcome::retry();
            }
            HandlerOutcome::ack()
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_plain_handler_frames_through_the_layer() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path(
                    "/subjects/in-process-plain-confirmations-value/versions/latest",
                ))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": CONFIRMATIONS_ID,
                    "version": 1,
                    "schema": CONFIRMATIONS_PROTO,
                    "schemaType": "PROTOBUF",
                })))
                .mount(&server)
                .await;
            let sr = SchemaRegistry::new(server.uri());

            let app = RustStream::new(AppInfo::new("proto-plain", "0.0.0"))
                .publish_layer(
                    ProtobufFrame::new(sr)
                        .message("in-process-plain-confirmations", "rsplain.Confirmation"),
                )
                .with_broker(KafkaTestBroker::new(), |b| {
                    b.include(confirm_in_process)
                        .out(DefaultSlot, KafkaPublish::default())
                        .build();
                });
            let tb = TestApp::start(app).await.expect("start");

            // The delivery goes on the topic already framed, as a registry-backed producer
            // writes it; the handler never sees the envelope.
            let seeded = protobuf::Subject::<PlainOrder>::pinned(9, &[1])
                .frame(&PlainOrder {
                    id: 42,
                    item: "anvil".to_owned(),
                })
                .expect("frame");
            tb.message(&seeded)
                .to("in-process-plain-orders")
                .publish()
                .await
                .expect("publish drives the handler to quiescence");

            let published = tb
                .broker::<KafkaTestBroker>()
                .published::<()>("in-process-plain-confirmations")
                .assert_called_once();
            let wire = published.messages()[0].payload();
            let frame = IncomingFrame::from_payload(wire).expect("the layer framed the publish");
            assert_eq!(frame.schema_id(), CONFIRMATIONS_ID);
            // `rsplain.Confirmation` is the second message, so the pinned path is a real one.
            assert_ne!(frame.datum()[0], 0);
            assert_eq!(
                protobuf::decode_confluent::<PlainConfirmation>(wire).expect("decode"),
                PlainConfirmation {
                    id: 42,
                    item: "anvil".to_owned(),
                },
            );

            let _ = SchemaType::Protobuf;
            tb.shutdown().await.expect("shutdown");
        }
    }
}

/// The short reply form: the handler returns its reply, the mount site names the framing, and
/// the reply type carries nothing but its encode half. Where `plain_handler` puts the framing on
/// the app (a layer, and a slot to leave through), this puts it on the reply's own publisher.
mod plain_reply {
    use super::{ConfirmationJson, scan_topic, unique};

    use ruststream::prelude::*;
    use ruststream::runtime::{App, AppInfo, Reply, RustStream};
    use ruststream::{Broker, ConnectedBroker, OutgoingMessage};
    use ruststream_rdkafka::{
        KafkaBroker, KafkaPublish, KafkaTopic, SchemaRegistry, SchemaType, StartOffset, protobuf,
    };

    /// The reply topic, fixed because the macro's `publish(..)` takes a string literal.
    const REPLY_TOPIC: &str = "proto-reply-confirmations-placeholder";

    /// Neither message is declared first, so a compact single-zero path would hide a wrong one.
    const ORDERS_PROTO: &str = r#"
syntax = "proto3";
package rsreply;

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
package rsreply;

message AlsoIgnored {
  string noise = 1;
}

message Confirmation {
  int64 id = 1;
  string item = 2;
}
"#;

    #[derive(Clone, PartialEq, prost::Message, Deserialized)]
    #[wire(decode = protobuf::decode_confluent)]
    struct ReplyOrder {
        #[prost(int64, tag = "1")]
        id: i64,
        #[prost(string, tag = "2")]
        item: String,
    }

    // The reply carries its encode half and an `Outgoing` derive that names nothing, so the
    // address comes from the `publish(..)` clause below.
    #[derive(Clone, PartialEq, prost::Message, Serialized, Outgoing)]
    #[wire(encode = ::prost::Message::encode)]
    struct ReplyConfirmation {
        #[prost(int64, tag = "1")]
        id: i64,
        #[prost(string, tag = "2")]
        item: String,
    }

    // No slot parameter, no `.publish().await`, no error branch. The handler returns its reply.
    #[subscriber(
        KafkaTopic::new(std::env::var("PROTO_REPLY_TRIGGER").expect("trigger env"))
            .group(std::env::var("PROTO_REPLY_GROUP").expect("group env"))
            .start(StartOffset::Earliest),
        publish("proto-reply-confirmations-placeholder")
    )]
    async fn confirm(order: &ReplyOrder) -> ReplyConfirmation {
        ReplyConfirmation {
            id: order.id,
            item: order.item.clone(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_a_returned_protobuf_reply_lands_framed() {
        let Some(registry_url) = std::env::var("SCHEMA_REGISTRY_TEST_URL").ok() else {
            return;
        };
        let Some(kafka) = std::env::var("KAFKA_TEST_URL").ok() else {
            return;
        };
        let trigger = unique("proto-reply-trigger");
        unsafe {
            std::env::set_var("PROTO_REPLY_TRIGGER", &trigger);
            std::env::set_var("PROTO_REPLY_GROUP", unique("proto-reply-group"));
        }
        let marker = i64::from(std::process::id()) * 1000 + 11;

        let registry = SchemaRegistry::new(&registry_url);
        let orders_subject = unique("proto-reply-orders");
        registry
            .register(&orders_subject, SchemaType::Protobuf, ORDERS_PROTO)
            .await
            .expect("register orders");
        registry
            .register(
                &format!("{REPLY_TOPIC}-value"),
                SchemaType::Protobuf,
                CONFIRMATIONS_PROTO,
            )
            .await
            .expect("register confirmations");

        let orders =
            protobuf::Subject::<ReplyOrder>::resolve(&registry, &orders_subject, "rsreply.Order")
                .await
                .expect("resolve orders");
        let seeded = orders
            .frame(&ReplyOrder {
                id: marker,
                item: "anvil".to_owned(),
            })
            .expect("frame");
        let mut buf = BytesMut::new();
        let payload = seeded.wire_bytes(&mut buf).expect("infallible").to_vec();
        let seed_broker = KafkaBroker::new([kafka.clone()])
            .connect()
            .await
            .expect("connect seed");
        seed_broker
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(trigger.as_str(), payload.as_slice()))
            .await
            .expect("seed trigger");
        seed_broker.shutdown().await.expect("seed shutdown");

        let app = RustStream::new(AppInfo::new("proto-reply", "0.0.0")).with_broker(
            KafkaBroker::new([kafka.clone()]),
            |b| {
                // `rsreply.Confirmation` is the second message of its schema, so the index path
                // is pinned; a single-message `.proto` needs nothing here.
                b.include(confirm).out(
                    Reply,
                    KafkaPublish::framed(&registry).message(REPLY_TOPIC, "rsreply.Confirmation"),
                );
            },
        );

        let kafka_for_wait = kafka.clone();
        let registry_for_wait = registry_url.clone();
        let wait = async move {
            // Straight to the wire assertion: a transcoding consumer resolves the reply's schema
            // id and its index path through the registry's own compiled descriptor, so it only
            // reaches `rsreply.Confirmation` if the reply's publisher framed it correctly.
            let transcoding = KafkaBroker::new([kafka_for_wait])
                .schema_registry(SchemaRegistry::new(&registry_for_wait))
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
    }

    /// In process, with the registry mocked: the reply really carries the envelope, and the
    /// layer and the policy together frame it exactly once rather than twice.
    #[cfg(feature = "testing")]
    mod in_process {
        use ruststream::prelude::*;
        use ruststream::runtime::{AppInfo, Reply, RustStream};
        use ruststream::testing::TestApp;
        use ruststream_rdkafka::testing::KafkaTestBroker;
        use ruststream_rdkafka::{
            IncomingFrame, KafkaPublish, ProtobufFrame, SchemaRegistry, protobuf,
        };
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        use super::{CONFIRMATIONS_PROTO, ReplyConfirmation, ReplyOrder};

        const CONFIRMATIONS_ID: u32 = 31;
        const REPLY_TOPIC: &str = "in-process-reply-confirmations";

        #[subscriber("in-process-reply-orders", publish("in-process-reply-confirmations"))]
        async fn confirm(order: &ReplyOrder) -> ReplyConfirmation {
            ReplyConfirmation {
                id: order.id,
                item: order.item.clone(),
            }
        }

        async fn registry() -> (MockServer, SchemaRegistry) {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path(format!(
                    "/subjects/{REPLY_TOPIC}-value/versions/latest"
                )))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": CONFIRMATIONS_ID,
                    "version": 1,
                    "schema": CONFIRMATIONS_PROTO,
                    "schemaType": "PROTOBUF",
                })))
                .mount(&server)
                .await;
            let registry = SchemaRegistry::new(server.uri());
            (server, registry)
        }

        /// Drives one delivery through the app and returns the framed reply off the topic.
        async fn framed_reply<S: Send + Sync + 'static>(tb: &TestApp<S>) -> Vec<u8> {
            let seeded = protobuf::Subject::<ReplyOrder>::pinned(9, &[1])
                .frame(&ReplyOrder {
                    id: 42,
                    item: "anvil".to_owned(),
                })
                .expect("frame");
            tb.message(&seeded)
                .to("in-process-reply-orders")
                .publish()
                .await
                .expect("publish drives the handler to quiescence");
            tb.broker::<KafkaTestBroker>()
                .published::<()>(REPLY_TOPIC)
                .assert_called_once()
                .messages()[0]
                .payload()
                .to_vec()
        }

        fn assert_framed_once(wire: &[u8]) {
            let frame = IncomingFrame::from_payload(wire).expect("the reply carries an envelope");
            assert_eq!(frame.schema_id(), CONFIRMATIONS_ID);
            // The pinned message is the second one, so the path is real rather than compact.
            assert_ne!(frame.datum()[0], 0);
            assert_eq!(
                protobuf::decode_confluent::<ReplyConfirmation>(wire).expect("decode"),
                ReplyConfirmation {
                    id: 42,
                    item: "anvil".to_owned(),
                },
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_returned_reply_is_framed_by_the_mount_sites_policy() {
            let (_server, registry) = registry().await;
            let app = RustStream::new(AppInfo::new("proto-reply", "0.0.0")).with_broker(
                KafkaTestBroker::new(),
                |b| {
                    b.include(confirm).out(
                        Reply,
                        KafkaPublish::framed(&registry)
                            .message(REPLY_TOPIC, "rsreply.Confirmation"),
                    );
                },
            );
            let tb = TestApp::start(app).await.expect("start");
            assert_framed_once(&framed_reply(&tb).await);
            tb.shutdown().await.expect("shutdown");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn the_layer_and_the_policy_together_frame_once() {
            let (_server, registry) = registry().await;
            let app = RustStream::new(AppInfo::new("proto-reply", "0.0.0"))
                .publish_layer(
                    ProtobufFrame::new(registry.clone())
                        .message(REPLY_TOPIC, "rsreply.Confirmation"),
                )
                .with_broker(KafkaTestBroker::new(), |b| {
                    b.include(confirm).out(
                        Reply,
                        KafkaPublish::framed(&registry)
                            .message(REPLY_TOPIC, "rsreply.Confirmation"),
                    );
                });
            let tb = TestApp::start(app).await.expect("start");

            // Whichever of the two runs first frames it; the other sees the envelope already
            // there and leaves it alone. A double envelope would fail this decode.
            assert_framed_once(&framed_reply(&tb).await);
            tb.shutdown().await.expect("shutdown");
        }
    }
}
