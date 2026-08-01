//! Live end-to-end for the Protobuf registry middleware: a publishing handler's plain-JSON
//! replies get framed as Protobuf by the app's `SchemaFrame` publish layer (with a pinned
//! message, exercising a real index path), and a plain JSON handler consumes them back
//! through the broker's transcoding subscription.

#![cfg(feature = "protobuf")]

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream, State, TypedPublisher};
use ruststream::{Broker, ConnectedBroker, FromRef, OutgoingMessage, Publisher, subscriber};
use ruststream_rdkafka::{
    KafkaBroker, KafkaPublish, KafkaTopic, SchemaFrame, SchemaRegistry, SchemaType, StartOffset,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

/// The fixed reply topic (the macro's `publish(..)` takes a string literal). Runs share it:
/// each pins its own uniquely-named subject on the `SchemaFrame`, so its messages carry a
/// schema id no other run has, and the probe filters deliveries by a marker id range.
const FRAMED_TOPIC: &str = "proto-mw-frames-placeholder";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    id: i64,
    item: String,
}

#[derive(Clone)]
struct ProtoProbe {
    base: i64,
    expected: usize,
    seen: Arc<Mutex<Vec<Order>>>,
    done: Arc<Notify>,
}

#[derive(FromRef)]
struct ProtoApp {
    probe: ProtoProbe,
}

// The producing side: plain JSON through the pipeline; the SchemaFrame layer serializes the
// reply as the pinned Protobuf message under this run's subject.
#[subscriber(
    KafkaTopic::new(std::env::var("PROTO_MW_TRIGGER").expect("trigger env"))
        .group(std::env::var("PROTO_MW_TRIGGER_GROUP").expect("trigger group env"))
        .start(StartOffset::Earliest),
    publish("proto-mw-frames-placeholder")
)]
async fn proto_relay(order: &Order) -> Order {
    order.clone()
}

// The consuming side: a plain handler on the default JSON codec; the subscription already
// converted the Protobuf message back to JSON.
#[subscriber(
    KafkaTopic::new("proto-mw-frames-placeholder")
        .group(std::env::var("PROTO_MW_GROUP").expect("group env"))
        .start(StartOffset::Earliest)
)]
async fn proto_mw(order: &Order, State(probe): State<ProtoProbe>) -> HandlerResult {
    let marker_range = probe.base..probe.base + i64::try_from(probe.expected).expect("small");
    if !marker_range.contains(&order.id) {
        return HandlerResult::Ack; // an earlier run's message on the shared topic
    }
    {
        let mut seen = probe.seen.lock().expect("seen mutex poisoned");
        seen.push(order.clone());
        if seen.len() < probe.expected {
            return HandlerResult::Ack;
        }
    }
    probe.done.notify_waiters();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_protobuf_middleware_end_to_end() {
    const COUNT: usize = 3;
    static NEXT: AtomicU64 = AtomicU64::new(0);

    let Some(registry) = std::env::var("SCHEMA_REGISTRY_TEST_URL").ok() else {
        return;
    };
    let Some(kafka) = std::env::var("KAFKA_TEST_URL").ok() else {
        return;
    };
    let run = format!(
        "{}x{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let base = i64::from(std::process::id()) * 100_000;
    let trigger = format!("proto-mw-trigger-{run}");
    unsafe {
        std::env::set_var("PROTO_MW_TRIGGER", &trigger);
        std::env::set_var("PROTO_MW_TRIGGER_GROUP", format!("trigger-group-{run}"));
        std::env::set_var("PROTO_MW_GROUP", format!("group-{run}"));
    }

    // A run-unique subject in a run-unique package: its schema id belongs to this run alone,
    // so seeing it back proves the layer framed. `Order` is deliberately the second message,
    // so the pinned name must produce a real index path (not the compact zero).
    let subject = format!("proto-mw-{run}");
    let package = format!("acme{base}");
    let schema = format!(
        "syntax = \"proto3\";\npackage {package};\n\
         message Ignored {{ string noise = 1; }}\n\
         message Order {{ int64 id = 1; string item = 2; }}\n",
    );
    let sr = SchemaRegistry::new(&registry);
    let id = sr
        .register(&subject, SchemaType::Protobuf, schema)
        .await
        .expect("register");

    // Seed the trigger topic with plain JSON before the app starts.
    let seed_broker = KafkaBroker::new([kafka.clone()])
        .connect()
        .await
        .expect("connect seed");
    let count = i64::try_from(COUNT).expect("small count");
    for n in 0..count {
        let json = format!(r#"{{"id":{},"item":"item-{n}"}}"#, base + n);
        seed_broker
            .publisher(KafkaPublish::default())
            .publish(OutgoingMessage::new(&trigger, json.as_bytes()))
            .await
            .expect("seed trigger");
    }
    seed_broker.shutdown().await.expect("seed shutdown");

    // One app, both edges cold: the SchemaFrame resolves the pinned subject lazily on the
    // first reply, and the consuming subscription fetches the schema by wire id.
    let consumer_sr = SchemaRegistry::new(&registry);
    let probe = ProtoProbe {
        base,
        expected: COUNT,
        seen: Arc::new(Mutex::new(Vec::new())),
        done: Arc::new(Notify::new()),
    };
    let app_probe = probe.clone();
    let broker = KafkaBroker::new([kafka]).schema_registry(consumer_sr.clone());
    let replies = TypedPublisher::new(KafkaPublish::default());
    let app = RustStream::new(AppInfo::new("proto-mw", "0.0.0"))
        .publish_layer(
            SchemaFrame::new(SchemaRegistry::new(&registry))
                .subject(FRAMED_TOPIC, &subject)
                .message(FRAMED_TOPIC, format!("{package}.Order")),
        )
        .on_startup(async move |()| Ok::<_, Infallible>(ProtoApp { probe: app_probe }))
        .with_broker(broker, |b| {
            b.include(proto_mw);
            b.include(proto_relay).publisher(replies);
        });

    let done = Arc::clone(&probe.done);
    let wait = async move {
        tokio::time::timeout(std::time::Duration::from_secs(20), done.notified())
            .await
            .expect("all deliveries within timeout");
    };
    App::run_until(app, wait).await.expect("run");

    let mut seen = probe.seen.lock().expect("seen mutex poisoned").clone();
    seen.sort_by_key(|order| order.id);
    let expected: Vec<Order> = (0..count)
        .map(|n| Order {
            id: base + n,
            item: format!("item-{n}"),
        })
        .collect();
    assert_eq!(
        seen, expected,
        "Protobuf on the wire must arrive as JSON in plain handlers",
    );
    assert!(
        consumer_sr.cached_schema(id).is_some(),
        "the consumer transcoded through this run's schema id, so the layer really framed",
    );
}
