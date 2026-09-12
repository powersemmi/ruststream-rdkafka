//! A Protobuf handler under test, in process: the same generated types and the same mount, no
//! cluster and no registry.
//!
//! Reading is what makes that possible. A delivery decodes against the reader's own generated
//! type, so the envelope a registry-backed producer wrote in front of the message costs one
//! step-over and no lookup. The publish side is the half that needs the registry, and this test
//! leaves the reply unframed - what the mount names on a cluster is
//! `KafkaPublish::framed(&registry)`, covered by the live suite.
//!
//! The handler is written here without the `macros` feature, as the `Handle` impl the attribute
//! would have emitted. The signature is the same either way: the generated message in, the
//! generated message out.
//!
//! ```text
//! cargo run --example kafka_protobuf_testing --features protobuf,testing
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ruststream::prelude::*;
use ruststream::runtime::{AppInfo, RustStream};
use ruststream::testing::TestApp;
use ruststream_rdkafka::protobuf;
use ruststream_rdkafka::testing::KafkaTestBroker;

/// What `prost-build` emits, plus the two lane derives. `prost` owns the bytes on the way out;
/// reading steps over the Confluent envelope first, which is the only asymmetry.
#[derive(Clone, PartialEq, prost::Message, Deserialized, Serialized)]
#[wire(encode = ::prost::Message::encode, decode = protobuf::decode_confluent)]
struct Order {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    item: String,
}

#[derive(Clone, PartialEq, prost::Message, Deserialized, Serialized, Outgoing)]
#[wire(encode = ::prost::Message::encode, decode = protobuf::decode_confluent)]
struct Confirmation {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(bool, tag = "2")]
    accepted: bool,
}

/// A payload published exactly as written, for seeding the topic the way a registry-backed
/// producer writes it.
#[derive(Serialized, Outgoing)]
struct Wire(Vec<u8>);

/// What the service counts, shared the way any application state is.
#[derive(Clone, Default)]
struct Seen(Arc<AtomicUsize>);

struct Orders {
    seen: Seen,
}

impl FromRef<Orders> for Seen {
    fn from_ref(state: &Orders) -> Self {
        state.seen.clone()
    }
}

// --8<-- [start:manual]
/// The definition value `#[subscriber("orders", publish("confirmations"))]` would have minted.
struct Confirm;

// The input axis is the generated message, the reply axis is the generated message, and neither
// position resolves a codec: both types carry their own bytes. The third parameter is the
// injections arena (none here), the fourth the broker's per-delivery context, the fifth the app
// state the body extracts from.
impl Handle<Order, Confirmation, (), (), Orders> for Confirm {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), Orders>,
    ) -> Result<Confirmation, HandlerOutcome> {
        // One binding per extractor parameter, before the body: what the attribute emits for
        // `State(seen): State<Seen>`.
        let State(seen) = match <State<Seen> as FromContext<(), Orders>>::from_context(ctx).await {
            Ok(value) => value,
            Err(rejection) => return Err(HandlerOutcome::from(rejection)),
        };

        seen.0.fetch_add(1, Ordering::Relaxed);
        Ok(Confirmation {
            id: order.id,
            accepted: !order.item.is_empty(),
        })
    }
}
// --8<-- [end:manual]

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let seen = Seen::default();
    let app_seen = seen.clone();
    // --8<-- [start:mount]
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, std::io::Error>(Orders { seen: app_seen }))
        .with_broker(KafkaTestBroker::new(), |b| {
            // The mount names the subscription and the reply's destination, and nothing else.
            // A Kafka descriptor (`KafkaTopic::new("orders").group(..)`) goes here the same way
            // when the subscription needs settings a bare name cannot carry.
            b.include(
                subscriber("orders", Confirm)
                    .reply()
                    .to("confirmations")
                    .build(),
            );
        });
    // --8<-- [end:mount]
    let tb = TestApp::start(app).await?;

    // --8<-- [start:testapp]
    // The seeded record carries the Confluent envelope a registry-backed producer writes: the
    // zero magic byte, the schema id, then the message-index path of the schema's first message.
    // The handler never sees any of it.
    let mut seeded = vec![0x00, 0x00, 0x00, 0x00, 0x2a, 0x00];
    prost::Message::encode(
        &Order {
            id: 42,
            item: "anvil".to_owned(),
        },
        &mut seeded,
    )?;
    tb.message(&Wire(seeded)).to("orders").publish().await?;

    // The reply is a publish value like any other, and its own bytes go on the topic untouched.
    let published = tb
        .broker::<KafkaTestBroker>()
        .published::<()>("confirmations")
        .assert_called_once();
    assert_eq!(
        Confirmation::from_payload(published.messages()[0].payload()).expect("decode the reply"),
        Confirmation {
            id: 42,
            accepted: true,
        },
    );
    // --8<-- [end:testapp]

    tb.broker::<KafkaTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    assert_eq!(seen.0.load(Ordering::Relaxed), 1);

    tb.shutdown().await?;
    println!("all in-process checks passed");
    Ok(())
}
