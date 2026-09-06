//! A byte-lane handler under test, in process: the same handler and the same descriptors, no
//! cluster and no registry.
//!
//! The Protobuf lane is what makes that possible. Reading a framed delivery needs no registry at
//! all - the envelope's message-index path only says which message of the schema was written, and
//! the handler has already decided which one it reads - so the consume path runs against
//! `KafkaTestBroker` with nothing else up. The publish side needs one number, the subject's
//! schema id, and a test names it instead of standing a registry up to be told what it knows.
//!
//! ```text
//! cargo run --example kafka_lanes_testing --features protobuf,testing
//! ```

use ruststream::prelude::*;
use ruststream::runtime::{AppInfo, RustStream};
use ruststream::testing::TestApp;
use ruststream_rdkafka::testing::KafkaTestBroker;
use ruststream_rdkafka::{IncomingFrame, KafkaTopic, OutgoingFrame, protobuf};

// --8<-- [start:types]
/// What `prost-build` emits from the `.proto`. Unlike an Avro model this is not a serde type, so
/// on a topic with no registry it would ride the byte lanes itself under `#[wire(prost)]`; here
/// it travels inside the Confluent envelope, which is what `IncomingFrame` carries.
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
    #[prost(bool, tag = "2")]
    accepted: bool,
}
// --8<-- [end:types]

/// The publish-side framing, resolved once at startup and injected into the handler. In
/// production `protobuf::Subject::resolve` fills it from the registry; the shape the handler sees
/// is the same either way, which is what lets one handler serve both.
#[derive(Clone)]
struct Wiring {
    confirmations: protobuf::Subject<Confirmation>,
}

#[derive(FromRef)]
struct Orders {
    wiring: Wiring,
}

// --8<-- [start:handler]
#[subscriber(KafkaTopic::new("orders"), publish("confirmations"))]
async fn confirm(
    frame: &IncomingFrame<'_>,
    State(wiring): State<Wiring>,
) -> Result<OutgoingFrame, HandlerOutcome> {
    let order: Order = protobuf::decode_framed(frame).map_err(|_| HandlerOutcome::drop())?;
    wiring
        .confirmations
        .frame(&Confirmation {
            id: order.id,
            accepted: true,
        })
        .map_err(|_| HandlerOutcome::drop())
}
// --8<-- [end:handler]

/// The schema ids this run pins: the two subjects a registry would have assigned.
const ORDERS_SCHEMA_ID: u32 = 3;
const CONFIRMATIONS_SCHEMA_ID: u32 = 7;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:testapp]
    let wiring = Wiring {
        confirmations: protobuf::Subject::pinned(CONFIRMATIONS_SCHEMA_ID, &[0]),
    };
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, std::io::Error>(Orders { wiring }))
        .with_broker(KafkaTestBroker::new(), |b| {
            b.include(confirm);
        });
    let tb = TestApp::start(app).await?;

    // An `OutgoingFrame` is a publish value like any other, so the injection is the ordinary
    // typed one - and because the frame carries its own bytes, no codec touches them.
    let seeded = protobuf::Subject::<Order>::pinned(ORDERS_SCHEMA_ID, &[0]).frame(&Order {
        id: 42,
        item: "anvil".to_owned(),
    })?;
    tb.message(&seeded).to("orders").publish().await?;

    // The reply is on the wire in its framed form, so the assertion reads it as a consumer would.
    let published = tb
        .broker::<KafkaTestBroker>()
        .published::<()>("confirmations")
        .assert_called_once();
    let reply = IncomingFrame::from_payload(published.messages()[0].payload())?;
    assert_eq!(reply.schema_id(), CONFIRMATIONS_SCHEMA_ID);
    assert_eq!(
        protobuf::decode_framed::<Confirmation>(&reply)?,
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

    tb.shutdown().await?;
    println!("all in-process checks passed");
    Ok(())
}
