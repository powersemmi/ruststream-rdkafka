//! The byte-lane service written without the `macros` feature: the same handler, the same
//! in-process test, with every impl the attribute would have emitted spelled out.
//!
//! It doubles as the answer to what a lane handler is underneath. `IncomingFrame` is the input
//! axis, `OutgoingFrame` the reply, and neither position resolves a codec - the input constructs
//! itself from the delivery's bytes and the reply hands its own bytes back. Nothing in the mount
//! or the body knows a codec exists.
//!
//! ```text
//! cargo run --example kafka_lanes_manual --features protobuf,testing
//! ```

use ruststream::prelude::*;
use ruststream::runtime::{AppInfo, RustStream};
use ruststream::testing::TestApp;
use ruststream_rdkafka::testing::KafkaTestBroker;
use ruststream_rdkafka::{IncomingFrame, OutgoingFrame, protobuf};

// --8<-- [start:types]
/// What `prost-build` emits. On an unframed topic the two lane derives and `#[wire(prost)]` would
/// put this type itself on the lanes; on a registry-backed one the envelope is what travels, so
/// the type stays exactly what the generator wrote.
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

// --8<-- [start:state]
/// The publish-side framing, resolved once at startup. `#[derive(FromRef)]` would write the impl
/// below; on the manual path the projection is one function.
#[derive(Clone)]
struct Wiring {
    confirmations: protobuf::Subject<Confirmation>,
}

struct Orders {
    wiring: Wiring,
}

impl FromRef<Orders> for Wiring {
    fn from_ref(state: &Orders) -> Self {
        state.wiring.clone()
    }
}
// --8<-- [end:state]

// --8<-- [start:handler]
/// The definition value `#[subscriber(.., publish("confirmations"))]` would have minted.
struct Confirm;

// The input axis is `IncomingFrame<'_>`, a borrowing view of the delivery, and the reply axis is
// `OutgoingFrame`. The third parameter is the injections arena (none here), the fourth the
// broker's per-delivery context - `()`, which is what the attribute emits for a signature with no
// `Ctx` key - and the fifth the app state the body extracts from.
impl Handle<IncomingFrame<'_>, OutgoingFrame, (), (), Orders> for Confirm {
    async fn handle(
        &self,
        frame: &IncomingFrame<'_>,
        _outs: &(),
        ctx: &mut Context<'_, (), Orders>,
    ) -> Result<OutgoingFrame, HandlerOutcome> {
        // One binding per extractor parameter, before the body: what the attribute emits for
        // `State(wiring): State<Wiring>`.
        let State(wiring) =
            match <State<Wiring> as FromContext<(), Orders>>::from_context(ctx).await {
                Ok(value) => value,
                Err(rejection) => return Err(HandlerOutcome::from(rejection)),
            };

        // The lane already handed the body the wire form; turning it into a message is the
        // format's own reader, and for Protobuf that reaches no registry.
        let order: Order = protobuf::decode_framed(frame).map_err(|_| HandlerOutcome::drop())?;
        wiring
            .confirmations
            .frame(&Confirmation {
                id: order.id,
                accepted: true,
            })
            .map_err(|_| HandlerOutcome::drop())
    }
}
// --8<-- [end:handler]

/// The schema ids this run pins: the two subjects a registry would have assigned.
const ORDERS_SCHEMA_ID: u32 = 3;
const CONFIRMATIONS_SCHEMA_ID: u32 = 7;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:mount]
    let wiring = Wiring {
        confirmations: protobuf::Subject::pinned(CONFIRMATIONS_SCHEMA_ID, &[0]),
    };
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, std::io::Error>(Orders { wiring }))
        .with_broker(KafkaTestBroker::new(), |b| {
            // The mount names the subscription and the reply's destination, and nothing else:
            // the wire of both positions came off the types. A Kafka descriptor
            // (`KafkaTopic::new("orders").group(..)`) mounts here the same way when the
            // subscription needs settings the bare name cannot carry.
            b.include(
                subscriber("orders", Confirm)
                    .reply()
                    .to("confirmations")
                    .build(),
            );
        });
    // --8<-- [end:mount]
    let tb = TestApp::start(app).await?;

    let seeded = protobuf::Subject::<Order>::pinned(ORDERS_SCHEMA_ID, &[0]).frame(&Order {
        id: 42,
        item: "anvil".to_owned(),
    })?;
    tb.message(&seeded).to("orders").publish().await?;

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

    tb.shutdown().await?;
    println!("all in-process checks passed");
    Ok(())
}
