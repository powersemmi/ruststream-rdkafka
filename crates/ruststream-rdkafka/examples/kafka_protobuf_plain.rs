//! Protobuf on the byte lanes: a `prost` message arrives as itself, past its Confluent envelope,
//! and the handler returns its reply. No registry takes part. The scenarios are tests.
//!
//! ```text
//! cargo test --example kafka_protobuf_plain --features protobuf,testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_rdkafka::prelude::*;
use ruststream_rdkafka::protobuf::decode_confluent;

// --8<-- [start:types]
// What `prost-build` emits, plus the lane derives: the order decodes past the envelope, and the
// confirmation encodes itself through `prost`.
#[derive(Clone, PartialEq, prost::Message, Deserialized)]
#[wire(decode = decode_confluent)]
struct Order {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    item: String,
}

#[derive(Clone, PartialEq, prost::Message, Serialized, Outgoing)]
#[wire(encode = prost::Message::encode)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(bool, tag = "2")]
    accepted: bool,
}
// --8<-- [end:types]

// --8<-- [start:handler]
/// Confirms an order that names an item.
#[subscriber("orders", reply)]
async fn confirm(order: &Order) -> Confirmation {
    tracing::info!(order = order.id, item = %order.item, "confirming");
    Confirmation {
        id: order.id,
        accepted: !order.item.is_empty(),
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
        |b| {
            b.include(confirm);
        },
    )
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use prost::Message as _;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;
    use ruststream_rdkafka::schema_registry::encode_envelope;

    use super::{Confirmation, Order};

    /// A record exactly as a producer wrote it.
    #[derive(Serialized, Outgoing)]
    #[outgoing(name = "orders")]
    struct Record(Vec<u8>);

    fn order() -> Order {
        Order {
            id: 42,
            item: "anvil".to_owned(),
        }
    }

    /// The reply's bytes on the topic: the bare message `prost` writes.
    fn confirmed() -> Vec<u8> {
        Confirmation {
            id: 42,
            accepted: true,
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn a_framed_order_is_read_past_its_envelope() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(super::app()).await?;
        let broker = tb.broker::<KafkaBroker>();
        // Schema id 7, then the index path of the schema's first message (a single zero byte).
        let mut datum = vec![0];
        datum.extend(order().encode_to_vec());
        broker
            .message(&Record(encode_envelope(7, &datum)))
            .publish()
            .await?;

        broker
            .published::<()>("confirmations")
            .assert_called_once()
            .with_raw(&confirmed());
        broker
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_bare_order_is_read_as_it_is() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(super::app()).await?;
        let broker = tb.broker::<KafkaBroker>();
        broker
            .message(&Record(order().encode_to_vec()))
            .publish()
            .await?;

        broker
            .published::<()>("confirmations")
            .assert_called_once()
            .with_raw(&confirmed());
        tb.shutdown().await?;
        Ok(())
    }
}
