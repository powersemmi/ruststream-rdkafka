//! What a publish through this crate costs for the headers it carries.
//!
//! A publisher is handed the header map the publish filled, and hands it on rather than copying
//! it. The framing publisher is the one that rebuilds the message on its way through, so it is
//! the one where a copy can hide; the test reads the cost off this thread's allocation counter,
//! because content equality and addresses cannot tell a hand-over from a copy.
#![cfg(all(feature = "protobuf", feature = "testing"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ruststream::{Broker, HeaderMap, OutgoingMessage, PublishPolicy, Publisher};
use ruststream_rdkafka::testing::KafkaTestBroker;
use ruststream_rdkafka::{KafkaFramedPublish, KafkaPublish, SchemaRegistry};
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Counts this thread's allocations, so the cost of one publish can be read off directly. A
/// thread-local count rather than a global one: nothing another thread does belongs in this
/// measurement.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        // SAFETY: the layout is the caller's, forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What this thread has allocated so far.
fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

/// One message declaring one message type, which is all the framing needs to resolve an index
/// path.
const ORDERS_PROTO: &str = r#"
syntax = "proto3";
package acme;

message Order {
  int64 id = 1;
}
"#;

/// A bare `prost` message: field 1, varint 42. Its first byte is a field tag, so the framing
/// reads it as unframed.
const BARE: &[u8] = &[0x08, 0x2a];

/// The headers a publish carries here: what a transform stamps on the way out.
fn stamped() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-stamp", b"1".to_vec());
    headers.insert("x-tenant", b"acme".to_vec());
    headers
}

/// A registry that answers every subject with the same Protobuf schema, so the framing resolves
/// without a cluster and without a real registry.
async fn registry() -> (MockServer, SchemaRegistry) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/subjects/.+/versions/latest$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 11,
            "version": 1,
            "schema": ORDERS_PROTO,
            "schemaType": "PROTOBUF",
        })))
        .mount(&server)
        .await;
    let registry = SchemaRegistry::new(server.uri());
    (server, registry)
}

/// What one publish to `topic` costs, headers and all, with the subject, the index prefix and
/// the topic's own log entry already resolved by a warm-up publish before the count.
async fn spent<P: Publisher>(publisher: &P, topic: &str, headers: HeaderMap) -> usize {
    publisher
        .publish(OutgoingMessage::new(topic, BARE), None)
        .await
        .map_err(|err| err.to_string())
        .expect("the in-process transport accepts the warm-up");

    let msg = OutgoingMessage::new(topic, BARE).with_headers(headers);
    let before = allocations();
    publisher
        .publish(msg, None)
        .await
        .map_err(|err| err.to_string())
        .expect("the in-process transport accepts the publish");
    allocations() - before
}

/// The framing publisher rebuilds the message around the framed payload, and the header map
/// travels into the rebuilt one rather than being copied into it: what the headers cost is what
/// they cost through the publisher underneath, and framing adds nothing.
#[tokio::test]
async fn framing_a_message_costs_nothing_for_its_headers() {
    let (_server, registry) = registry().await;
    let connected = KafkaTestBroker::new().connect().await.expect("connect");
    let framed = KafkaFramedPublish::over(KafkaPublish::default(), &registry)
        .pair(&connected)
        .await
        .expect("the in-process transport pairs the framing policy");
    let plain = KafkaPublish::default()
        .pair(&connected)
        .await
        .expect("the in-process transport pairs the plain policy");

    // A topic apiece, so every measured publish is the second one on a fresh log and the
    // transport's own bookkeeping is the same under all four.
    let framed_with = spent(&framed, "framed-with", stamped()).await;
    let framed_without = spent(&framed, "framed-without", HeaderMap::new()).await;
    let plain_with = spent(&plain, "plain-with", stamped()).await;
    let plain_without = spent(&plain, "plain-without", HeaderMap::new()).await;

    assert_eq!(
        framed_with - framed_without,
        plain_with - plain_without,
        "two headers cost the framing publisher what they cost the publisher underneath; \
         anything above it is a copy of the map ({framed_with} vs {framed_without} framed, \
         {plain_with} vs {plain_without} plain)",
    );
}
