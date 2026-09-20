//! What a delivery owns: the record librdkafka fetched, or a copy taken out of it.
//!
//! Two properties, both of them about ownership rather than about routing.
//!
//! The first is that reading a body costs no copy of it: a record whose body is far larger than
//! anything else on the delivery path arrives without a single allocation of that size on the
//! consuming thread. A delivery that copies the body out of the fetch buffer fails it by the
//! size of the body.
//!
//! The second is that holding a delivery is safe for as long as the handler wants it: the
//! delivery outlives the subscription that fetched it, its body and its headers read the same
//! after the subscriber is gone, and the consumer is closed when the delivery is dropped - not
//! before it, which would leave the delivery pointing at freed memory, and not after it, which
//! would leak the client.
//!
//! ```text
//! just brokers-up
//! KAFKA_TEST_URL=127.0.0.1:9092 cargo test --test live_held_delivery -- --test-threads=1
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt as _;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use ruststream::{Broker as _, ConnectedBroker as _, IncomingMessage as _, Subscriber as _};
use ruststream_rdkafka::{KafkaBroker, KafkaTopic, StartOffset};

mod live;

thread_local! {
    /// The largest single block this thread has asked the allocator for since the last mark.
    static LARGEST: Cell<usize> = const { Cell::new(0) };
}

/// Watches this thread's allocation sizes. A thread-local watch rather than a global one: the
/// client's own threads allocate the fetch buffers, which is what this test wants the delivery
/// to be reading from.
struct Watching;

unsafe impl GlobalAlloc for Watching {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LARGEST.with(|largest| {
            if layout.size() > largest.get() {
                largest.set(layout.size());
            }
        });
        // SAFETY: the layout is the caller's, forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Watching = Watching;

/// Forgets what was allocated before the window under test.
fn mark() {
    LARGEST.with(|largest| largest.set(0));
}

/// The largest block this thread took since [`mark`].
fn largest() -> usize {
    LARGEST.with(Cell::get)
}

/// How many threads this process runs, which is how the client's own threads are observed
/// without a handle on the client: librdkafka starts them when a consumer is created and joins
/// them in `rd_kafka_destroy`.
fn threads() -> usize {
    let status = fs::read_to_string("/proc/self/status").expect("proc reports this process");
    status
        .lines()
        .find_map(|line| line.strip_prefix("Threads:"))
        .and_then(|count| count.trim().parse().ok())
        .expect("the status file reports a thread count")
}

/// Deliveries pulled before the window, so nothing the subscription paid once is read as the
/// cost of a delivery.
const WARM: usize = 8;

/// Further deliveries pulled while the first one is held.
const POLLS: usize = 100;

/// The body every record carries: a quarter of a megabyte, so a copy of it cannot hide among
/// the allocations a delivery legitimately makes.
const BODY_BYTES: usize = 256 * 1024;

/// A header every record carries, so a held delivery has something to answer after the
/// subscription that fetched it is gone.
const TRACE: &str = "trace-id";

/// Per-run unique names, so a rerun never reads another run's records.
fn unique(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "{base}-{}-{}",
        process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Creates `topic` up front, so the first subscribe does not race topic auto-creation.
async fn create_topic(url: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("admin client");
    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await
        .expect("create_topics call");
    for result in results {
        match result {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => panic!("creating topic {name} failed: {code}"),
        }
    }
}

/// Fills `topic` with `count` records of `body`, each carrying the [`TRACE`] header.
async fn fill(url: &str, topic: &str, body: &[u8], count: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", url)
        .set("message.max.bytes", "4194304")
        .set("queue.buffering.max.kbytes", "1048576")
        .create()
        .expect("producer");
    for index in 0..count {
        let key = index.to_string();
        let headers = OwnedHeaders::new().insert(Header {
            key: TRACE,
            value: Some(&key),
        });
        producer
            .send(
                FutureRecord::<[u8], [u8]>::to(topic)
                    .payload(body)
                    .headers(headers),
                Timeout::After(Duration::from_secs(30)),
            )
            .await
            .expect("the cluster accepts the record");
    }
}

/// The record body: built before the window opens, so its own allocation is not the one under
/// test.
fn body() -> Vec<u8> {
    vec![b'k'; BODY_BYTES]
}

/// A delivery reads the body where librdkafka put it, instead of copying it out.
///
/// The runtime is single-threaded so every allocation the delivery path makes lands on the
/// watched thread.
#[tokio::test]
async fn a_delivery_reads_the_body_where_librdkafka_left_it() {
    let Some(url) = live::url("KAFKA_TEST_URL") else {
        return;
    };
    let topic = unique("held-delivery");
    create_topic(&url, &topic).await;
    let body = body();
    fill(&url, &topic, &body, WARM + 1).await;

    let broker = KafkaBroker::new([url.clone()])
        .connect()
        .await
        .expect("connect");
    let mut subscriber = broker
        .subscribe_with(
            KafkaTopic::new(&topic)
                .group(unique("held-delivery"))
                .start(StartOffset::Earliest),
        )
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());
    for _ in 0..WARM {
        stream
            .next()
            .await
            .expect("the stream does not end")
            .expect("the delivery arrives");
    }

    mark();
    let delivery = stream
        .next()
        .await
        .expect("the stream does not end")
        .expect("the delivery arrives");
    let largest = largest();

    assert_eq!(delivery.payload(), body, "the body arrives intact");
    assert!(
        largest < BODY_BYTES,
        "a delivery of a {BODY_BYTES}-byte record took a {largest}-byte block on the consuming \
         thread: the body is being copied out of librdkafka's buffer, not read where it lies",
    );

    drop(delivery);
    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

/// A held delivery outlives the subscription that fetched it, and takes the consumer with it
/// when it goes.
#[tokio::test]
async fn a_held_delivery_outlives_the_consumer_that_fetched_it() {
    let Some(url) = live::url("KAFKA_TEST_URL") else {
        return;
    };
    let topic = unique("held-delivery");
    create_topic(&url, &topic).await;
    let body = body();
    fill(&url, &topic, &body, POLLS + 2).await;

    let broker = KafkaBroker::new([url.clone()])
        .connect()
        .await
        .expect("connect");
    let mut subscriber = broker
        .subscribe_with(
            KafkaTopic::new(&topic)
                .group(unique("held-delivery"))
                .start(StartOffset::Earliest),
        )
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());

    let held = stream
        .next()
        .await
        .expect("the stream does not end")
        .expect("the delivery arrives");
    // A hundred further deliveries pass through while the first one is held, so the fetch
    // buffer it points into is long behind the consumer's read position.
    for _ in 0..POLLS {
        stream
            .next()
            .await
            .expect("the stream does not end")
            .expect("the delivery arrives");
    }

    drop(stream);
    drop(subscriber);
    let holding = threads();

    assert_eq!(
        held.payload(),
        body,
        "a held delivery reads its body after the subscription that fetched it is gone",
    );
    assert_eq!(
        held.headers().get(TRACE),
        Some(b"0".as_slice()),
        "a held delivery reads its headers after the subscription that fetched it is gone",
    );

    drop(held);
    let closed = threads();
    assert!(
        closed < holding,
        "the consumer must be closed when the last delivery of it goes: {holding} threads while \
         the delivery was held, {closed} after it was dropped",
    );

    broker.shutdown().await.expect("shutdown");
}
