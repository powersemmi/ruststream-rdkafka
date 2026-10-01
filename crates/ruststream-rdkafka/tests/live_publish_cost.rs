//! What a publish through the exactly-once pipeline costs for the headers it carries, against a
//! real cluster.
//!
//! The pipeline is a publisher like any other: it strips the source coordinates the reply
//! carries and hands the record to the transactional publisher underneath. Stripping is a
//! header operation, and the map the publish filled should travel into the record rather than be
//! copied on the way. The cost is read off this thread's allocation counter, because content
//! equality cannot tell a hand-over from a copy.
//!
//! The plain publisher is the pair: it reaches the same producer with the same record, so what
//! it spends is the floor, and what the pipeline spends above it is the pipeline's own. A copy
//! of the header map is one allocation of that difference, whatever the map holds - the table is
//! copied, the shared keys and values are not.
//!
//! The plain publisher has a pair of its own, the raw client publishing the same record. What
//! librdkafka allocates is invisible to a Rust allocator, so that comparison reads the bytes this
//! thread allocated through the C allocator instead, the Rust allocations among them.
//!
//! ```text
//! just brokers-up
//! KAFKA_TEST_URL=127.0.0.1:9092 cargo test --test live_publish_cost -- --test-threads=1
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
#[cfg(all(target_os = "linux", target_env = "gnu"))]
use rdkafka::producer::{FutureProducer, FutureRecord};
#[cfg(all(target_os = "linux", target_env = "gnu"))]
use rdkafka::util::Timeout;
use ruststream::{
    Broker, HeaderMap, IncomingMessage, OutgoingMessage, PublishPolicy, Publisher, Str, Subscriber,
};
use ruststream_rdkafka::{
    Commit, KafkaBroker, KafkaEosPublish, KafkaPublish, KafkaTopic, PARTITION_KEY_HEADER,
    StartOffset,
};
#[cfg(all(target_os = "linux", target_env = "gnu"))]
use tikv_jemalloc_ctl::thread;

mod live;

/// What this thread has allocated through the C allocator so far, in bytes: librdkafka's
/// allocations, and Rust's, which reach the same allocator through the system one.
///
/// jemalloc replaces the C allocator in this test binary, so its per-thread counter sees what
/// librdkafka allocates on the publishing thread as well as what Rust does.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn natively_allocated() -> u64 {
    thread::allocatedp::read()
        .expect("jemalloc reports its per-thread counter")
        .get()
}

/// Counts this thread's allocations. A thread-local count rather than a global one: the client's
/// own threads are not what this measures.
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

/// The body every record carries.
const BODY: &[u8] = b"{\"id\":1}";

/// Per-run unique names, so a rerun never meets another run's topics or transactions.
fn unique(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "{base}-{}-{}",
        process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Creates `topic` up front, so the first publish does not race topic auto-creation.
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

/// How many headers a publish carries here: what a couple of transforms stamp on the way out.
const STAMPS: usize = 8;

/// What the pipeline may allocate over the publisher underneath, for the same record.
///
/// Three: the source coordinates it decodes out of the reply's headers, and the window
/// bookkeeping that admits the record into the open transaction. A copy of the header map is not
/// one of them.
const BUDGET: usize = 3;

/// A publish's headers, built fresh every time, so no measurement is paid for by an earlier one.
fn headers(base: &HeaderMap, stamps: usize) -> HeaderMap {
    let mut headers = base.clone();
    for stamp in 0..stamps {
        headers.insert(Str::from(format!("x-stamp-{stamp}")), format!("{stamp}"));
    }
    headers
}

/// What one publish costs, with the topic's producer and the pipeline's window already open.
async fn spent<P, H>(publisher: &P, topic: &str, headers: H) -> usize
where
    P: Publisher,
    H: Fn() -> HeaderMap,
{
    let warm = OutgoingMessage::new(topic, BODY).with_headers(headers());
    publisher
        .publish(warm, None)
        .await
        .map_err(|err| err.to_string())
        .expect("the cluster accepts the warm-up");

    let msg = OutgoingMessage::new(topic, BODY).with_headers(headers());
    let before = allocations();
    publisher
        .publish(msg, None)
        .await
        .map_err(|err| err.to_string())
        .expect("the cluster accepts the publish");
    allocations() - before
}

/// The pipeline takes the header map its reply carries rather than copying it: what it
/// allocates over the plain publisher is its own bookkeeping, with no map among it.
#[tokio::test]
async fn an_exactly_once_reply_costs_nothing_for_its_headers() {
    let Some(url) = live::url("KAFKA_TEST_URL") else {
        return;
    };
    let input = unique("eos-cost-in");
    let output = unique("eos-cost-out");
    create_topic(&url, &input).await;
    create_topic(&url, &output).await;
    let broker = KafkaBroker::new([url.clone()])
        .connect()
        .await
        .expect("connect");
    let pipeline_id = unique("eos-cost");

    // One record on the input, consumed under the transactional commit mode: its delivery
    // carries the source coordinates every pipeline reply must quote.
    let plain = KafkaPublish::default()
        .pair(&broker)
        .await
        .expect("pair the plain publisher");
    plain
        .publish(OutgoingMessage::new(&input, BODY), None)
        .await
        .expect("seed the input");

    // A commit interval longer than the test, so no window closes between the two measurements.
    let pipeline = KafkaEosPublish::new(&pipeline_id)
        .commit_interval(Duration::from_secs(600))
        .pair(&broker)
        .await
        .expect("pair the pipeline");
    let mut subscriber = broker
        .subscribe_with(
            KafkaTopic::new(&input)
                .group(unique("eos-cost"))
                .start(StartOffset::Earliest)
                .commit(Commit::Transactional(pipeline_id.clone())),
        )
        .await
        .expect("subscribe the input");
    let source = {
        let mut stream = Box::pin(subscriber.stream());
        let delivery = stream
            .next()
            .await
            .expect("the stream does not end")
            .expect("the delivery arrives");
        IncomingMessage::headers(&delivery).clone()
    };

    let empty = HeaderMap::new();
    let through_pipeline = spent(&pipeline, &output, || headers(&source, STAMPS)).await;
    let through_plain = spent(&plain, &output, || headers(&empty, STAMPS)).await;

    assert!(
        through_pipeline - through_plain <= BUDGET,
        "a reply through the pipeline allocates {through_pipeline} where the publisher \
         underneath allocates {through_plain}, which is {} over the budget of {BUDGET}",
        through_pipeline - through_plain - BUDGET,
    );
}

/// A publish takes the record key out of the map it was handed.
///
/// The partition key travels as a header and becomes the record's native key, so it is read on
/// every publish that carries one. The map owns its values by reference count, so taking the key
/// out of it costs nothing a keyless publish does not already pay.
#[tokio::test]
async fn a_keyed_publish_costs_nothing_for_its_key() {
    let Some(url) = live::url("KAFKA_TEST_URL") else {
        return;
    };
    let topic = unique("key-cost");
    create_topic(&url, &topic).await;
    let broker = KafkaBroker::new([url.clone()])
        .connect()
        .await
        .expect("connect");
    let publisher = KafkaPublish::default()
        .pair(&broker)
        .await
        .expect("pair the publisher");

    let empty = HeaderMap::new();
    let mut keyed = HeaderMap::new();
    keyed.insert(Str::from_static(PARTITION_KEY_HEADER), "order-1");

    let keyless_cost = spent(&publisher, &topic, || headers(&empty, STAMPS)).await;
    let keyed_cost = spent(&publisher, &topic, || headers(&keyed, STAMPS)).await;

    assert_eq!(
        keyed_cost, keyless_cost,
        "a keyed publish allocates {keyed_cost} where a keyless one allocates \
         {keyless_cost}: the key is being copied out of the map instead of taken from it",
    );
}

/// What a publish without a wire header may allocate over the raw client publishing the same
/// record, the C library's allocations included: nothing. The native header list is opened on the
/// first wire header, and a reply carries none unless a transform stamps one.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const BARE_BUDGET: u64 = 0;

/// Publishes `send` twice and reads what the second allocates through the C allocator, in bytes,
/// with the producer and its topic already warm. The least of a few rounds, so a background task
/// of the runtime that happens to allocate in between is not read as the publish's cost.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
async fn natively_spent<F, Fut>(send: F) -> u64
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()>,
{
    send().await;
    let mut least = u64::MAX;
    for _ in 0..5 {
        let before = natively_allocated();
        send().await;
        least = least.min(natively_allocated() - before);
    }
    least
}

/// A publish opens no native header list for a record without wire headers: it allocates what the
/// raw client allocates for the same record.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[tokio::test]
async fn a_publish_without_wire_headers_costs_what_the_raw_client_costs() {
    let Some(url) = live::url("KAFKA_TEST_URL") else {
        return;
    };
    let topic = unique("bare-cost");
    create_topic(&url, &topic).await;
    let broker = KafkaBroker::new([url.clone()])
        .connect()
        .await
        .expect("connect");
    let publisher = KafkaPublish::default()
        .pair(&broker)
        .await
        .expect("pair the publisher");
    let raw: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &url)
        .create()
        .expect("raw producer");

    let ours = natively_spent(async || {
        publisher
            .publish(OutgoingMessage::new(&topic, BODY), None)
            .await
            .map_err(|err| err.to_string())
            .expect("the cluster accepts the publish");
    })
    .await;
    let theirs = natively_spent(async || {
        raw.send(
            FutureRecord::<[u8], [u8]>::to(&topic).payload(BODY),
            Timeout::Never,
        )
        .await
        .map_err(|(err, _record)| err.to_string())
        .expect("the cluster accepts the raw publish");
    })
    .await;

    assert!(
        ours <= theirs + BARE_BUDGET,
        "a publish without wire headers allocates {ours} bytes where the raw client allocates \
         {theirs}, which is {} over the budget of {BARE_BUDGET}",
        ours - theirs - BARE_BUDGET,
    );
}
