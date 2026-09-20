//! What one delivery costs this crate, in allocations, against a real cluster.
//!
//! A consumer's fixed per-delivery work is what separates this crate from the raw `rdkafka` loop
//! it wraps, and allocation count is the part of it a test can hold still: it is deterministic,
//! it does not move with the machine's load, and every copy on the delivery path shows up in it.
//! The budget below is a ceiling that may only go down.
//!
//! Only this thread's allocations are counted, and the runtime is single-threaded on purpose:
//! librdkafka's own threads allocate the fetch buffers, which is the client's cost and not this
//! crate's. The slope of a second window over a first one cancels what the subscription paid to
//! start.
//!
//! ```text
//! just brokers-up
//! KAFKA_TEST_URL=127.0.0.1:9092 cargo test --test live_delivery_cost -- --test-threads=1
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use ruststream::{Broker, IncomingMessage, Subscriber};
use ruststream_rdkafka::{Commit, KafkaBroker, KafkaMessage, KafkaTopic, StartOffset};

mod live;

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

/// Deliveries consumed before the first count, so nothing the subscription paid once is read as
/// a per-delivery cost.
const WARM: usize = 200;
/// Deliveries in each counted window.
const STEP: usize = 1_000;
/// The body every record carries: large enough that the one copy left is a real one, small
/// enough that the run is quick.
const BODY: &[u8] = b"{\"id\":1,\"item\":\"anvil\",\"quantity\":37,\"note\":\"a plain record\"}";

/// What one delivery may allocate on this thread over what the raw client loop allocates for
/// the same record.
///
/// One: the payload, which is copied out of librdkafka's fetch buffer because the delivery
/// outlives the poll that produced it. Everything else a delivery carries is either shared with
/// the subscription or written on the first ask. The budget may only go down.
const BUDGET: usize = 1;

/// What one settled delivery may allocate on this thread over the raw client loop, under
/// [`Commit::Tracked`]: the payload copy plus the tracker's own bookkeeping (the keys it hashes
/// the partition by, and librdkafka's topic handle behind `store_offset`). The budget may only
/// go down.
const TRACKED_BUDGET: usize = 5;

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

/// Fills `topic` with `count` keyless, header-free records: the shape whose per-delivery cost is
/// this crate's own, with nothing of the record's making in it.
async fn fill(url: &str, topic: &str, count: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", url)
        .set("queue.buffering.max.messages", "1000000")
        .create()
        .expect("producer");
    let sends: Vec<_> = (0..count)
        .map(|_| {
            producer.send(
                FutureRecord::<[u8], [u8]>::to(topic).payload(BODY),
                Timeout::After(Duration::from_secs(30)),
            )
        })
        .collect();
    for send in sends {
        send.await.expect("the cluster accepts the record");
    }
}

/// Consumes `count` deliveries, reading nothing of them: a handler that only decodes the body
/// asks for no more than this. Under [`Commit::Tracked`] each one is settled, which is the
/// second thing a handler does.
async fn consume<S>(stream: &mut S, count: usize, tracked: bool)
where
    S: Stream<Item = Result<KafkaMessage, ruststream_rdkafka::KafkaError>> + Unpin,
{
    for _ in 0..count {
        let delivery = stream
            .next()
            .await
            .expect("the stream does not end")
            .expect("the delivery arrives");
        // What every handler does: read the body. Nothing else is asked of the delivery.
        assert_eq!(IncomingMessage::payload(&delivery), BODY);
        if tracked {
            delivery.ack().await.expect("the offset settles");
        }
    }
}

/// What a window of [`STEP`] deliveries costs this crate's consumer, counted after a first
/// window of the same size: whatever the subscription paid to start is in the first one and not
/// in the second.
async fn crate_window(url: &str, topic: &str, commit: Commit) -> usize {
    let tracked = commit == Commit::Tracked;
    let broker = KafkaBroker::new([url.to_owned()])
        .connect()
        .await
        .expect("connect");
    let mut subscriber = broker
        .subscribe_with(
            KafkaTopic::new(topic)
                .group(unique("delivery-cost"))
                .start(StartOffset::Earliest)
                .commit(commit),
        )
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());

    consume(&mut stream, WARM, tracked).await;
    let first = allocations();
    consume(&mut stream, STEP, tracked).await;
    let second = allocations();
    consume(&mut stream, STEP, tracked).await;
    let third = allocations();
    assert_eq!(
        second - first,
        third - second,
        "the two windows must cost the same, or the count is not a per-delivery cost",
    );
    third - second
}

/// The same window over the raw `rdkafka` loop this crate wraps: the client's own floor, which
/// no consumer built on it can go below.
async fn raw_window(url: &str, topic: &str) -> usize {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", url)
        .set("group.id", unique("delivery-cost-raw"))
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("raw consumer");
    consumer.subscribe(&[topic]).expect("raw subscribe");

    let raw = async |count: usize| {
        for _ in 0..count {
            let delivery = consumer.recv().await.expect("the delivery arrives");
            assert_eq!(delivery.payload().expect("a body"), BODY);
        }
    };
    raw(WARM).await;
    raw(STEP).await;
    let second = allocations();
    raw(STEP).await;
    allocations() - second
}

/// A delivery costs the payload copy over what the raw client loop costs, and nothing else.
///
/// The runtime is single-threaded so every allocation either loop makes lands on the counting
/// thread. The tracked window is measured beside the auto one and reported, because what a
/// settled delivery costs on top is the other half of this crate's per-delivery bill.
#[tokio::test]
async fn a_delivery_allocates_only_its_payload_over_the_raw_loop() {
    let Some(url) = live::url("KAFKA_TEST_URL") else {
        return;
    };
    let topic = unique("delivery-cost");
    create_topic(&url, &topic).await;
    fill(&url, &topic, WARM + 2 * STEP).await;

    let raw = raw_window(&url, &topic).await;
    let ours = crate_window(&url, &topic, Commit::Auto).await;
    let tracked = crate_window(&url, &topic, Commit::Tracked).await;

    println!(
        "allocations over {STEP} deliveries: raw {raw}, auto {ours} (+{}), tracked {tracked} \
         (+{})",
        ours - raw,
        tracked - raw,
    );
    assert!(
        ours <= raw + BUDGET * STEP,
        "over {STEP} deliveries this crate allocates {ours} where the raw loop allocates {raw}: \
         {} per delivery against a budget of {BUDGET}",
        (ours - raw) / STEP,
    );
    assert!(
        tracked <= raw + TRACKED_BUDGET * STEP,
        "over {STEP} settled deliveries this crate allocates {tracked} where the raw loop \
         allocates {raw}: {} per delivery against a budget of {TRACKED_BUDGET}",
        (tracked - raw) / STEP,
    );
}
