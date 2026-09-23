//! What reading a delivery's context costs, in allocations.
//!
//! A handler binds a context field per delivery - `Ctx(topic): Ctx<Topic>` and its siblings - so
//! every read is on the delivery path, as often as the handler names one. A field whose value is
//! already on the delivery is handed over rather than copied, and this holds that still.
//!
//! The in-process transport carries the same context type as the live consumer, which is what
//! makes this measurable without a cluster: what a read costs is a property of the context, not
//! of the broker underneath it.

#![cfg(feature = "testing")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, BuildContext as _, ContextField as _, Field as _, HeaderMap, OutgoingMessage,
    Publisher, Str, Subscriber,
};
use ruststream_rdkafka::context::KafkaContext;
use ruststream_rdkafka::context::keys::{Key, Source, Topic};
use ruststream_rdkafka::testing::KafkaTestBroker;
use ruststream_rdkafka::{KafkaPublish, PARTITION_KEY_HEADER};

/// Counts this thread's allocations. The reads under test run on it and nothing else does.
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

/// Reads of one field in a counted window, enough that a per-read copy cannot hide in a rounding.
const READS: usize = 1_000;

const WAIT: Duration = Duration::from_secs(1);

/// One delivery's context, off the in-process transport: a keyed record, so the key field has
/// something to answer.
async fn context() -> KafkaContext {
    let broker = KafkaTestBroker::new().connect().await.expect("connect");
    let mut subscriber = broker.subscribe_with("orders").await.expect("subscribe");
    let mut headers = HeaderMap::new();
    headers.insert(Str::from_static(PARTITION_KEY_HEADER), "order-1");
    broker
        .publisher(KafkaPublish::default())
        .publish(
            OutgoingMessage::new("orders", b"{\"id\":1}").with_headers(headers),
            None,
        )
        .await
        .expect("publish");
    let mut stream = Box::pin(subscriber.stream());
    let delivery = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("the delivery arrives within the timeout")
        .expect("the stream does not end")
        .expect("the delivery arrives");
    KafkaContext::build(&delivery)
}

/// A context field is handed over, not copied.
///
/// The topic name is the one the subscription minted, the key is the delivery's own bytes, and
/// the source coordinates are built out of both: all three are owned by reference count, so
/// reading them as many times as a handler likes costs nothing. The key's buffer is promoted to
/// a shared block on its first read, which is the one allocation a keyed delivery pays.
#[tokio::test]
async fn reading_a_context_field_allocates_nothing() {
    let context = context().await;

    let before = allocations();
    for _ in 0..READS {
        black_box(Topic.read(&context));
    }
    let topic = allocations() - before;

    let before = allocations();
    black_box(Key.read(&context));
    let first_key = allocations() - before;

    let before = allocations();
    for _ in 0..READS {
        black_box(Key.read(&context));
    }
    let key = allocations() - before;

    let before = allocations();
    for _ in 0..READS {
        black_box(Source.read(&context));
    }
    let source = allocations() - before;

    assert!(
        first_key <= 1,
        "the first read of the key took {first_key} allocations: the delivery's own buffer is \
         promoted to a shared block once, and nothing else is paid",
    );
    assert_eq!(
        (topic, key, source),
        (0, 0, 0),
        "over {READS} reads each, the topic took {topic} allocations, the key {key} and the \
         source coordinates {source}: the context is copying what the delivery already holds",
    );
}

/// The borrowing form of the topic owns into the same shared name.
///
/// A transform that names a destination after the source topic reads the borrowing form and
/// hands an owned name to the outgoing message; that step is a reference count, not a copy.
#[tokio::test]
async fn owning_the_borrowed_topic_allocates_nothing() {
    let context = context().await;

    let before = allocations();
    for _ in 0..READS {
        black_box(Topic.get(&context).to_owned());
    }
    let owned = allocations() - before;

    assert_eq!(
        owned, 0,
        "over {READS} reads, owning the borrowed topic took {owned} allocations: the borrowing \
         form is a plain string slice, so owning it copies the name",
    );
}
