// The benchmark is a binary of its own, not library surface: the framework's macros generate the
// handler scaffolding, and a measured loop panics on a broker fault rather than threading a
// `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate costs over the `rdkafka` client it wraps, and what the runtime costs on top.
//!
//! Every scenario runs three times over, as three loops that differ in one thing each: what
//! carries the deliveries.
//!
//! - **raw** - a librdkafka `StreamConsumer`, driven directly.
//! - **adapter** - this crate's own consumer: the broker, the topic descriptor, the
//!   [`Subscriber`](ruststream::Subscriber) stream it yields and the
//!   [`ack`](ruststream::IncomingMessage::ack) that settles a delivery. A loop in this file pulls
//!   from that stream, decodes and settles; there is no handler, no app and no dispatch in it.
//! - **framework** - the service a user writes: a `#[subscriber]` handler, the app, the runtime.
//!
//! `adapter` against `raw` is what this crate's consumer costs over the client it wraps, which is
//! the question this repository answers. `framework` against `adapter` is what the runtime costs
//! on top of it, over this broker in particular: the core owns that code, but how it meets a
//! transport that delivers in fetched batches is a fact about this crate.
//!
//! Everything else is held equal across the three - the consumer properties, the consumer group,
//! the commit mode, the position of the acknowledgement, the decode into the same type, the
//! payload bytes, the tokio runtime and the binary. The procedure the numbers follow is the
//! framework's own, published at <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! # What a run is
//!
//! The topic is created first, the consumer joins the group, and only once the cluster reports
//! that member does a producer on a second client start feeding it. The window runs from the
//! first delivery to the end of the last decode, so joining the group, creating the topic and the
//! first allocations behind them are startup cost and sit outside it. Every run owns a fresh
//! topic and a fresh consumer group, and drops the topic when it is done, so a run never sees
//! what the one before it left behind.
//!
//! The message count is not a constant: a probe run measures the raw loop's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on.
//!
//! The three loops are interleaved - raw, adapter, framework, raw, adapter, framework - and each
//! reports its best round: noise only ever slows a run down, so the fastest round is the closest
//! to the undisturbed cost. Running one loop to the end and then the next would charge every
//! drift of the machine to whichever ran last.
//!
//! # How the broker-bound flag is decided
//!
//! A row is broker-bound when the raw client spent most of the run waiting on the socket, so what
//! the crate adds happened inside a wait that was already being paid. That is decided by
//! measurement, never by watching whether the producer had to wait for the consumer: a round-trip
//! probe outside every loop times one request and its answer, a second probe counts the requests
//! librdkafka actually sends per delivery, and the row is flagged when their product reaches half
//! the measured time per message. Kafka charges no request per message - a fetch answers with as
//! many records as it can carry - so the count is far below one and the arithmetic is what says
//! whether the round trip is paid per delivery at all.

use std::convert::Infallible;
use std::env;
use std::fmt::Write as _;
use std::hint::black_box;
use std::iter::repeat_n;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::sleep as block_for;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt as _;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{Client, ClientContext, DefaultClientContext};
use rdkafka::consumer::{BaseConsumer, Consumer as _, ConsumerContext, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message as _;
use rdkafka::producer::{
    BaseRecord, DeliveryResult, Producer as _, ProducerContext, ThreadedProducer,
};
use rdkafka::statistics::Statistics;
use ruststream::runtime::RunningApp;
use ruststream::{Broker as _, ConnectedBroker as _, IncomingMessage as _, Subscriber as _};
use ruststream_rdkafka::prelude::*;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::task;
use tokio::time::timeout;

// A benchmark measures what ships. With the framework's harness feature compiled in, this crate
// carries its in-process mode beside the live transport and every delivery records what the
// handler saw, so a number taken with it on is not the production path. The benchmark lives in a package
// of its own for the same reason: `ruststream-rdkafka`'s dev-dependencies enable that feature
// through the conformance harness, and a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// Deliveries the probe run takes to measure the raw loop's rate.
const PROBE_MESSAGES: usize = 200_000;
/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin the fastest
/// scenario lands just under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count, so a machine an order faster does not turn a run into an
/// afternoon. At [`BODY_BYTES`] a run at the ceiling writes eight gigabytes into the topic it
/// then drops.
const MAX_MESSAGES: usize = 16_000_000;
/// Rounds run. The best of them is reported.
const ROUNDS: usize = 3;
/// Worker threads every loop is driven on.
const WORKERS: usize = 4;

/// Requests the round-trip probe times.
const ROUND_TRIPS: u32 = 20_000;
/// The probe stops early rather than spending an unbounded time on a slow cluster; the mean is
/// taken over the requests it did complete.
const ROUND_TRIP_BUDGET: Duration = Duration::from_secs(10);
/// How often librdkafka publishes its request counters during the accounting run.
const STATS_INTERVAL_MS: &str = "100";

/// Partitions the run's topic is created with.
///
/// One: the loop under measurement is serial in all three forms, and a single partition is the
/// shape in which Kafka's own ordering and the tracked watermark are least ambiguous.
const PARTITIONS: i32 = 1;
/// How far ahead of the consumer the producer may run, in records the broker has acknowledged.
///
/// Acknowledged rather than handed to librdkafka: the client's own queue holds records the
/// consumer could not have seen yet, and counting those would report the producer as held back by
/// a consumer that is not behind at all.
const IN_FLIGHT: usize = 65_536;
/// How often the producer checks that ceiling.
const CHECK_EVERY: usize = 512;
/// How long a run may go without a delivery before it is called stuck.
const STALL: Duration = Duration::from_secs(60);
/// How long a consumer group is given to report its member before a run gives up waiting.
const JOIN: Duration = Duration::from_secs(30);
/// How long a librdkafka metadata or admin call may block.
const CALL: Duration = Duration::from_secs(30);
/// How long the producer waits before re-checking a ceiling it is held by.
const BACKOFF: Duration = Duration::from_micros(200);

/// The body size every loop publishes and decodes, to the byte: the scenario is published under
/// this number, so the bytes on the wire have to be it.
const BODY_BYTES: usize = 512;
/// How wide one padding value is before the next field starts.
const PAD_WIDTH: usize = 16;
/// The values every body carries. Fixed, so every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// What every loop decodes a delivery into.
///
/// Two integer fields the loop reads, and a padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// A JSON body carrying the two fields, padded with fields [`Order`] ignores until it is exactly
/// `size` bytes.
///
/// The padding is a run of equally wide fields and one last field cut to whatever is left, so a
/// scenario published as a 512 byte body is one. Building it is startup work, and the assertion
/// below holds the promise the published name makes.
fn json_body(size: usize) -> Vec<u8> {
    let mut body = format!("{{\"id\":{ID},\"quantity\":{QUANTITY}");
    let mut field = 0u32;
    loop {
        let key = format!(",\"f{field}\":\"\"");
        // One byte stays reserved for the closing brace.
        let Some(room) = size.checked_sub(body.len() + key.len() + 1) else {
            break;
        };
        // A full-width field only when what it leaves behind can still hold the next one, whose
        // key is at most one digit longer. Otherwise this is the last field and it takes the
        // rest, because a remainder too small to start a field would come out as a short body.
        let width = if room > PAD_WIDTH + key.len() {
            PAD_WIDTH
        } else {
            room
        };
        body.push_str(&key[..key.len() - 1]);
        body.extend(repeat_n('x', width));
        body.push('"');
        field += 1;
    }
    body.push('}');
    assert_eq!(
        body.len(),
        size,
        "a body has to be the size the scenario publishes"
    );
    body.into_bytes()
}

/// The names one run owns: nothing is shared with the run before it.
#[derive(Clone, Debug)]
struct Names {
    topic: String,
    group: String,
}

impl Names {
    fn fresh() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        Self {
            topic: format!("ruststream-bench-{stamp}"),
            group: format!("rs-bench-{stamp}"),
        }
    }
}

/// The names the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run that is starting. A run installs its own names here first, so the
/// subscription the framework opens is the one this run publishes to.
static NAMES: Mutex<Option<Names>> = Mutex::new(None);

fn install(names: &Names) {
    *NAMES
        .lock()
        .expect("the names cell is never held across a panic") = Some(names.clone());
}

fn installed() -> Names {
    NAMES
        .lock()
        .expect("the names cell is never held across a panic")
        .clone()
        .expect("a run installs its names before it builds the service")
}

/// Counts deliveries and marks the ends of the measured window.
///
/// Every loop calls the same methods, so all three pay for the signal. A delivery pays one relaxed
/// increment and two comparisons; the waiter is a single future for the whole run, woken once.
#[derive(Clone, Debug)]
struct Run(Arc<RunInner>);

#[derive(Debug)]
struct RunInner {
    total: usize,
    seen: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Run {
    fn new(total: usize) -> Self {
        Self(Arc::new(RunInner {
            total,
            seen: AtomicUsize::new(0),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, and answers whether the run is over.
    fn arrived(&self) -> bool {
        let seen = self.0.seen.fetch_add(1, Ordering::Relaxed) + 1;
        if seen == 1 {
            let _ = self.0.first.set(Instant::now());
        }
        if seen == self.0.total {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
        seen >= self.0.total
    }

    fn handled(&self) -> usize {
        self.0.seen.load(Ordering::Acquire).min(self.0.total)
    }

    /// Resolves once every expected delivery has been handled.
    async fn drained(&self) {
        while self.0.seen.load(Ordering::Acquire) < self.0.total {
            self.0.drained.notified().await;
        }
    }

    /// The measured window: the first delivery to the end of the last decode.
    fn window(&self) -> Duration {
        let first = *self.0.first.get().expect("the run took a delivery");
        let last = *self.0.last.get().expect("the run took its last delivery");
        last - first
    }
}

/// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
async fn drain(run: &Run, loop_name: &str) {
    let mut seen = 0;
    loop {
        if timeout(STALL, run.drained()).await.is_ok() {
            return;
        }
        let handled = run.handled();
        assert!(
            handled > seen,
            "{loop_name}: {handled} of {} deliveries handled and nothing moved for {STALL:?}",
            run.0.total
        );
        seen = handled;
    }
}

/// What one measured run produced.
#[derive(Clone, Copy, Debug)]
struct Sample {
    window: Duration,
    /// How often the producer had to wait for the consumer, for the log: the broker-bound verdict
    /// is decided by the round-trip probes, not by this.
    throttled: usize,
}

impl Sample {
    fn rate(self, messages: usize) -> f64 {
        messages as f64 / self.window.as_secs_f64()
    }
}

// ---------------------------------------------------------------------------------------------
// The client configurations every loop shares
// ---------------------------------------------------------------------------------------------

/// The subscription this crate is asked for, and whose resolved client properties the raw loop
/// spells out below.
fn descriptor(names: &Names, tracked: bool) -> KafkaTopic {
    let topic = KafkaTopic::new(&names.topic)
        .group(&names.group)
        .start(StartOffset::Earliest);
    if tracked {
        topic.commit(Commit::Tracked)
    } else {
        topic
    }
}

/// The consumer properties this crate resolves for [`descriptor`], spelled out so the raw loop
/// asks librdkafka for exactly the same client.
///
/// `KafkaBroker::new([url])` contributes `bootstrap.servers` and nothing else; the descriptor
/// contributes the group, `auto.offset.reset` for [`StartOffset::Earliest`], and - under
/// [`Commit::Tracked`] - the offset store this crate takes over.
fn consumer_config(url: &str, names: &Names, tracked: bool) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", url);
    config.set("group.id", &names.group);
    config.set("auto.offset.reset", "earliest");
    if tracked {
        config.set("enable.auto.offset.store", "false");
    }
    config
}

/// Counts what the broker acknowledged, so the producer can bound its lead by records the
/// consumer could actually have seen.
#[derive(Clone, Debug, Default)]
struct Feed {
    delivered: Arc<AtomicUsize>,
    failed: Arc<AtomicUsize>,
}

impl ClientContext for Feed {}

impl ProducerContext for Feed {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, (): Self::DeliveryOpaque) {
        // The callback runs on librdkafka's polling thread, where a panic would take the run down
        // with no diagnosis; a failure is counted here and asserted on where the producer is
        // joined.
        let counter = if result.is_ok() {
            &self.delivered
        } else {
            &self.failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// The producer that feeds a run.
///
/// It is not the subject of the measurement - all three loops are fed by this same code - so it is
/// configured for throughput: the consumer is what the row is about, and a producer that paces it
/// turns the row into a measurement of the server.
fn producer(url: &str) -> ThreadedProducer<Feed> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", url);
    config.set("acks", "1");
    config.set("linger.ms", "20");
    config.set("batch.num.messages", "50000");
    config.set("batch.size", "1048576");
    config.set("queue.buffering.max.messages", "200000");
    config.set("queue.buffering.max.kbytes", "1048576");
    config.set("compression.type", "none");
    config
        .create_with_context(Feed::default())
        .expect("the producer client is created")
}

/// Creates the run's topic and waits for the cluster to report it.
///
/// Explicit rather than left to the broker's auto-creation: a consumer that subscribes to a topic
/// that does not exist yet spends its first seconds on metadata refreshes, and that would land
/// inside the window of whichever loop raced it.
async fn create_topic(url: &str, names: &Names) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("the admin client is created");
    let topic = NewTopic::new(&names.topic, PARTITIONS, TopicReplication::Fixed(1))
        // The single-node stand deletes a topic's segments `file.delete.delay.ms` after the topic
        // goes; the default minute would keep several runs' logs on disk at once.
        .set("file.delete.delay.ms", "1000");
    let results = admin
        .create_topics([&topic], &AdminOptions::new())
        .await
        .expect("the topic creation request is accepted");
    for result in results {
        result.expect("the topic is created");
    }
}

/// Drops the run's topic once its numbers are taken, so the stand does not accumulate a log per
/// run.
async fn delete_topic(url: &str, names: &Names) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("the admin client is created");
    let results = admin
        .delete_topics(&[names.topic.as_str()], &AdminOptions::new())
        .await
        .expect("the topic deletion request is accepted");
    for result in results {
        result.expect("the topic is deleted");
    }
}

/// Blocks until the cluster reports a member in the run's consumer group.
///
/// This is what "the consumer is attached before the first message is published" means on Kafka:
/// a subscription is a join, and a producer that starts before the join finishes leaves the
/// consumer a backlog to work through instead of the live stream the other loops read. The group
/// is read from the cluster, so all three are held to the same condition - the framework's
/// subscription offers nothing to observe from outside.
fn await_group(client: &Client<Feed>, group: &str) {
    let deadline = Instant::now() + JOIN;
    loop {
        let joined = client
            .fetch_group_list(Some(group), CALL)
            .is_ok_and(|list| {
                list.groups()
                    .iter()
                    .any(|found| found.name() == group && !found.members().is_empty())
            });
        if joined {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the consumer group {group} never reported a member within {JOIN:?}"
        );
        block_for(BACKOFF * 500);
    }
}

/// Waits for the group, then feeds the run, never letting the consumer fall further behind than
/// [`IN_FLIGHT`].
///
/// Synchronous on purpose: `send` hands the record to librdkafka's queue and returns, so a
/// blocking loop on a blocking thread feeds faster than a future per record would, and the
/// producer is not what the window measures.
fn feed(producer: &ThreadedProducer<Feed>, names: &Names, messages: usize, run: &Run) -> usize {
    await_group(producer.client(), &names.group);
    let context = Arc::clone(producer.client().context());
    let topic = names.topic.as_str();
    let body = json_body(BODY_BYTES);
    let mut throttled = 0;
    for sent in 0..messages {
        if sent % CHECK_EVERY == 0 {
            while context
                .delivered
                .load(Ordering::Relaxed)
                .saturating_sub(run.handled())
                > IN_FLIGHT
            {
                throttled += 1;
                block_for(BACKOFF);
            }
        }
        let mut record = BaseRecord::<(), [u8]>::to(topic).payload(&body);
        // A full client queue is the producer's own limit, not the consumer's: it is waited out
        // here and never counted as the consumer holding the producer back.
        while let Err((err, held)) = producer.send(record) {
            assert!(
                matches!(
                    err,
                    KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
                ),
                "the producer refused a record: {err}"
            );
            block_for(BACKOFF);
            record = held;
        }
    }
    producer.flush(CALL).expect("the producer flushes");
    assert_eq!(
        context.failed.load(Ordering::Relaxed),
        0,
        "every record of a run has to reach the topic"
    );
    throttled
}

/// Feeds the run from a blocking thread and reports how often the consumer held it back.
async fn feed_on_blocking_thread(url: &str, names: &Names, messages: usize, run: Run) -> usize {
    let producer = producer(url);
    let names = names.clone();
    task::spawn_blocking(move || feed(&producer, &names, messages, &run))
        .await
        .expect("the feeding thread ends")
}

// ---------------------------------------------------------------------------------------------
// raw: the client, driven directly
// ---------------------------------------------------------------------------------------------

/// The loop the raw form runs, over any client context, so the accounting run below measures the
/// very code the measured runs do.
fn spawn_raw_loop<C>(
    consumer: Arc<StreamConsumer<C>>,
    run: Run,
    tracked: bool,
) -> task::JoinHandle<()>
where
    C: ConsumerContext + 'static,
{
    task::spawn(async move {
        loop {
            let delivery = consumer.recv().await.expect("the consumer delivers");
            let order: Order =
                serde_json::from_slice(delivery.payload().expect("a delivery carries its body"))
                    .expect("the body decodes");
            black_box((order.id, order.quantity));
            // This crate stores the offset once `ack` is called, so the window closes before the
            // store here too.
            let done = run.arrived();
            if tracked {
                consumer
                    .store_offset_from_message(&delivery)
                    .expect("the offset is stored");
            }
            if done {
                break;
            }
        }
    })
}

async fn raw(url: &str, names: &Names, messages: usize, tracked: bool) -> Sample {
    create_topic(url, names).await;
    let consumer: StreamConsumer = consumer_config(url, names, tracked)
        .create()
        .expect("the consumer client is created");
    consumer
        .subscribe(&[names.topic.as_str()])
        .expect("the consumer subscribes");
    let consumer = Arc::new(consumer);

    let run = Run::new(messages);
    let consuming = spawn_raw_loop(Arc::clone(&consumer), run.clone(), tracked);
    let throttled = feed_on_blocking_thread(url, names, messages, run.clone()).await;
    drain(&run, "raw").await;
    consuming.await.expect("the consuming task ends");
    drop(consumer);
    let sample = Sample {
        window: run.window(),
        throttled,
    };
    delete_topic(url, names).await;
    sample
}

// ---------------------------------------------------------------------------------------------
// adapter: this crate's own consumer, hand-driven
// ---------------------------------------------------------------------------------------------

/// Drains the run through this crate's subscription and its `ack`.
///
/// A loop in the benchmark, not a service: what it exercises is the subscription this crate opens,
/// the stream it yields, the delivery it hands out and the settlement it performs, and nothing
/// above them.
async fn adapter(url: &str, names: &Names, messages: usize, tracked: bool) -> Sample {
    create_topic(url, names).await;
    let connected = KafkaBroker::new([url])
        .connect()
        .await
        .expect("the broker connects");
    let subscriber = connected
        .subscribe_with(descriptor(names, tracked))
        .await
        .expect("the subscription opens");

    let run = Run::new(messages);
    let consuming = task::spawn({
        let run = run.clone();
        async move {
            let mut subscriber = subscriber;
            // The stream borrows the subscriber and holds a future across its own yields, so it
            // is pinned where it is polled rather than moved.
            let mut stream = pin!(subscriber.stream());
            while let Some(delivery) = stream.next().await {
                let message = delivery.expect("the subscription delivers");
                let order: Order =
                    serde_json::from_slice(message.payload()).expect("the body decodes");
                black_box((order.id, order.quantity));
                // The window closes on the decode, before the acknowledgement, in every loop.
                let done = run.arrived();
                message.ack().await.expect("the delivery is acknowledged");
                if done {
                    break;
                }
            }
        }
    });

    let throttled = feed_on_blocking_thread(url, names, messages, run.clone()).await;
    drain(&run, "adapter").await;
    consuming.await.expect("the consuming task ends");
    connected.shutdown().await.expect("the broker shuts down");
    let sample = Sample {
        window: run.window(),
        throttled,
    };
    delete_topic(url, names).await;
    sample
}

// ---------------------------------------------------------------------------------------------
// framework: the service a user writes
// ---------------------------------------------------------------------------------------------

#[subscriber(
    KafkaTopic::new(installed().topic)
        .group(installed().group)
        .start(StartOffset::Earliest)
)]
async fn auto_consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber(
    KafkaTopic::new(installed().topic)
        .group(installed().group)
        .start(StartOffset::Earliest)
        .commit(Commit::Tracked)
)]
async fn tracked_consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

async fn start(url: &str, run: Run, tracked: bool) -> RunningApp {
    let app = RustStream::new(AppInfo::new("kafka-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run));
    if tracked {
        app.with_broker(KafkaBroker::new([url]), |b| {
            b.include(tracked_consume);
        })
        .start()
        .await
    } else {
        app.with_broker(KafkaBroker::new([url]), |b| {
            b.include(auto_consume);
        })
        .start()
        .await
    }
    .expect("the service starts")
}

async fn framework(url: &str, names: &Names, messages: usize, tracked: bool) -> Sample {
    create_topic(url, names).await;
    let run = Run::new(messages);
    install(names);
    let app = start(url, run.clone(), tracked).await;

    let throttled = feed_on_blocking_thread(url, names, messages, run.clone()).await;
    drain(&run, "framework").await;
    app.shutdown().await.expect("the service stops");
    let sample = Sample {
        window: run.window(),
        throttled,
    };
    delete_topic(url, names).await;
    sample
}

// ---------------------------------------------------------------------------------------------
// The two probes behind the broker-bound verdict
// ---------------------------------------------------------------------------------------------

/// The transport's round-trip time, measured outside every loop.
///
/// A metadata request for one topic is the cheapest question a client can ask this cluster whose
/// answer it waits for, so the loop below times a request and its response and nothing else, on
/// one connection.
fn round_trip(url: &str, topic: &str) -> Duration {
    let client: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", url)
        .set("group.id", "ruststream-bench-round-trip")
        .create()
        .expect("the probe client is created");
    // The first request pays for the connection and the API version handshake.
    client
        .fetch_metadata(Some(topic), CALL)
        .expect("the cluster answers");

    let start = Instant::now();
    let mut done = 0;
    while done < ROUND_TRIPS {
        client
            .fetch_metadata(Some(topic), CALL)
            .expect("the cluster answers");
        done += 1;
        if start.elapsed() > ROUND_TRIP_BUDGET {
            break;
        }
    }
    start.elapsed() / done
}

/// The request counters of the last statistics snapshot librdkafka published.
#[derive(Clone, Debug, Default)]
struct Accounting(Arc<Mutex<Option<(i64, i64)>>>);

impl Accounting {
    /// Requests sent and messages received, as of the last snapshot.
    fn last(&self) -> Option<(i64, i64)> {
        *self
            .0
            .lock()
            .expect("the statistics cell is never held across a panic")
    }
}

impl ClientContext for Accounting {
    fn stats(&self, statistics: Statistics) {
        let requests = statistics
            .brokers
            .values()
            .flat_map(|broker| broker.req.values())
            .sum();
        *self
            .0
            .lock()
            .expect("the statistics cell is never held across a panic") =
            Some((requests, statistics.rxmsgs));
    }
}

impl ConsumerContext for Accounting {}

/// How many requests librdkafka sends the cluster per delivery.
///
/// Kafka charges no request per message: a fetch answers with as many records as it can carry and
/// an offset store is local, so the figure is a ratio rather than a constant, and it is what
/// decides whether the round-trip time is paid per delivery at all. It is read from the client's
/// own counters, which is why this run is separate from the measured rounds: the statistics
/// callback is a property this run has and they do not.
///
/// Both counters come from the same snapshot, so the ratio does not depend on how much of the run
/// the snapshot covers. What it does include is the handful of requests the client spends joining
/// the group - a rounding error against millions of deliveries, and one that can only push the
/// verdict towards flagging the row.
async fn requests_per_delivery(url: &str, names: &Names, messages: usize, tracked: bool) -> f64 {
    create_topic(url, names).await;
    let accounting = Accounting::default();
    let consumer: StreamConsumer<Accounting> = consumer_config(url, names, tracked)
        .set("statistics.interval.ms", STATS_INTERVAL_MS)
        .create_with_context(accounting.clone())
        .expect("the accounting consumer is created");
    consumer
        .subscribe(&[names.topic.as_str()])
        .expect("the consumer subscribes");
    let consumer = Arc::new(consumer);

    let run = Run::new(messages);
    let consuming = spawn_raw_loop(Arc::clone(&consumer), run.clone(), tracked);
    feed_on_blocking_thread(url, names, messages, run.clone()).await;
    drain(&run, "accounting").await;
    consuming.await.expect("the consuming task ends");
    drop(consumer);
    delete_topic(url, names).await;

    let (requests, received) = accounting
        .last()
        .expect("librdkafka published a statistics snapshot");
    assert!(received > 0, "the accounting run received no deliveries");
    requests as f64 / received as f64
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Auto,
    Tracked,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Self::Auto => "consumer group, auto-commit, 512 B JSON",
            Self::Tracked => "consumer group, tracked commits, 512 B JSON, ack each",
        }
    }

    const fn tracked(self) -> bool {
        matches!(self, Self::Tracked)
    }
}

/// Best and worst of the rounds.
///
/// Noise on the machine only ever slows a run down, so the fastest round is the closest to the
/// undisturbed cost, and the slowest says how far from quiet the machine was.
#[derive(Clone, Copy, Debug)]
struct Stats {
    best: f64,
    worst: f64,
}

impl Stats {
    fn of(rates: &[f64]) -> Self {
        assert!(!rates.is_empty(), "no round was run");
        Self {
            best: rates.iter().copied().fold(f64::MIN, f64::max),
            worst: rates.iter().copied().fold(f64::MAX, f64::min),
        }
    }

    fn spread(self) -> f64 {
        self.best - self.worst
    }
}

/// A difference smaller than the run-to-run spread of either side is not a percentage anyone
/// measured, so it is published as a verdict instead.
fn verdict(raw: Stats, other: Stats) -> &'static str {
    if (raw.best - other.best).abs() < raw.spread().max(other.spread()) {
        "indistinguishable"
    } else {
        "measured"
    }
}

#[derive(Debug)]
struct Measured {
    scenario: Scenario,
    messages: usize,
    rounds: usize,
    raw: Stats,
    adapter: Stats,
    framework: Stats,
    overhead_percent: f64,
    adapter_overhead_percent: f64,
    verdict: &'static str,
    adapter_verdict: &'static str,
    broker_bound: bool,
}

async fn measure(
    scenario: Scenario,
    url: &str,
    rounds: usize,
    seconds: f64,
    round_trip: Duration,
) -> Measured {
    let tracked = scenario.tracked();
    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`.
    let probe = raw(url, &Names::fresh(), PROBE_MESSAGES, tracked).await;
    let messages = ((probe.rate(PROBE_MESSAGES) * seconds * MARGIN) as usize)
        .clamp(PROBE_MESSAGES, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run ({:.0} msg/s probed)",
        scenario.name(),
        probe.rate(PROBE_MESSAGES)
    );

    let per_delivery = requests_per_delivery(url, &Names::fresh(), messages, tracked).await;
    println!(
        "  {per_delivery:.4} requests per delivery, {:.0} us per round trip",
        round_trip.as_secs_f64() * 1e6,
    );

    let mut raws = Vec::with_capacity(rounds);
    let mut adapters = Vec::with_capacity(rounds);
    let mut frameworks = Vec::with_capacity(rounds);
    let mut throttled = 0;
    for round in 1..=rounds {
        let raw = raw(url, &Names::fresh(), messages, tracked).await;
        let adapter = adapter(url, &Names::fresh(), messages, tracked).await;
        let framework = framework(url, &Names::fresh(), messages, tracked).await;
        println!(
            "  round {round:>2}: raw {:>10.0}, adapter {:>10.0}, framework {:>10.0} msg/s",
            raw.rate(messages),
            adapter.rate(messages),
            framework.rate(messages)
        );
        raws.push(raw.rate(messages));
        adapters.push(adapter.rate(messages));
        frameworks.push(framework.rate(messages));
        throttled += raw.throttled + adapter.throttled + framework.throttled;
    }
    println!("  the producer waited for a consumer {throttled} times");

    let raw = Stats::of(&raws);
    let adapter = Stats::of(&adapters);
    let framework = Stats::of(&frameworks);
    // The flag the core's page defines: the raw client spent most of the run waiting on the
    // socket, so what this crate adds happened inside a wait already being paid.
    let waiting = per_delivery * round_trip.as_secs_f64();
    Measured {
        scenario,
        messages,
        rounds,
        raw,
        adapter,
        framework,
        overhead_percent: (raw.best - framework.best) / raw.best * 100.0,
        adapter_overhead_percent: (raw.best - adapter.best) / raw.best * 100.0,
        verdict: verdict(raw, framework),
        adapter_verdict: verdict(raw, adapter),
        broker_bound: waiting >= 0.5 / raw.best,
    }
}

fn document(measured: &[Measured], round_trip: Duration) -> String {
    let mut out = String::new();
    write!(
        out,
        "{{\n  \"round_trip_micros\": {:.1},\n  \"scenarios\": [\n",
        round_trip.as_secs_f64() * 1e6
    )
    .expect("writing to a String");
    for (index, row) in measured.iter().enumerate() {
        let comma = if index + 1 == measured.len() { "" } else { "," };
        write!(
            out,
            concat!(
                "    {{\n",
                "      \"name\": \"{name}\",\n",
                "      \"unit\": \"msg/s\",\n",
                "      \"messages\": {messages},\n",
                "      \"pairs\": {rounds},\n",
                "      \"raw\": {{ \"best\": {raw_best:.0}, \"worst\": {raw_worst:.0} }},\n",
                "      \"adapter\": {{ \"best\": {ad_best:.0}, \"worst\": {ad_worst:.0} }},\n",
                "      \"framework\": {{ \"best\": {fw_best:.0}, \"worst\": {fw_worst:.0} }},\n",
                "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
                "      \"overhead_percent\": {overhead:.1},\n",
                "      \"adapter_verdict\": \"{adapter_verdict}\",\n",
                "      \"verdict\": \"{verdict}\",\n",
                "      \"broker_bound\": {broker_bound}\n",
                "    }}{comma}\n",
            ),
            name = row.scenario.name(),
            messages = row.messages,
            rounds = row.rounds,
            raw_best = row.raw.best,
            raw_worst = row.raw.worst,
            ad_best = row.adapter.best,
            ad_worst = row.adapter.worst,
            fw_best = row.framework.best,
            fw_worst = row.framework.worst,
            adapter_overhead = row.adapter_overhead_percent,
            overhead = row.overhead_percent,
            adapter_verdict = row.adapter_verdict,
            verdict = row.verdict,
            broker_bound = row.broker_bound,
            comma = comma,
        )
        .expect("writing to a String");
    }
    out.push_str("  ]\n}\n");
    out
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

/// A positive count from the environment, or the default.
///
/// The parse target rejects zero, so a round count of zero is refused here rather than after the
/// probe run, where it would panic in the statistics with no round to report.
fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
            .get()
    })
}

fn main() {
    let url = env::var("KAFKA_TEST_URL")
        .expect("KAFKA_TEST_URL names the cluster to measure against; `just bench` sets it");
    let rounds = number("RUSTSTREAM_BENCH_PAIRS", ROUNDS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    // One transport, one round-trip time: the probe runs once, on a topic of its own, before any
    // round is measured.
    let probe_names = Names::fresh();
    runtime.block_on(create_topic(&url, &probe_names));
    let round_trip = round_trip(&url, &probe_names.topic);
    runtime.block_on(delete_topic(&url, &probe_names));

    let measured: Vec<Measured> = [Scenario::Auto, Scenario::Tracked]
        .into_iter()
        .map(|scenario| runtime.block_on(measure(scenario, &url, rounds, seconds, round_trip)))
        .collect();

    println!();
    for row in &measured {
        println!(
            "{}: raw {:.0}, adapter {:.0} ({:.1}%, {}), framework {:.0} ({:.1}%, {}){}",
            row.scenario.name(),
            row.raw.best,
            row.adapter.best,
            row.adapter_overhead_percent,
            row.adapter_verdict,
            row.framework.best,
            row.overhead_percent,
            row.verdict,
            if row.broker_bound {
                ", broker-bound"
            } else {
                ""
            }
        );
    }

    std::fs::write(&out, document(&measured, round_trip)).expect("the summary is written");
    println!("\nwrote {out}");
}
