//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service setup, the latch a handler counts deliveries down on, and the measurement
//! configuration. The method is the core's, described in its `benches/common` and on the
//! [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes on the broker a user connects: the app, built with
//! [`KafkaBroker::new`] pointed at the stand in `docker-compose.test.yml` (`KAFKA_TEST_URL`), and
//! started through [`RustStream::start`]. The subscription is a consumer group reading a topic of
//! one partition from its earliest offset, the descriptor the comparison's service loop opens.
//! Every run owns a fresh topic and a fresh group, so it never sees what the run before it left
//! behind.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] more and over twice as many.
//! The slope between the last two is the steady-state cost of a message: everything that happens
//! once is in both totals and cancels in the subtraction. The one-delivery run is the cold start,
//! reported on its own.
//!
//! The one delivery is a primer published before the service starts, so the cold region is
//! starting the service, joining the group and handling the first record. The rest is published
//! once the service has handled the primer, from another thread with a producer of its own, and
//! the producer's flush returns only once the broker has acknowledged every record. The service's
//! runtime is single-threaded and runs only inside `block_on`, so nothing is consumed while the
//! topic fills, and the fill is in no region.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. Everything on the service's thread inside the region is counted: the dispatcher, the
//! codec, this crate's code, the librdkafka calls this crate makes on that thread, and tokio's
//! share of driving them. What librdkafka does on threads of its own - fetching, the group
//! protocol, producing and its delivery reports - is not: callgrind collects per thread, and DHAT
//! reads only the allocations made under the measured frame. [`measure`] is the only frame that
//! carries its name, because a toggle on a name that also appears inside closure types switches
//! collection off again one frame deeper. The number DHAT reports is `Total blocks`, allocations
//! per run.
//!
//! The broker is real, so how often the service thread finds librdkafka's queue empty and waits
//! depends on timing, and every wait costs a wakeup. That is what moves a count between runs of
//! one binary.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::convert::Infallible;
use std::env;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer as _, ProducerContext};
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream_rdkafka::KafkaBroker;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::time::timeout;

// A benchmark measures what ships. With the framework's harness feature compiled in, every
// delivery records what the handler saw, so a number taken with it on is not the production path.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench-code`"
);

/// The topic every reply goes to. The reply type names it in its own `#[outgoing(..)]`
/// attribute, which takes a literal, so every run shares it; nothing reads it.
pub const REPLIES: &str = "confirmations";

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate and the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run after the primer: large enough that entering and leaving the
/// region is lost in the per-message number, small enough that a scenario stays within a minute of
/// valgrind time. `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 1_000;

/// How long a region may wait for its deliveries before the run is called stuck. Valgrind slows
/// the client down by an order of magnitude or more, and a reply waits for its acknowledgement.
const STALL: Duration = Duration::from_secs(600);

/// How long a librdkafka admin call or a producer flush may block.
const CALL: Duration = Duration::from_secs(120);

/// How many percent more instructions than the run it compares with a run may take before it
/// fails: the previous run on the machine, or `main` with `--baseline=main`.
///
/// Five rather than the core's two, because the broker is real: across three runs of one binary
/// the reply scenario's run of a thousand deliveries moved by 2.3 percent, since how often the
/// service's thread parks on a delivery report depends on timing. Consume and batch moved by less
/// than a hundredth of a percent.
const INSTRUCTION_LIMIT_PERCENT: f64 = 5.0;

/// The measurement configuration every gated scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what starting the
/// service and taking the first delivery allocate once; together they are the hard limit the
/// longest run of the scenario (twice [`MESSAGES`] deliveries after the primer) is held to, so
/// the run fails when the path allocates more than it does today. Both are floors the code is
/// held to, so a number that goes down is lowered here in the same change. The instruction limit
/// is relative, [`INSTRUCTION_LIMIT_PERCENT`]: `just bench-code --save-baseline=main` records a
/// baseline and `just bench-code --baseline=main` compares against it.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations do not come one per delivery: `steady` blocks per
/// `per` deliveries, as a batch handler allocates per batch.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .pass_through_env("KAFKA_TEST_URL")
        .tool(callgrind().soft_limits([(EventKind::Ir, INSTRUCTION_LIMIT_PERCENT)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    config
}

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`]. The division rounds up.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    cold + (steady * 2 * MESSAGES as u64).div_ceil(per)
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
///
/// The result passes through `black_box` so that the call to `body` stays a call. A body whose
/// result is `()` otherwise compiles to a tail jump, which leaves this frame off the stack:
/// callgrind still toggles on the entry, but DHAT reads the stack and would miss every allocation
/// of the region.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    black_box(body())
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// A single-threaded runtime: one thread carries the whole service, and nothing of it runs
/// outside `block_on`.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
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
            topic: format!("ruststream-code-{stamp}"),
            group: format!("rs-code-{stamp}"),
        }
    }
}

/// The names the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run being set up. A run installs its names here first, so the
/// subscription the service opens is the one this run publishes to.
static NAMES: Mutex<Option<Names>> = Mutex::new(None);

fn installed() -> Names {
    NAMES
        .lock()
        .expect("the names cell is never held across a panic")
        .clone()
        .expect("a run installs its names before it builds the service")
}

/// The topic of the run being set up, which every scenario's subscription reads.
pub fn topic() -> String {
    installed().topic
}

/// The consumer group of the run being set up, which every scenario's subscription joins.
pub fn group() -> String {
    installed().group
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every delivery carries: the two fields a handler reads.
pub fn json_body() -> Vec<u8> {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}").into_bytes()
}

/// A service that is built but not started, and what its topic will hold.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    url: String,
    topic: String,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<KafkaBroker, Identity, (), Latch>;

/// Builds a one-handler service on a fresh topic and group, and publishes the primer the cold
/// region takes. `messages` is what the drain region handles after it; zero leaves the run at the
/// cold start alone.
pub fn pending(messages: usize, mount: impl FnOnce(Mount<'_>)) -> Pending {
    let url = env::var("KAFKA_TEST_URL")
        .expect("KAFKA_TEST_URL names the cluster to measure against; `just bench-code` sets it");
    let names = Names::fresh();
    let runtime = runtime();
    runtime.block_on(create_topics(&url, &names.topic));
    *NAMES
        .lock()
        .expect("the names cell is never held across a panic") = Some(names.clone());

    let latch = Latch::default();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(KafkaBroker::new([url.as_str()]), mount);
    fill(&url, &names.topic, 1);
    Pending {
        runtime,
        latch,
        url,
        topic: names.topic,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
    }
}

/// Creates the run's topic, and the reply topic where an earlier run has not.
///
/// Explicit rather than left to the broker's auto-creation: a consumer that subscribes to a topic
/// that does not exist yet spends its first seconds on metadata refreshes, and a producer's first
/// record to one waits on the same.
async fn create_topics(url: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", url)
        .create()
        .expect("the admin client is created");
    let options = AdminOptions::new().operation_timeout(Some(CALL));
    let topics = [
        NewTopic::new(topic, 1, TopicReplication::Fixed(1)),
        NewTopic::new(REPLIES, 1, TopicReplication::Fixed(1)),
    ];
    let results = admin
        .create_topics(&topics, &options)
        .await
        .expect("the topic creation request is accepted");
    for result in results {
        match result {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => panic!("topic {name} is not created: {code}"),
        }
    }
}

/// Counts what the broker acknowledged and what it refused.
#[derive(Debug, Default)]
struct Confirmed {
    delivered: AtomicUsize,
    failed: AtomicUsize,
}

impl ClientContext for Confirmed {}

impl ProducerContext for Confirmed {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, (): Self::DeliveryOpaque) {
        let counter = if result.is_ok() {
            &self.delivered
        } else {
            &self.failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// Publishes `count` records to `topic` from a thread of its own, and returns once the broker has
/// acknowledged every one of them.
///
/// Part of every setup, never of a measured region, and never on the service's thread: the
/// service's runtime is not driven while this runs, so the records wait on the broker and what
/// the drain region pays for is consuming them, not producing them.
fn fill(url: &str, topic: &str, count: usize) {
    thread::scope(|scope| {
        scope
            .spawn(|| {
                let producer: BaseProducer<Confirmed> = ClientConfig::new()
                    .set("bootstrap.servers", url)
                    .create_with_context(Confirmed::default())
                    .expect("the producer client is created");
                let body = json_body();
                for _ in 0..count {
                    producer
                        .send(BaseRecord::<(), [u8]>::to(topic).payload(&body))
                        .map_err(|(err, _)| err)
                        .expect("the record is queued");
                }
                producer.flush(CALL).expect("every record is acknowledged");
                let context = producer.context();
                assert_eq!(
                    (
                        context.delivered.load(Ordering::Relaxed),
                        context.failed.load(Ordering::Relaxed)
                    ),
                    (count, 0),
                    "the broker acknowledges every record of the fill"
                );
            })
            .join()
            .expect("the fill thread finishes");
    });
}

/// Waits for the latch, and fails with what it was waiting for if it never drains.
fn drain(runtime: &Runtime, latch: &Latch) {
    runtime.block_on(async {
        timeout(STALL, latch.drained()).await.unwrap_or_else(|_| {
            panic!(
                "{} deliveries still expected after {STALL:?}",
                latch.remaining()
            )
        });
    });
}

/// Starts the service, takes the primer, fills its topic, and drains it: the shape of every
/// scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// the second the deliveries.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        url,
        topic,
        start,
        messages,
    } = pending;
    latch.expect(1);
    let running = measure(|| {
        let running = start(&runtime);
        drain(&runtime, &latch);
        running
    });
    if messages > 0 {
        latch.expect(messages);
        fill(&url, &topic, messages);
        assert_eq!(
            latch.remaining(),
            messages,
            "the topic was consumed while it was being filled, so the measured region would be short"
        );
        measure(|| drain(&runtime, &latch));
    }
    black_box(&topic);
    runtime
        .block_on(running.shutdown())
        .expect("the service stops");
}
