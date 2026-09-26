//! Services under `TestApp` that move their own subscription or batch their deliveries: the
//! production app on `KafkaBroker`, connected in process.

#![cfg(feature = "testing")]

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ruststream::nonzero;
use ruststream::runtime::{AppInfo, Ctx, HandlerOutcome, Out, RustStream, SubscriberSettings as _};
use ruststream::subscriber;
use ruststream::testing::TestApp;
use ruststream::{OutSlot, Outgoing, Publisher, Seeker as _};
use ruststream_rdkafka::context::keys::{Position, SeekHandle};
use ruststream_rdkafka::context::{KafkaBatchContext, KafkaContext};
use ruststream_rdkafka::{KafkaBroker, KafkaPosition, KafkaPublish, KafkaTopic};
use serde::{Deserialize, Serialize};

/// The broker every app here runs on, configured as a service configures it.
fn broker() -> KafkaBroker {
    KafkaBroker::new(["kafka:9092"]).default_group("svc")
}

// ------------------------------------------------------------------ repositioning a service

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Job {
    id: u64,
}

/// The handler's own rewind budget, held in typed app state.
#[derive(Clone, Default)]
struct Rewinds(Arc<AtomicUsize>);

#[subscriber(KafkaTopic::new("seek-jobs"))]
async fn rewind_stuck_job(
    job: &Job,
    ctx: &mut Context<'_, KafkaContext, Rewinds>,
    Ctx(here): Ctx<Position>,
    Ctx(seeker): Ctx<SeekHandle>,
) -> HandlerOutcome {
    if job.id == 1
        && ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0
        && seeker.seek(here).await.is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_replays_its_own_delivery_position_through_the_context() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(|()| async { Ok::<_, Infallible>(Rewinds::default()) })
        .with_broker(broker(), |b| {
            b.include(rewind_stuck_job);
        });
    let tb = TestApp::start(app).await.expect("start");

    for id in 0..3 {
        tb.broker::<KafkaBroker>()
            .message(&Job { id })
            .to("seek-jobs")
            .publish()
            .await
            .expect("publish drives the reaction, replay included, to a standstill");
    }

    let seen: Vec<u64> = tb
        .broker::<KafkaBroker>()
        .subscriber("seek-jobs")
        .received::<Job>()
        .into_iter()
        .map(|job| job.id)
        .collect();
    assert_eq!(
        seen,
        [0, 1, 1, 2],
        "the seek to its own position redelivers the record"
    );
    tb.shutdown().await.expect("shutdown");
}

/// The producer's cursor contract: an element carrying `resume_at` asks the consumer to
/// reposition the subscription there once the batch is settled.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Cursor {
    id: u64,
    resume_at: Option<i64>,
}

#[subscriber(KafkaTopic::new("seek-batches"))]
async fn drain_batches(
    cursors: &[Cursor],
    ctx: &mut Context<'_, KafkaBatchContext, Rewinds>,
) -> HandlerOutcome {
    let resume_at = cursors.iter().find_map(|entry| entry.resume_at);
    if let Some(offset) = resume_at
        && ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0
        && ctx
            .context(SeekHandle)
            .seek(KafkaPosition::offset(0, offset))
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// A batch body repositions through the subscription-scoped context.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_body_repositions_through_its_subscription_context() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(|()| async { Ok::<_, Infallible>(Rewinds::default()) })
        .with_broker(broker(), |b| {
            b.include(drain_batches.batch(nonzero!(8)));
        });
    let tb = TestApp::start(app).await.expect("start");

    for (id, resume_at) in [(0, Some(0)), (1, None)] {
        tb.broker::<KafkaBroker>()
            .message(&Cursor { id, resume_at })
            .to("seek-batches")
            .publish()
            .await
            .expect("publish");
    }

    let seen: Vec<u64> = tb
        .broker::<KafkaBroker>()
        .subscriber("seek-batches")
        .received::<Cursor>()
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    assert_eq!(
        seen,
        [0, 0, 1],
        "the reposition replays the record its element named"
    );
    tb.shutdown().await.expect("shutdown");
}

#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct Seed {
    jobs: u64,
}

#[derive(OutSlot)]
#[publishes(Job)]
struct Jobs;

/// Fans one request out into a run of jobs.
#[subscriber("batch-seed")]
async fn seed_jobs(seed: &Seed, Out(jobs): Out<impl Publisher, Jobs>) -> HandlerOutcome {
    for id in 0..seed.jobs {
        if jobs
            .message(&Job { id })
            .to("batch-sizes")
            .publish()
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

#[subscriber(KafkaTopic::new("batch-sizes"))]
async fn count_batches(jobs: &[Job]) -> HandlerOutcome {
    let _ = jobs;
    HandlerOutcome::ack()
}

/// A batch holds at most what the mount site asked for, and whatever the consumer had fetched.
/// On the current-thread runtime the fan-out lands in full before the batch handler runs.
#[tokio::test]
async fn a_batch_is_cut_at_the_size_the_mount_site_named() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker(), |b| {
        b.include(seed_jobs)
            .out(Jobs, KafkaPublish::default())
            .build();
        b.include(count_batches.batch(nonzero!(2)));
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<KafkaBroker>()
        .message(&Seed { jobs: 5 })
        .to("batch-seed")
        .publish()
        .await
        .expect("publish");

    tb.broker::<KafkaBroker>()
        .subscriber("batch-sizes")
        .assert_batch_sizes(&[2, 2, 1])
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown");
}
