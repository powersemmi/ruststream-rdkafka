//! Repositioning: opening a subscription at a fixed point in the log, and moving a live one
//! from inside a handler.
//!
//! Kafka keeps the log, so a consumer can be moved through it. Three forms appear here: the
//! `start_at(..)` clause, which forces a starting position on every startup; the `SeekHandle`
//! context key, which hands a per-message handler the running subscription's seeker; and the
//! same key on the page-scoped context a batch handler declares.
//!
//! A seek moves this consumer instance over the partitions it holds - it is not a group
//! operation, and a rebalance discards it (the subscription then resumes from the committed
//! offsets).
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_seek -- run
//! ```

use ruststream_rdkafka::context::KafkaBatchContext;
use ruststream_rdkafka::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct AuditEntry {
    id: u64,
}

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
    /// Set by the producer when a run of jobs is known to be unprocessable; the handler jumps
    /// the partition past it instead of failing its way through record by record.
    resume_at: Option<i64>,
}

// --8<-- [start:start_at]
// `start_at` forces the starting position every time the service starts, whatever the group
// committed before. The audit projection is rebuilt from the whole retained log on each boot,
// so the descriptor's `StartOffset` (which only applies when the group has no committed offset)
// would not do.
#[subscriber(
    KafkaTopic::new("audit").group("audit-svc").commit(Commit::Tracked),
    start_at(KafkaPosition::earliest())
)]
async fn replay(entry: &AuditEntry) -> HandlerOutcome {
    println!("audit entry {}", entry.id);
    HandlerOutcome::ack()
}
// --8<-- [end:start_at]

// --8<-- [start:handler]
// The seeker is a field of the delivery's context: the subscription mints it when it opens, so
// the `SeekHandle` key hands the handler a live handle by construction. Here it skips a poison
// run - the partition jumps straight to `resume_at` instead of the handler failing its way
// through every record.
#[subscriber(
    KafkaTopic::new("jobs").group("jobs-svc").commit(Commit::Tracked),
    workers(4, by_key)
)]
async fn work(
    job: &Job,
    Ctx(partition): Ctx<Partition>,
    Ctx(seeker): Ctx<SeekHandle>,
) -> HandlerOutcome {
    if let Some(resume_at) = job.resume_at {
        // The delivery's own partition, at an absolute offset: everything between here and
        // there is skipped, and the offsets follow the seek, so nothing in between is
        // committed as handled.
        if seeker
            .seek(KafkaPosition::offset(partition, resume_at))
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
        return HandlerOutcome::ack();
    }
    println!("job {}", job.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:batch]
// A page spans many deliveries, so it gets the subscription-scoped context instead: the same
// `SeekHandle` key, no position (which record of the page would it name?). The target rides the
// elements, and the reposition applies once the whole page has settled.
#[subscriber(KafkaTopic::new("jobs.bulk").group("jobs-bulk-svc").commit(Commit::Tracked))]
async fn drain(page: &[Job], ctx: &mut Context<'_, KafkaBatchContext>) -> HandlerOutcome {
    println!("draining {} jobs", page.len());
    let resume_at = page.iter().find_map(|job| job.resume_at);
    if let Some(offset) = resume_at
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
// --8<-- [end:batch]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("jobs-svc"),
        |b| {
            b.include(replay);
            b.include(work);
            b.include(drain.batch(nonzero!(128)));
        },
    )
}
