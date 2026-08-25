//! Repositioning: opening a subscription at a fixed point in the log, and moving a live one
//! from inside a handler.
//!
//! Kafka keeps the log, so a consumer can be moved through it. Two forms appear here: the
//! `start_at(..)` clause, which forces a starting position on every startup, and the `Seek`
//! handler parameter, which hands the running subscription's seeker to the handler so it can
//! skip or replay while it works.
//!
//! A seek moves this consumer instance over the partitions it holds - it is not a group
//! operation, and a rebalance discards it (the subscription then resumes from the committed
//! offsets).
//!
//! ```text
//! just brokers-up
//! cargo run --example kafka_seek -- run
//! ```

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
async fn replay(entry: &AuditEntry) -> HandlerResult {
    println!("audit entry {}", entry.id);
    HandlerResult::Ack
}
// --8<-- [end:start_at]

// --8<-- [start:handler]
// The seeker is a handler parameter: the runtime mints it off this subscription's own
// subscriber at startup, so it is live by construction. Here it skips a poison run - the
// partition jumps straight to `resume_at` instead of the handler failing through every record.
#[subscriber(
    KafkaTopic::new("jobs").group("jobs-svc").commit(Commit::Tracked),
    workers(4, by_key)
)]
async fn work(
    job: &Job,
    Ctx(partition): Ctx<Partition>,
    Seek(seeker): Seek<KafkaSeeker>,
) -> HandlerResult {
    if let Some(resume_at) = job.resume_at {
        // The delivery's own partition, at an absolute offset: everything between here and
        // there is skipped, and the offsets follow the seek, so nothing in between is
        // committed as handled.
        if seeker
            .seek(KafkaPosition::offset(partition, resume_at))
            .await
            .is_err()
        {
            return HandlerResult::retry();
        }
        return HandlerResult::Ack;
    }
    println!("job {}", job.id);
    HandlerResult::Ack
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("jobs-svc"),
        |b| {
            b.include(replay);
            b.include(work);
        },
    )
}
