//! The services of `examples/kafka_seek.rs` written without the macros: `.start_at(position)`
//! is a step of the mount chain, and a body reads the seek handle off its Kafka context.
//!
//! ```text
//! KAFKA_TEST_URL=127.0.0.1:9092 cargo test --example manual_kafka_seek --features testing
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::schemars::{self, JsonSchema};
use ruststream::{CallerName, FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct AuditEntry {
    id: u64,
}

// `#[outgoing(name = "ledger.audit")]` by hand: the type fixes its destination.
impl OutgoingDestination for AuditEntry {
    type Form = FixedName;

    const DESTINATION: &'static str = "ledger.audit";
}

impl MessageHeaders for AuditEntry {
    type Contract = NoHeaders;
}

/// A job; the producer sets `resume_at` when it knows the jobs up to that offset are
/// unprocessable.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Job {
    id: u64,
    resume_at: Option<i64>,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the topic.
impl OutgoingDestination for Job {
    type Form = CallerName;
}

impl MessageHeaders for Job {
    type Contract = NoHeaders;
}

mod start_at {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::AuditEntry;
    #[cfg(test)]
    use super::live;

    // --8<-- [start:start_at]
    /// Projects every audit entry it reads.
    struct Replay;

    impl Handle<AuditEntry> for Replay {
        fn handle(
            &self,
            entry: &AuditEntry,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(entry.id, "projected");
            ready(Ok(()))
        }
    }

    fn app(servers: &str, group: &str) -> impl App<State = ()> {
        let broker = KafkaBroker::new([servers]).default_group(group);
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
            let audit = KafkaTopic::new("ledger.audit").commit(Commit::Tracked);
            // The projection is rebuilt from the oldest retained entry on every startup.
            b.include(
                subscriber("ledger.audit", Replay)
                    .build()
                    .map_source(|_| audit)
                    .start_at(KafkaPosition::earliest()),
            );
        })
    }
    // --8<-- [end:start_at]

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_startup_replays_the_retained_log() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url() else {
            return Ok(());
        };
        live::recreate_topic(&url, "ledger.audit", 1).await;
        let group = live::group("audit");

        // The first run reads the entry and commits it.
        let first = TestApp::start_live_within(app(&url, &group), live::START).await?;
        first
            .broker::<KafkaBroker>()
            .message(&AuditEntry { id: 1 })
            .publish()
            .await?;
        first.shutdown().await?;

        let tb = TestApp::start_live_within(app(&url, &group), live::START).await?;
        tb.broker::<KafkaBroker>()
            .message(&AuditEntry { id: 2 })
            .publish()
            .await?;

        // A live publish returns once the subscription has handled as many records as the test
        // published, so the first of them is what decides: the entry the group had committed.
        let projected = tb
            .broker::<KafkaBroker>()
            .subscriber("ledger.audit")
            .received::<AuditEntry>();
        assert_eq!(projected.first(), Some(&AuditEntry { id: 1 }));
        tb.shutdown().await?;
        Ok(())
    }
}

mod handler {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::context::KafkaContext;
    use ruststream_rdkafka::prelude::*;

    use super::Job;

    // --8<-- [start:handler]
    /// Skips a run of jobs known to fail: the partition jumps to `resume_at`.
    struct Work;

    impl Handle<Job, (), (), KafkaContext> for Work {
        async fn handle(
            &self,
            job: &Job,
            _outs: &(),
            ctx: &mut Context<'_, KafkaContext>,
        ) -> Result<(), HandlerOutcome> {
            tracing::info!(job.id, "worked");
            let Some(resume_at) = job.resume_at else {
                return Ok(());
            };
            let position = KafkaPosition::offset(ctx.context(Partition), resume_at);
            ctx.context(SeekHandle)
                .seek(position)
                .await
                .map_err(|_| HandlerOutcome::retry())
        }
    }

    fn app() -> impl App<State = ()> {
        let broker = KafkaBroker::new(["localhost:9092"]);
        RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
            let jobs = KafkaTopic::new("jobs")
                .group("jobs-svc")
                .commit(Commit::Tracked);
            b.include(subscriber("jobs", Work).build().map_source(|_| jobs));
        })
    }
    // --8<-- [end:handler]

    #[tokio::test]
    async fn the_jobs_before_resume_at_are_skipped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // Job 1 says the jobs up to offset 4 cannot be processed; all five wait in the queue.
        let jobs: Vec<Job> = (0..5)
            .map(|id| Job {
                id,
                resume_at: (id == 1).then_some(4),
            })
            .collect();
        let broker = tb.broker::<KafkaBroker>();
        for published in join_all(
            jobs.iter()
                .map(|job| broker.message(job).to("jobs").publish()),
        )
        .await
        {
            published?;
        }

        let worked: Vec<u64> = broker
            .subscriber("jobs")
            .received::<Job>()
            .iter()
            .map(|job| job.id)
            .collect();
        assert_eq!(worked, [0, 1, 4]);
        Ok(())
    }
}

mod batch {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_rdkafka::context::KafkaBatchContext;
    use ruststream_rdkafka::prelude::*;

    use super::Job;

    // --8<-- [start:batch]
    /// A batch spans many records, so its context is the subscription's: the seek handle, and
    /// no position. The move applies once the whole batch has settled.
    struct Drain;

    impl Handle<[Job], (), (), KafkaBatchContext> for Drain {
        async fn handle(
            &self,
            jobs: &[Job],
            _outs: &(),
            ctx: &mut Context<'_, KafkaBatchContext>,
        ) -> Result<(), Vec<HandlerOutcome>> {
            tracing::info!(count = jobs.len(), "drained");
            let Some(resume_at) = jobs.iter().find_map(|job| job.resume_at) else {
                return Ok(());
            };
            ctx.context(SeekHandle)
                .seek(KafkaPosition::offset(0, resume_at))
                .await
                .map_err(|_| jobs.iter().map(|_| HandlerOutcome::retry()).collect())
        }
    }

    fn app() -> impl App<State = ()> {
        let broker = KafkaBroker::new(["localhost:9092"]);
        RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
            let bulk = KafkaTopic::new("jobs.bulk")
                .group("jobs-bulk-svc")
                .commit(Commit::Tracked);
            b.include(
                subscriber("jobs.bulk", Drain)
                    .batch(nonzero!(3))
                    .build()
                    .map_source(|_| bulk),
            );
        })
    }
    // --8<-- [end:batch]

    #[tokio::test]
    async fn the_next_batch_starts_at_resume_at() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // Job 0 says the jobs up to offset 4 cannot be processed; all five wait in the queue.
        let jobs: Vec<Job> = (0..5)
            .map(|id| Job {
                id,
                resume_at: (id == 0).then_some(4),
            })
            .collect();
        let broker = tb.broker::<KafkaBroker>();
        for published in join_all(
            jobs.iter()
                .map(|job| broker.message(job).to("jobs.bulk").publish()),
        )
        .await
        {
            published?;
        }

        let batches: Vec<Vec<u64>> = broker
            .subscriber("jobs.bulk")
            .batches::<Job>()
            .iter()
            .map(|batch| batch.iter().map(|job| job.id).collect())
            .collect();
        assert_eq!(batches, [vec![0, 1, 2], vec![4]]);
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}

#[cfg(test)]
#[path = "../support/kafka_subscriptions_live.rs"]
mod live;
