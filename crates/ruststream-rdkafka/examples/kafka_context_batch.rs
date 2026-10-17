//! The context of a batch handler: the subscription's seek handle, shared by every record of the
//! batch, while each record's own coordinates travel with the elements.
//!
//! ```text
//! cargo test --example kafka_context_batch --features testing
//! cargo run --example kafka_context_batch -- run
//! ```

use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use ruststream_rdkafka::context::KafkaBatchContext;
use ruststream_rdkafka::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
    /// Set by the producer when a downstream system lost the results of earlier jobs: the
    /// partition and offset the run to redo starts at.
    redo_from: Option<(i32, i64)>,
}

/// The jobs that already asked for a redo, so the replayed run does not ask again.
#[derive(Debug, Default)]
struct Redos {
    asked: Mutex<HashSet<u64>>,
}

impl Redos {
    fn first_time(&self, job: &Job) -> bool {
        self.asked
            .lock()
            .is_ok_and(|mut asked| asked.insert(job.id))
    }
}

/// What the handler shares across batches; each field is injectable with `State<T>`.
#[derive(Clone, Default, FromRef)]
struct AppState {
    redos: Arc<Redos>,
}

// --8<-- [start:batch]
/// The batch settles first; the seek then opens the next batch at the offset a job named, so
/// the run from there is processed again.
#[subscriber(KafkaTopic::new("jobs").group("jobs-svc").commit(Commit::Tracked))]
async fn drain(
    jobs: &[Job],
    ctx: &mut Context<'_, KafkaBatchContext>,
    State(redos): State<Arc<Redos>>,
) -> HandlerOutcome {
    tracing::info!(jobs = jobs.len(), "draining jobs");
    let target = jobs
        .iter()
        .filter(|job| redos.first_time(job))
        .find_map(|job| job.redo_from);
    if let Some((partition, offset)) = target
        && ctx
            .context(SeekHandle)
            .seek(KafkaPosition::offset(partition, offset))
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:batch]

#[ruststream::app]
fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("jobs", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(AppState::default()))
        .with_broker(KafkaBroker::new(["localhost:9092"]), |b| {
            b.include(drain.batch(nonzero!(128usize)));
        })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rdkafka::prelude::*;

    use super::{Job, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_job_naming_an_offset_has_the_run_from_there_redone() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // Offsets 0, 1 and 2: the last job asks for the run from offset 1 again.
        for (id, redo_from) in [(0, None), (1, None), (2, Some((0, 1)))] {
            tb.broker::<KafkaBroker>()
                .message(&Job { id, redo_from })
                .publish()
                .await?;
        }

        let drained: Vec<u64> = tb
            .broker::<KafkaBroker>()
            .subscriber("jobs")
            .batches::<Job>()
            .into_iter()
            .flatten()
            .map(|job| job.id)
            .collect();
        assert_eq!(drained, [0, 1, 2, 1, 2], "jobs 1 and 2 ran again, once");
        tb.shutdown().await?;
        Ok(())
    }
}
