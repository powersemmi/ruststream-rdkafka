//! Repositioning the in-process transport: the retained publish log replayed into one
//! subscription.
//!
//! The router keeps every published message per topic, which is the whole reason a seek can mean
//! anything here: a reposition re-enqueues the log suffix from the target on, so a service that
//! replays or skips runs against `KafkaTestBroker` exactly as it does against a cluster. What the
//! transport does not have, it refuses rather than pretends: it stamps no timestamps and has one
//! partition per topic.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::broker::TestBrokerState;
use super::router::{DeliverySender, PublishLog, TestDelivery};
use crate::error::KafkaError;
use crate::seek::KafkaPosition;

/// The in-process reposition handle behind [`KafkaSeeker`](crate::KafkaSeeker).
///
/// It owns a sender into its subscription's delivery channel, so a replay is applied where it is
/// requested instead of being handed to the polling side: everything queued before the seek is
/// left in the channel and dropped on arrival by its generation, and the replay is appended
/// behind it. That ordering is what keeps the harness's in-flight count from touching zero
/// between the two, which is what lets `TestApp` settle a reaction that repositions.
pub(crate) struct InProcessSeek {
    state: Arc<TestBrokerState>,
    /// The topics this subscription reads, in subscribe order; a reposition covers all of them
    /// unless the position names one.
    topics: Vec<String>,
    sender: DeliverySender,
    /// Bumped by every reposition, and shared with the router so a publish stamps its deliveries
    /// under the same lock the bump happens in. A delivery stamped below the current value was
    /// queued for a read position this subscription no longer has, so the polling side drops it.
    generation: Arc<AtomicU64>,
}

impl std::fmt::Debug for InProcessSeek {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessSeek")
            .field("topics", &self.topics)
            .field("generation", &self.generation.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl InProcessSeek {
    pub(crate) fn new(
        state: &Arc<TestBrokerState>,
        topics: &[String],
        sender: DeliverySender,
        generation: &Arc<AtomicU64>,
    ) -> Self {
        Self {
            state: Arc::clone(state),
            topics: topics.to_vec(),
            sender,
            generation: Arc::clone(generation),
        }
    }

    /// Replays the retained log of the subscribed topics from `to` on.
    ///
    /// Resolution: [`Earliest`](KafkaPosition::Earliest) is log position zero and
    /// [`Latest`](KafkaPosition::Latest) the current end (so only later publishes arrive), both
    /// on every subscribed topic; [`Offset`](KafkaPosition::Offset) names the log position
    /// directly, on the topic it names or on all of them when it names none. The log position is
    /// the offset here, and every topic has exactly one partition, numbered zero.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] for a position this transport cannot resolve: a
    /// timestamp (it stamps none), a partition other than zero, or a topic this subscription
    /// does not read.
    pub(crate) fn replay(&self, to: &KafkaPosition) -> Result<(), KafkaError> {
        // Resolving the target, installing the new generation and snapshotting the suffix happen
        // under the router lock, the same one a publish appends and stamps under; a publish
        // racing this is therefore either inside the snapshot or stamped with the generation it
        // installs, never dropped as stale and left out of the replay.
        let replay = self.state.router.with_log(|log| {
            let targets = self.resolve(log, to)?;
            let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
            let mut deliveries = Vec::new();
            for (topic, from) in targets {
                let entries = log.get(&topic).map_or(&[][..], Vec::as_slice);
                for (seq, message) in entries.iter().enumerate().skip(from) {
                    deliveries.push(TestDelivery {
                        topic: topic.clone(),
                        seq,
                        payload: message.payload_bytes(),
                        headers: message.headers().clone(),
                        generation,
                    });
                }
            }
            Ok::<_, KafkaError>(deliveries)
        })?;

        let coordinator = self.state.coordinator();
        for delivery in replay {
            // The subscription holds both ends of its own channel, so the send cannot fail while
            // this handle exists.
            if self.sender.send(delivery).is_ok()
                && let Some(coordinator) = &coordinator
            {
                // Counted here, while the delivery that asked for the seek is still in flight: a
                // count that started only after it settled would let the harness observe
                // quiescence in the gap and return before the replay was handled.
                coordinator.enqueued();
            }
        }
        Ok(())
    }

    /// The log position each affected topic resumes from.
    fn resolve(
        &self,
        log: &PublishLog,
        to: &KafkaPosition,
    ) -> Result<Vec<(String, usize)>, KafkaError> {
        let end = |topic: &str| log.get(topic).map_or(0, Vec::len);
        match to {
            KafkaPosition::Earliest => {
                Ok(self.topics.iter().map(|topic| (topic.clone(), 0)).collect())
            }
            KafkaPosition::Latest => Ok(self
                .topics
                .iter()
                .map(|topic| (topic.clone(), end(topic)))
                .collect()),
            KafkaPosition::Timestamp(_) => Err(KafkaError::InvalidOptions(
                "the in-process test broker stamps no record timestamps, so a timestamp position \
                 cannot be resolved; seek by offset, or run the scenario against a cluster"
                    .to_owned(),
            )),
            KafkaPosition::Offset {
                topic,
                partition,
                offset,
            } => {
                if *partition != 0 {
                    return Err(KafkaError::InvalidOptions(format!(
                        "the in-process test broker gives every topic one partition, numbered 0; \
                         partition {partition} exists only on a cluster",
                    )));
                }
                let named: Vec<String> = match topic {
                    Some(name) => {
                        if !self.topics.iter().any(|subscribed| subscribed == name) {
                            return Err(KafkaError::InvalidOptions(format!(
                                "{name:?} is not read by this subscription, which reads {:?}",
                                self.topics,
                            )));
                        }
                        vec![name.clone()]
                    }
                    None => self.topics.clone(),
                };
                Ok(named
                    .into_iter()
                    .map(|topic| {
                        // Clamped like a live seek past the end: the subscription parks at the
                        // log end, and only later publishes arrive.
                        let from = usize::try_from(*offset).unwrap_or(0).min(end(&topic));
                        (topic, from)
                    })
                    .collect())
            }
        }
    }
}
