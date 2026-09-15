//! The in-memory transport core: exact topic-name fanout over a retained publish log.
//!
//! The log is what gives the transport a position vocabulary: every message keeps its zero-based
//! index in its topic's log, that index is the offset a delivery reports, and a reposition
//! re-enqueues the suffix from a chosen index on (see [`super::seek`]).

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ruststream::testing::Coordinator;
use ruststream::{HeaderMap, RawMessage};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SubscriptionId(u64);

/// The consumer group a subscription joined.
///
/// A subscription that names no group is [`Alone`](Self::Alone) in one of its own: nothing else
/// can be assigned its topic away from it, which is what an anonymous consumer is. That is a
/// variant rather than an `Option` beside a name, so "names no group" cannot be read as "shares
/// one nameless group with every other anonymous subscriber".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum GroupId {
    Named(String),
    Alone(SubscriptionId),
}

/// One in-flight test delivery.
#[derive(Debug, Clone)]
pub(crate) struct TestDelivery {
    /// The topic this delivery came from; a subscription may read several.
    pub(crate) topic: String,
    /// The message's zero-based index in its topic's log, which is the offset it reports.
    pub(crate) seq: usize,
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
    /// The read position this delivery was queued under. A reposition bumps the subscription's
    /// generation, so anything queued before it arrives stale and is dropped unhandled.
    pub(crate) generation: u64,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<TestDelivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<TestDelivery>;

#[derive(Debug)]
struct Subscription {
    topic: String,
    /// Which group this entry competes in for `topic`.
    group: GroupId,
    sender: DeliverySender,
    /// The subscription's read-position generation, shared with its seeker. Read under the
    /// router lock so a publish racing a reposition either lands in the replay's snapshot or is
    /// stamped with the generation the reposition installed - never dropped as stale and left
    /// out of the replay.
    generation: Arc<AtomicU64>,
}

/// The retained publish log: every topic's messages in publish order, indexed by their offset.
pub(crate) type PublishLog = HashMap<String, Vec<RawMessage>>;

#[derive(Debug, Default)]
struct RouterState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    log: PublishLog,
}

/// Routes published messages to subscribers by exact topic name, one member per consumer group.
///
/// There are no real partitions here by design (every topic has exactly one, numbered zero), and
/// that single partition is what group membership divides: Kafka assigns a partition to exactly
/// one member of each group, so a record reaches one subscription per group and every group gets
/// its own copy.
#[derive(Default)]
pub(crate) struct KeyRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
}

impl KeyRouter {
    /// Registers one subscription entry per topic, all feeding one delivery channel and sharing
    /// one read-position generation, so a multi-topic subscriber consumes them as a single
    /// stream and one reposition covers all of them.
    pub(crate) fn subscribe_many(
        &self,
        topics: &[String],
        group: Option<&str>,
        generation: &Arc<AtomicU64>,
    ) -> (Vec<SubscriptionId>, DeliverySender, DeliveryReceiver) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut state = self.state.lock().expect("test router mutex poisoned");
        let ids = topics
            .iter()
            .map(|topic| {
                let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
                state.subscriptions.insert(
                    id,
                    Subscription {
                        topic: topic.clone(),
                        group: group
                            .map_or(GroupId::Alone(id), |name| GroupId::Named(name.to_owned())),
                        sender: sender.clone(),
                        generation: Arc::clone(generation),
                    },
                );
                id
            })
            .collect();
        (ids, sender, receiver)
    }

    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        let mut state = self.state.lock().expect("test router mutex poisoned");
        state.subscriptions.remove(&id);
    }

    /// Appends `payload` to `topic`'s log and hands it to one subscription per consumer group,
    /// synchronously. Every successful enqueue is reported to the coordinator so the harness's
    /// in-flight accounting stays balanced.
    ///
    /// The log append and each subscription's generation stamp happen under one lock, which is
    /// what keeps a publish racing a reposition from falling between the two: it is either in
    /// the replay's snapshot, or stamped with the generation the replay installed.
    pub(crate) fn publish(
        &self,
        topic: &str,
        payload: &Bytes,
        headers: &HeaderMap,
        coordinator: Option<&Coordinator>,
    ) {
        let outgoing: Vec<(DeliverySender, TestDelivery)> = {
            let mut state = self.state.lock().expect("test router mutex poisoned");
            let seq = {
                let entries = state.log.entry(topic.to_owned()).or_default();
                let seq = entries.len();
                entries.push(RawMessage::new(topic, payload.clone()).with_headers(headers.clone()));
                seq
            };
            Self::owners(&state.subscriptions, topic)
                .map(|subscription| {
                    (
                        subscription.sender.clone(),
                        TestDelivery {
                            topic: topic.to_owned(),
                            seq,
                            payload: payload.clone(),
                            headers: headers.clone(),
                            generation: subscription.generation.load(Ordering::Acquire),
                        },
                    )
                })
                .collect()
        };
        for (sender, delivery) in outgoing {
            if sender.send(delivery).is_ok()
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
            }
        }
    }

    /// The one subscription per group that `topic` is assigned to.
    ///
    /// Kafka hands a partition to exactly one member of each group, and this transport gives every
    /// topic a single partition, so a group's records all land on one of its members rather than
    /// spreading over them. The owner is the lowest live subscription id in the group, which makes
    /// the choice deterministic and hands the topic over to the next member when the owner drops -
    /// the effect a rebalance has.
    fn owners<'a>(
        subscriptions: &'a HashMap<SubscriptionId, Subscription>,
        topic: &str,
    ) -> impl Iterator<Item = &'a Subscription> {
        let mut assigned: HashMap<&'a GroupId, (SubscriptionId, &'a Subscription)> = HashMap::new();
        for (id, subscription) in subscriptions
            .iter()
            .filter(|(_, subscription)| subscription.topic == topic)
        {
            assigned
                .entry(&subscription.group)
                .and_modify(|owner| {
                    if *id < owner.0 {
                        *owner = (*id, subscription);
                    }
                })
                .or_insert((*id, subscription));
        }
        assigned.into_values().map(|(_, subscription)| subscription)
    }

    /// Runs `f` over the retained log under the router lock, so a reposition can resolve its
    /// target, bump its generation and snapshot the suffix as one step.
    pub(crate) fn with_log<R>(&self, f: impl FnOnce(&PublishLog) -> R) -> R {
        let state = self.state.lock().expect("test router mutex poisoned");
        f(&state.log)
    }

    pub(crate) fn published(&self, topic: &str) -> Vec<RawMessage> {
        let state = self.state.lock().expect("test router mutex poisoned");
        state.log.get(topic).cloned().unwrap_or_default()
    }

    pub(crate) fn clear(&self) {
        let mut state = self.state.lock().expect("test router mutex poisoned");
        state.subscriptions.clear();
        state.log.clear();
    }
}

impl fmt::Debug for KeyRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyRouter").finish_non_exhaustive()
    }
}
