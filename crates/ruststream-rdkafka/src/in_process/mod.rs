//! The in-process transport: a Kafka cluster modelled inside the test process, behind the
//! `testing` feature.
//!
//! [`KafkaBroker`](crate::KafkaBroker) connects to it through
//! [`InProcess`](ruststream::testing::InProcess), and what comes out is the production
//! [`ConnectedKafkaBroker`](crate::ConnectedKafkaBroker) with this cluster in place of
//! librdkafka: the same subscriber, delivery, publishers, seeker and exactly-once pipeline, each
//! with a second arm that reaches here. The cluster has no settings of its own. Every one it
//! applies is read off the librdkafka configuration the production broker and the descriptor
//! build, with librdkafka's own defaults where nothing is set.
//!
//! What it models, and how:
//!
//! - A topic is a set of append-only partition logs. It comes into being with one partition (the
//!   broker's `num.partitions` default) the first time a record is produced to it or a
//!   subscription names it, and every member sees it at once.
//! - A record lands on the partition a publish names, the one its key hashes to (librdkafka's
//!   `consistent_random`), or the next one in turn when it has neither. A partition the topic
//!   does not have, an illegal topic name and a record over `message.max.bytes` are refused.
//! - A consumer group hands each partition to one member (`range` or `roundrobin`, from
//!   `partition.assignment.strategy`), so every group reads each record once. A member joining
//!   or leaving rebalances its group, and a partition changing hands resumes from the group's
//!   committed offset, or from `auto.offset.reset` where it has none.
//! - A delivery stores its offset as `enable.auto.offset.store` says (under `Commit::Tracked` the
//!   ack stores it), and a stored offset is committed where `enable.auto.commit` is on. Committed
//!   offsets belong to the group and outlive its members.
//! - A transaction holds its records pending: a `read_committed` reader (librdkafka's default)
//!   stops in front of them until the commit, and never sees them after an abort. A commit or an
//!   abort writes a marker that takes an offset. A second pairing of a transactional id fences the
//!   first, and a transaction open past `transaction.timeout.ms` is aborted.
//! - Offsets sent to a transaction commit into their group with it.

mod group;
mod log;
mod member;
mod transaction;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use rdkafka::ClientConfig;
use rdkafka::error::{KafkaError as RdKafkaError, RDKafkaErrorCode};
use ruststream::testing::Coordinator;
use ruststream::{HeaderMap, RawMessage, Str};
use tokio::sync::Notify;

pub(crate) use self::group::{Reset, Strategy};
use self::log::{Entry, Partition, Stored, Topic, Visibility};
pub(crate) use self::log::{ProducerId, WireHeader};
pub(crate) use self::member::{InProcessRecord, Member, MemberSpec, Reader};
use self::transaction::Transactional;
use crate::error::KafkaError;
use crate::message::PARTITION_KEY_HEADER;
use crate::seek::KafkaPosition;

/// What the cluster reads off the producer configuration when the broker connects.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ProducerSettings {
    /// `message.max.bytes`: the largest record a producer hands the cluster.
    max_message_bytes: usize,
    /// `transaction.timeout.ms`: how long the cluster lets a transaction stay open.
    transaction_timeout: Duration,
}

impl ProducerSettings {
    /// Reads the settings off `config`, validating it the way librdkafka does when it creates a
    /// client from it, without creating one.
    ///
    /// # Errors
    ///
    /// Returns librdkafka's own error for a property name or value it refuses.
    pub(crate) fn read(config: &ClientConfig) -> Result<Self, RdKafkaError> {
        let native = config.create_native_config()?;
        let number = |key: &str| -> Result<u64, RdKafkaError> {
            let value = native.get(key)?;
            value
                .parse()
                .map_err(|_| RdKafkaError::ClientCreation(format!("{key}={value} is not a number")))
        };
        Ok(Self {
            max_message_bytes: usize::try_from(number("message.max.bytes")?).unwrap_or(usize::MAX),
            transaction_timeout: Duration::from_millis(number("transaction.timeout.ms")?),
        })
    }
}

/// The cluster: every topic, group, member and transaction of one in-process connection.
pub(crate) struct Cluster {
    state: Mutex<State>,
    settings: ProducerSettings,
    coordinator: OnceLock<Coordinator>,
    /// Handed to the timers that abort a transaction left open too long, which must not keep the
    /// cluster alive.
    this: Weak<Self>,
}

impl fmt::Debug for Cluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cluster").finish_non_exhaustive()
    }
}

#[derive(Default)]
struct State {
    topics: BTreeMap<String, Topic>,
    /// Committed offsets per group, the next offset to read per partition. A group outlives its
    /// members.
    committed: HashMap<String, HashMap<(String, i32), i64>>,
    members: BTreeMap<u64, MemberState>,
    next_member: u64,
    transactions: HashMap<String, Transactional>,
    next_sequence: u64,
    /// Deliveries handed to a subscriber and not yet dropped.
    deliveries: usize,
    /// What the harness has been told is outstanding: every record a member is still to be
    /// handed, every delivery still alive, and every record of an open transaction.
    counted: usize,
}

/// One member's place in the cluster.
struct MemberState {
    spec: MemberSpec,
    /// The partitions this member reads, with the offset it reads next.
    assignment: BTreeMap<(String, i32), i64>,
    /// Where the partitions it reads were last handed out from, for a rotation that does not
    /// starve one partition behind another.
    cursor: usize,
    /// A position named before the member held any partition, applied to its first assignment.
    held_start: Option<KafkaPosition>,
    /// What the member's stream reports next instead of a record.
    failure: Option<Failure>,
    wake: Arc<Notify>,
}

/// A failure a member's stream reports, kept as data until the stream takes it.
#[derive(Debug, Clone)]
enum Failure {
    /// The member shares no assignment strategy with its group.
    InconsistentProtocol,
    /// A partition has no committed offset and `auto.offset.reset` is `error`.
    NoOffset,
    /// The start position could not be applied to the assignment.
    Start(String),
}

impl Failure {
    fn into_error(self) -> KafkaError {
        match self {
            Self::InconsistentProtocol => KafkaError::consume(RdKafkaError::MessageConsumption(
                RDKafkaErrorCode::InconsistentGroupProtocol,
            )),
            Self::NoOffset => KafkaError::consume(RdKafkaError::MessageConsumption(
                RDKafkaErrorCode::AutoOffsetReset,
            )),
            Self::Start(message) => KafkaError::InvalidOptions(message),
        }
    }
}

/// What a mutation changed in the harness's count, applied once the lock is released.
#[must_use]
struct Changes {
    enqueued: usize,
    consumed: usize,
    wake: Vec<Arc<Notify>>,
}

impl Cluster {
    pub(crate) fn new(settings: ProducerSettings) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            state: Mutex::new(State::default()),
            settings,
            coordinator: OnceLock::new(),
            this: this.clone(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .expect("in-process cluster mutex poisoned")
    }

    /// Installs the harness coordinator. Idempotent: a second install is ignored.
    pub(crate) fn install(&self, coordinator: Coordinator) {
        if self.coordinator.set(coordinator).is_ok() {
            let changes = self.lock().reconcile();
            self.apply(changes);
        }
    }

    /// Tells the harness what a mutation changed and wakes the members it concerns.
    fn apply(&self, changes: Changes) {
        if let Some(coordinator) = self.coordinator.get() {
            for _ in 0..changes.enqueued {
                coordinator.enqueued();
            }
            for _ in 0..changes.consumed {
                coordinator.consumed();
            }
        }
        for wake in changes.wake {
            wake.notify_one();
        }
    }

    /// Appends a record, as a producer's publish does.
    ///
    /// `transaction` names the open transaction the record joins, or `None` for a plain publish.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Publish`] for what the cluster refuses: an illegal topic name, a
    /// record over `message.max.bytes`, a partition the topic does not have, and a transactional
    /// publish from a fenced producer or with no transaction open.
    pub(crate) fn produce(
        &self,
        topic: &str,
        partition: Option<i32>,
        payload: &[u8],
        headers: &HeaderMap,
        transaction: Option<&ProducerId>,
    ) -> Result<(), KafkaError> {
        if !log::legal_topic(topic) {
            return Err(produce_error(RDKafkaErrorCode::InvalidTopic));
        }
        let key = headers
            .get(PARTITION_KEY_HEADER)
            .map(Bytes::copy_from_slice);
        let wire = log::wire_headers(headers);
        let size = payload.len()
            + key.as_ref().map_or(0, Bytes::len)
            + wire
                .iter()
                .map(|(name, value)| name.len() + value.as_ref().map_or(0, Bytes::len))
                .sum::<usize>();
        if size > self.settings.max_message_bytes {
            return Err(produce_error(RDKafkaErrorCode::MessageSizeTooLarge));
        }
        let mut state = self.lock();
        if let Some(producer) = transaction {
            state.expire(self.settings.transaction_timeout);
            state.check_epoch(producer)?;
            if state.open_transaction(producer).is_none() {
                return Err(KafkaError::Publish(
                    format!(
                        "Local: Erroneous state: producer {} has no transaction open",
                        producer.transactional_id
                    )
                    .into(),
                ));
            }
        }
        state.create_topic(topic);
        let placed = state
            .topics
            .get_mut(topic)
            .and_then(|log| log.place(partition, key.as_deref()));
        let Some(index) = placed else {
            return Err(produce_error(RDKafkaErrorCode::UnknownPartition));
        };
        let sequence = state.next_sequence;
        state.next_sequence += 1;
        let stored = Arc::new(Stored {
            sequence,
            timestamp: now_millis(),
            payload: Bytes::copy_from_slice(payload),
            key,
            headers: wire,
        });
        let visibility = transaction.map_or(Visibility::Committed, |producer| {
            Visibility::Pending(producer.clone())
        });
        state
            .topics
            .get_mut(topic)
            .expect("the topic was just created")
            .partitions[index]
            .entries
            .push(Entry::Data(stored, visibility));
        if let Some(producer) = transaction
            && let Some(open) = state.open_transaction(producer)
        {
            open.partitions.insert((topic.to_owned(), index));
        }
        let changes = state.reconcile();
        drop(state);
        self.apply(changes);
        Ok(())
    }

    /// Every record visible on `topic`, in the order it was produced: what a `read_committed`
    /// reader of the whole topic reads, keyed the way a delivery reports its headers.
    pub(crate) fn published(&self, topic: &str) -> Vec<RawMessage> {
        let mut records: Vec<Arc<Stored>> = {
            let state = self.lock();
            state
                .topics
                .get(topic)
                .map(|log| {
                    log.partitions
                        .iter()
                        .flat_map(|partition| partition.entries.iter())
                        .filter_map(|entry| match entry {
                            Entry::Data(stored, Visibility::Committed) => Some(Arc::clone(stored)),
                            _ => None,
                        })
                        .collect()
                })
                .unwrap_or_default()
        };
        records.sort_by_key(|stored| stored.sequence);
        records
            .into_iter()
            .map(|stored| {
                RawMessage::new(topic.to_owned(), stored.payload.clone())
                    .with_headers(member::headers_of(&stored))
            })
            .collect()
    }

    /// Opens a member for a subscription: it joins its group, which rebalances, or takes the
    /// partitions it names.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Subscribe`] for a topic name or a pattern the cluster refuses.
    pub(crate) fn join(self: &Arc<Self>, spec: MemberSpec) -> Result<Arc<Member>, KafkaError> {
        spec.validate()?;
        let wake = Arc::new(Notify::new());
        let mut state = self.lock();
        let id = state.next_member;
        state.next_member += 1;
        if let Reader::Subscribed { topics, .. } = &spec.reader {
            for topic in topics {
                state.create_topic(topic);
            }
        }
        if let Reader::Assigned { topic, .. } = &spec.reader {
            state.create_topic(topic);
        }
        let group = spec.group.clone();
        let subscribed = matches!(spec.reader, Reader::Subscribed { .. });
        let failure = match &group {
            Some(group) if subscribed && !state.agrees_on_protocol(group, &spec) => {
                Some(Failure::InconsistentProtocol)
            }
            _ => None,
        };
        state.members.insert(
            id,
            MemberState {
                spec,
                assignment: BTreeMap::new(),
                cursor: 0,
                held_start: None,
                failure,
                wake: Arc::clone(&wake),
            },
        );
        match group {
            Some(group) if subscribed => state.rebalance(&group),
            _ => state.assign_named(id),
        }
        let changes = state.reconcile();
        drop(state);
        self.apply(changes);
        Ok(Arc::new(Member::new(Arc::clone(self), id, wake)))
    }

    /// Takes the member out of its group, which rebalances.
    fn leave(&self, id: u64) {
        let mut state = self.lock();
        let Some(member) = state.members.remove(&id) else {
            return;
        };
        if let (Some(group), Reader::Subscribed { .. }) = (&member.spec.group, &member.spec.reader)
        {
            state.rebalance(group);
        }
        let changes = state.reconcile();
        drop(state);
        self.apply(changes);
    }

    /// The next record for the member, or what its stream reports instead; `None` when there is
    /// nothing to hand out yet.
    fn fetch(&self, member: &Arc<Member>) -> Option<Result<InProcessRecord, KafkaError>> {
        let mut state = self.lock();
        state.expire(self.settings.transaction_timeout);
        let fetched = state.fetch(member);
        let changes = state.reconcile();
        drop(state);
        self.apply(changes);
        fetched
    }

    /// A delivery was dropped, settled or not.
    fn delivery_done(&self) {
        let mut state = self.lock();
        state.deliveries = state.deliveries.saturating_sub(1);
        let changes = state.reconcile();
        drop(state);
        self.apply(changes);
    }

    /// Moves the member's partitions to `to`, as a seek on the live consumer does.
    fn seek(&self, id: u64, to: &KafkaPosition) -> Result<(), KafkaError> {
        let mut state = self.lock();
        let result = state.seek(id, to);
        let changes = state.reconcile();
        drop(state);
        self.apply(changes);
        result
    }

    /// Moves one partition the member reads to `offset`, without touching its bookkeeping: the
    /// exactly-once pipeline's seek-back after an aborted window.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Consume`] when the member does not read that partition.
    fn seek_partition(
        &self,
        id: u64,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), KafkaError> {
        let mut state = self.lock();
        let result = state
            .members
            .get_mut(&id)
            .and_then(|member| member.assignment.get_mut(&(topic.to_owned(), partition)))
            .map(|position| *position = offset)
            .ok_or_else(|| {
                KafkaError::consume(RdKafkaError::Seek(format!(
                    "{topic}[{partition}] is not assigned to this consumer"
                )))
            });
        let changes = state.reconcile();
        drop(state);
        self.apply(changes);
        result
    }

    /// Stores `position` as the member's processed position on a partition, committing it into
    /// the group where auto-commit is on.
    fn store(&self, id: u64, topic: &str, partition: i32, position: i64) {
        let mut state = self.lock();
        state.store(id, topic, partition, position);
    }

    /// The group a member reads in, which its offsets commit into.
    fn group_of(&self, id: u64) -> Option<String> {
        self.lock()
            .members
            .get(&id)
            .and_then(|member| member.spec.group.clone())
    }
}

impl State {
    /// Creates `topic` with the default partition count, when it does not exist yet, and
    /// rebalances every group a member of which it is new to.
    fn create_topic(&mut self, topic: &str) {
        if self.topics.contains_key(topic) || !log::legal_topic(topic) {
            return;
        }
        self.topics
            .insert(topic.to_owned(), Topic::new(log::DEFAULT_PARTITIONS));
        let groups: BTreeSet<String> = self
            .members
            .values()
            .filter(|member| member.spec.reads(topic))
            .filter_map(|member| member.spec.group.clone())
            .collect();
        for group in groups {
            self.rebalance(&group);
        }
    }

    fn partition(&self, topic: &str, partition: i32) -> Option<&Partition> {
        self.topics
            .get(topic)
            .and_then(|log| log.partitions.get(usize::try_from(partition).ok()?))
    }

    /// Resolves `to` against `partitions` and moves each partition it names, resetting the
    /// bookkeeping first, as the live seeker does.
    fn apply_position(
        &mut self,
        id: u64,
        partitions: &[(String, i32)],
        to: &KafkaPosition,
    ) -> Result<(), KafkaError> {
        let mut targets = Vec::new();
        for key @ (topic, partition) in partitions {
            let Some(log) = self.partition(topic, *partition) else {
                continue;
            };
            let offset = match to {
                KafkaPosition::Earliest => Some(0),
                KafkaPosition::Latest => Some(log.end()),
                KafkaPosition::Timestamp(when) => Some(log.offset_for_time(*when)),
                KafkaPosition::Offset {
                    topic: named,
                    partition: wanted,
                    offset,
                } => (named.as_ref().is_none_or(|name| name == topic) && wanted == partition)
                    .then_some(*offset),
            };
            if let Some(offset) = offset {
                targets.push((key.clone(), offset, log.end()));
            }
        }
        if targets.is_empty() {
            return Err(KafkaError::InvalidOptions(format!(
                "{to:?} names no partition assigned to this consumer; a seek moves the partitions \
                 this instance holds, and its assignment is {}",
                describe(partitions),
            )));
        }
        let member = self.members.get_mut(&id).expect("the member exists");
        for ((topic, partition), offset, end) in targets {
            member
                .spec
                .tracker
                .reposition(&Str::from(topic.as_str()), partition);
            // Past either end of the log the fetch is out of range, and `auto.offset.reset`
            // decides where the partition resumes.
            let resolved = if (0..=end).contains(&offset) {
                Some(offset)
            } else {
                member.spec.reset.offset(end)
            };
            match resolved {
                Some(offset) => {
                    member.assignment.insert((topic, partition), offset);
                }
                None => {
                    member.failure.get_or_insert(Failure::NoOffset);
                }
            }
        }
        Ok(())
    }

    fn seek(&mut self, id: u64, to: &KafkaPosition) -> Result<(), KafkaError> {
        let Some(member) = self.members.get_mut(&id) else {
            return Err(KafkaError::Closed {
                topic: String::new(),
            });
        };
        if member.assignment.is_empty() {
            // A member holding nothing yet keeps the position for its first assignment, as the
            // live consumer does before the group has assigned it anything.
            member.held_start = Some(to.clone());
            return Ok(());
        }
        let partitions: Vec<_> = member.assignment.keys().cloned().collect();
        self.apply_position(id, &partitions, to)
    }

    fn store(&mut self, id: u64, topic: &str, partition: i32, position: i64) {
        let Some(member) = self.members.get(&id) else {
            return;
        };
        if !member.spec.auto_commit {
            return;
        }
        if let Some(group) = member.spec.group.clone() {
            self.committed
                .entry(group)
                .or_default()
                .insert((topic.to_owned(), partition), position + 1);
        }
    }

    fn fetch(&mut self, handle: &Arc<Member>) -> Option<Result<InProcessRecord, KafkaError>> {
        let id = handle.id();
        let member = self.members.get_mut(&id)?;
        if let Some(failure) = member.failure.take() {
            return Some(Err(failure.into_error()));
        }
        let keys: Vec<(String, i32)> = member.assignment.keys().cloned().collect();
        if keys.is_empty() {
            return None;
        }
        let isolation = member.spec.isolation;
        let start = member.cursor % keys.len();
        for step in 0..keys.len() {
            let key = &keys[(start + step) % keys.len()];
            let position = self.members[&id].assignment[key];
            let Some(log) = self.partition(&key.0, key.1) else {
                continue;
            };
            let Some((offset, stored)) = isolation.next(&log.entries, position) else {
                continue;
            };
            let stored = Arc::clone(stored);
            let member = self.members.get_mut(&id).expect("the member exists");
            member.assignment.insert(key.clone(), offset + 1);
            member.cursor = start + step + 1;
            let auto_store = member.spec.auto_store;
            if auto_store {
                self.store(id, &key.0, key.1, offset);
            }
            self.deliveries += 1;
            return Some(Ok(InProcessRecord::new(
                Arc::clone(handle),
                key.0.clone(),
                key.1,
                offset,
                stored,
            )));
        }
        None
    }

    /// Counts what is outstanding and returns the difference from what the harness was told,
    /// with the members to wake.
    fn reconcile(&mut self) -> Changes {
        let mut backlog = 0;
        let mut wake = Vec::new();
        for member in self.members.values() {
            let mut waiting = member.failure.is_some();
            for ((topic, partition), position) in &member.assignment {
                if let Some(log) = self.partition(topic, *partition) {
                    let pending = member.spec.isolation.backlog(&log.entries, *position);
                    backlog += pending;
                    waiting |= pending > 0;
                }
            }
            if waiting {
                wake.push(Arc::clone(&member.wake));
            }
        }
        // A record written into an open transaction is part of the reaction that wrote it until
        // the transaction commits or aborts: an exactly-once reply is only delivered at its
        // window's commit, and the harness waits for that.
        let pending: usize = self
            .topics
            .values()
            .flat_map(|topic| topic.partitions.iter())
            .flat_map(|partition| partition.entries.iter())
            .filter(|entry| matches!(entry, Entry::Data(_, Visibility::Pending(_))))
            .count();
        let total = backlog + self.deliveries + pending;
        let changes = Changes {
            enqueued: total.saturating_sub(self.counted),
            consumed: self.counted.saturating_sub(total),
            wake,
        };
        self.counted = total;
        changes
    }
}

/// A partition index as the partition number it is.
fn partition_number(index: usize) -> i32 {
    i32::try_from(index).unwrap_or(i32::MAX)
}

fn describe(partitions: &[(String, i32)]) -> String {
    if partitions.is_empty() {
        return "empty".to_owned();
    }
    partitions
        .iter()
        .map(|(topic, partition)| format!("{topic}[{partition}]"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn produce_error(code: RDKafkaErrorCode) -> KafkaError {
    KafkaError::publish(RdKafkaError::MessageProduction(code))
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests;
