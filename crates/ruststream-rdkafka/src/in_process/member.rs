//! A subscription's member of the in-process cluster, and the record it hands a delivery.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use rdkafka::ClientConfig;
use rdkafka::error::KafkaError as RdKafkaError;
use regex::Regex;
use ruststream::{HeaderMap, Str};
use tokio::sync::Notify;

use super::log::{Isolation, Stored, WireHeader};
use super::{Cluster, Reset, Strategy};
use crate::error::KafkaError;
use crate::message::PARTITION_KEY_HEADER;
use crate::seek::KafkaPosition;
use crate::subscription::StartOffset;
use crate::tracker::CommitTracker;

/// What a member reads.
#[derive(Debug, Clone)]
pub(crate) enum Reader {
    /// Through its group: the topics it names and every topic a pattern of it matches.
    Subscribed {
        topics: Vec<String>,
        patterns: Vec<Regex>,
    },
    /// Exactly these partitions of one topic, with no group membership.
    Assigned { topic: String, partitions: Vec<i32> },
}

/// Everything the cluster needs to know about a subscription, read off the consumer
/// configuration the production broker built for it.
#[derive(Debug, Clone)]
pub(crate) struct MemberSpec {
    /// The subscription's name, for the errors that name it.
    pub(crate) name: String,
    pub(crate) group: Option<String>,
    pub(crate) reader: Reader,
    /// The descriptor's start, which a member naming its partitions resumes from.
    pub(crate) start: StartOffset,
    pub(crate) isolation: Isolation,
    pub(crate) reset: Reset,
    /// `enable.auto.offset.store`: a delivery stores its own offset as it is handed over.
    pub(crate) auto_store: bool,
    /// `enable.auto.commit`: a stored offset is committed into the group.
    pub(crate) auto_commit: bool,
    pub(crate) strategies: Vec<Strategy>,
    pub(crate) tracker: Arc<CommitTracker>,
}

impl MemberSpec {
    /// Reads a member's settings off the consumer configuration, validating it the way
    /// librdkafka does when it creates a consumer from it.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Subscribe`] for a property name or value librdkafka refuses, and for
    /// a pattern that does not compile.
    pub(crate) fn read(
        name: String,
        group: Option<String>,
        names: Option<&[String]>,
        assigned: Option<(String, Vec<i32>)>,
        start: StartOffset,
        config: &ClientConfig,
        tracker: Arc<CommitTracker>,
    ) -> Result<Self, KafkaError> {
        let native = config
            .create_native_config()
            .map_err(KafkaError::subscribe)?;
        let value = |key: &str| native.get(key).map_err(KafkaError::subscribe);
        let reader = match (names, assigned) {
            (_, Some((topic, partitions))) => Reader::Assigned { topic, partitions },
            (names, None) => {
                let mut topics = Vec::new();
                let mut patterns = Vec::new();
                for entry in names.unwrap_or_default() {
                    if entry.starts_with('^') {
                        patterns.push(Regex::new(entry).map_err(|err| {
                            KafkaError::subscribe(RdKafkaError::Subscription(format!(
                                "invalid topic regex {entry:?}: {err}"
                            )))
                        })?);
                    } else {
                        topics.push(entry.clone());
                    }
                }
                Reader::Subscribed { topics, patterns }
            }
        };
        let strategies = Strategy::parse(&value("partition.assignment.strategy")?);
        Ok(Self {
            name,
            group,
            reader,
            start,
            isolation: if value("isolation.level")? == "read_uncommitted" {
                Isolation::ReadUncommitted
            } else {
                Isolation::ReadCommitted
            },
            // A topic-level property: librdkafka answers it off the global configuration only
            // once something set it, and its default is the end of the log.
            reset: native
                .get("auto.offset.reset")
                .map_or(Reset::Latest, |value| Reset::parse(&value)),
            auto_store: value("enable.auto.offset.store")? == "true",
            auto_commit: value("enable.auto.commit")? == "true",
            strategies: if strategies.is_empty() {
                vec![Strategy::Range]
            } else {
                strategies
            },
            tracker,
        })
    }

    /// Refuses a topic name the cluster would refuse.
    pub(crate) fn validate(&self) -> Result<(), KafkaError> {
        let topics: Vec<&String> = match &self.reader {
            Reader::Subscribed { topics, .. } => topics.iter().collect(),
            Reader::Assigned { topic, .. } => vec![topic],
        };
        for topic in topics {
            if !super::log::legal_topic(topic) {
                return Err(KafkaError::subscribe(RdKafkaError::Subscription(format!(
                    "{topic:?} is not a legal Kafka topic name"
                ))));
            }
        }
        Ok(())
    }

    /// Whether this member reads `topic`.
    pub(crate) fn reads(&self, topic: &str) -> bool {
        match &self.reader {
            Reader::Subscribed { topics, patterns } => {
                topics.iter().any(|name| name == topic)
                    || patterns.iter().any(|pattern| pattern.is_match(topic))
            }
            Reader::Assigned { topic: named, .. } => named == topic,
        }
    }
}

/// A subscription's handle on its member: what its stream fetches through, what its deliveries
/// store their offsets through, and what its seeker moves. The member leaves its group when the
/// last handle goes, as a live consumer closes when the last reference to it drops.
pub(crate) struct Member {
    cluster: Arc<Cluster>,
    id: u64,
    wake: Arc<Notify>,
}

impl fmt::Debug for Member {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Member")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Member {
    pub(super) fn new(cluster: Arc<Cluster>, id: u64, wake: Arc<Notify>) -> Self {
        Self { cluster, id, wake }
    }

    pub(super) fn id(&self) -> u64 {
        self.id
    }

    /// The next record, or what the stream reports instead; `None` when there is nothing yet.
    pub(crate) fn fetch(self: &Arc<Self>) -> Option<Result<InProcessRecord, KafkaError>> {
        self.cluster.fetch(self)
    }

    /// The next record, waiting for one.
    ///
    /// # Cancel safety
    ///
    /// Cancel safe: a record is taken only when the call resolves.
    pub(crate) async fn next(self: &Arc<Self>) -> Result<InProcessRecord, KafkaError> {
        loop {
            // The wake is a stored permit, so one given between the fetch and the wait is not
            // lost.
            let woken = self.wake.notified();
            if let Some(fetched) = self.fetch() {
                return fetched;
            }
            woken.await;
        }
    }

    /// Repositions the partitions this member reads (see
    /// [`KafkaSeeker`](crate::KafkaSeeker)).
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::InvalidOptions`] when the position names no partition this member
    /// reads.
    pub(crate) fn seek(&self, to: &KafkaPosition) -> Result<(), KafkaError> {
        self.cluster.seek(self.id, to)
    }

    /// Moves one partition to `offset` (the exactly-once pipeline's seek-back).
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Consume`] when this member does not read the partition.
    pub(crate) fn seek_partition(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), KafkaError> {
        self.cluster
            .seek_partition(self.id, topic, partition, offset)
    }

    /// The group this member commits into.
    pub(crate) fn group(&self) -> Option<String> {
        self.cluster.group_of(self.id)
    }
}

impl Drop for Member {
    fn drop(&mut self) {
        self.cluster.leave(self.id);
    }
}

/// A record the in-process cluster handed a delivery: its coordinates, the record itself, and the
/// member its offset is stored through.
pub(crate) struct InProcessRecord {
    member: Arc<Member>,
    topic: String,
    partition: i32,
    offset: i64,
    stored: Arc<Stored>,
}

impl fmt::Debug for InProcessRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InProcessRecord")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

impl InProcessRecord {
    pub(super) fn new(
        member: Arc<Member>,
        topic: String,
        partition: i32,
        offset: i64,
        stored: Arc<Stored>,
    ) -> Self {
        Self {
            member,
            topic,
            partition,
            offset,
            stored,
        }
    }

    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) fn partition(&self) -> i32 {
        self.partition
    }

    pub(crate) fn offset(&self) -> i64 {
        self.offset
    }

    pub(crate) fn timestamp_millis(&self) -> i64 {
        self.stored.timestamp
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.stored.payload
    }

    pub(crate) fn key(&self) -> Option<&[u8]> {
        self.stored.key.as_deref()
    }

    pub(crate) fn headers(&self) -> &[WireHeader] {
        &self.stored.headers
    }

    /// Stores `position` as processed, the in-process counterpart of the live consumer's offset
    /// store.
    pub(crate) fn store(&self, topic: &str, partition: i32, position: i64) {
        self.member
            .cluster
            .store(self.member.id, topic, partition, position);
    }
}

impl Drop for InProcessRecord {
    fn drop(&mut self) {
        // Settled or not, the delivery is over: the harness's count closes here once.
        self.member.cluster.delivery_done();
    }
}

/// A stored record's headers the way a delivery reports them: every wire header, a null value as
/// an empty one, and the record key under [`PARTITION_KEY_HEADER`].
pub(super) fn headers_of(stored: &Stored) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in &stored.headers {
        if name.eq_ignore_ascii_case(PARTITION_KEY_HEADER) {
            continue;
        }
        headers.insert(name.as_str(), value.clone().unwrap_or_else(Bytes::new));
    }
    if let Some(key) = &stored.key {
        headers.insert(Str::from_static(PARTITION_KEY_HEADER), key.clone());
    }
    headers
}
