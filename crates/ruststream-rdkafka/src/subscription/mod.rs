//! The subscription descriptors and the group settings they share.
//!
//! Kafka reads a topic in three shapes, and each is a type of its own because they differ in
//! what a deferred retry copy can be published to: [`KafkaTopic`] reads one topic and a publish
//! to that topic reaches it again, while [`KafkaTopics`] (several topics, or a regex) and
//! [`KafkaPartitions`] (named partitions of one topic) read a set no single publish addresses.

mod partitions;
mod topic;
mod topics;

pub use partitions::KafkaPartitions;
pub use topic::KafkaTopic;
pub use topics::KafkaTopics;

use crate::error::KafkaError;

/// Where a consumer group starts reading when it has no valid committed offset.
///
/// Kafka resumes from the group's committed position when a valid one exists; this choice (it
/// maps to librdkafka's `auto.offset.reset`) applies when there is none - the group has never
/// committed the partition, or the committed offset was deleted by retention / is out of
/// range. The second case is why it matters for long-idle groups: with the librdkafka default
/// (latest) an expired group skips to the end instead of reprocessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum StartOffset {
    /// Leave the choice to librdkafka (its default resets to the latest offset).
    #[default]
    Committed,
    /// Start from the earliest retained offset.
    Earliest,
    /// Start from the latest offset (only messages published after the group formed).
    Latest,
}

/// The partition assignment strategy for the consumer group (librdkafka's
/// `partition.assignment.strategy`).
///
/// These are librdkafka's built-in strategies; the client offers no API for a custom group
/// assignor (the rebalance callback only observes assignments). Cooperative and eager
/// strategies cannot mix within one group - librdkafka rejects the join, and the error
/// surfaces on the subscriber stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Assignment {
    /// Co-partitioned ranges per topic (the Kafka default family).
    Range,
    /// Round-robin across all subscribed topics.
    RoundRobin,
    /// Incremental cooperative rebalancing: unaffected partitions keep flowing during a
    /// rebalance instead of stopping the world.
    CooperativeSticky,
}

impl Assignment {
    pub(crate) fn as_config_value(self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::RoundRobin => "roundrobin",
            Self::CooperativeSticky => "cooperative-sticky",
        }
    }
}

/// What drives keyed worker lanes (`workers(n, by_key)`) for this subscription.
///
/// The runtime lanes deliveries by [`IncomingMessage::partition_key`]
/// (deliveries sharing a lane key process in order on one lane); this choice picks what that
/// key is for Kafka.
///
/// [`IncomingMessage::partition_key`]: ruststream::IncomingMessage::partition_key
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum LaneKey {
    /// The source partition (the default): lanes mirror Kafka's own ordering unit, so
    /// everything a partition delivers (keyless included) processes in order on one lane.
    #[default]
    Partition,
    /// The native record key: per-key ordering, finer than a partition, so messages of one
    /// partition may process concurrently when their keys differ. Keyless deliveries carry no
    /// lane key and rotate across lanes, losing their partition order.
    RecordKey,
}

/// How processed deliveries are committed back to the consumer group.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Commit {
    /// librdkafka auto-commit, the librdkafka default: positions are stored as messages are
    /// handed to the application and committed every `auto.commit.interval.ms`. `ack` and
    /// `nack` are advisory no-ops; a crash can lose the tail of processed-but-uncommitted work
    /// or skip unprocessed deliveries that were already stored.
    #[default]
    Auto,
    /// Per-message acknowledgement: `enable.auto.offset.store` is switched off and an `ack`
    /// advances the stored position to just below the lowest still-unsettled delivery (or to
    /// the highest delivered offset once none are outstanding). At-least-once stays precise
    /// with concurrent handler lanes, and offset gaps the consumer never receives (transaction
    /// markers, compacted-away records) cannot block the position. Auto-commit still flushes
    /// the stored position in the background and once more when the consumer closes.
    Tracked,
    /// Exactly-once: the consumer never commits its own offsets - the
    /// [`EosPipeline`](crate::EosPipeline) whose transactional id matches this name commits
    /// them through the producer transaction (`send_offsets_to_transaction`), so source
    /// positions move atomically with the records the handlers publish. `enable.auto.commit`
    /// and `enable.auto.offset.store` are switched off; `ack` advances the shared watermark
    /// exactly like [`Tracked`](Self::Tracked), and the pipeline picks the watermark up at its
    /// next window commit.
    Transactional(String),
}

/// The consumer-group settings every subscription form carries.
///
/// One struct rather than six repeated fields: the three descriptors differ in what they read,
/// not in how the group behaves around it.
#[derive(Debug, Clone, Default)]
pub(crate) struct GroupSettings {
    pub(crate) group: Option<String>,
    pub(crate) start: StartOffset,
    pub(crate) commit: Commit,
    pub(crate) assignment: Option<Assignment>,
    pub(crate) lane_key: LaneKey,
    pub(crate) config: Vec<(String, String)>,
}

/// How the consumer takes its partitions: through the group protocol, or by naming them.
#[derive(Debug)]
pub(crate) enum Reader {
    /// librdkafka `subscribe()` to these names; an entry starting with `^` is a topic regex.
    Subscribed(Vec<String>),
    /// librdkafka `assign()` of exactly these partitions of one topic: no group membership,
    /// no rebalancing.
    Assigned { topic: String, partitions: Vec<i32> },
}

/// A descriptor resolved into what opening a consumer needs, with nothing left to validate.
#[derive(Debug)]
pub(crate) struct SubscriptionPlan {
    /// The handler-metadata name, and the channel the generated document reports.
    pub(crate) name: String,
    pub(crate) reader: Reader,
    pub(crate) settings: GroupSettings,
}

/// Rejects a name that librdkafka would read as a topic regex.
///
/// The check is at startup because the name is a runtime string: a mount site may take it from
/// configuration, so no type can carry the anchor.
pub(crate) fn reject_pattern(name: &str, form: &str) -> Result<(), KafkaError> {
    if name.starts_with('^') {
        return Err(KafkaError::InvalidOptions(format!(
            "{form} reads one topic, and {name:?} is a librdkafka topic regex (the leading '^' \
             is its anchor); subscribe to a pattern with `KafkaTopics::pattern`"
        )));
    }
    Ok(())
}

/// Rejects an empty topic name, whichever form named it.
pub(crate) fn reject_empty(name: &str) -> Result<(), KafkaError> {
    if name.is_empty() {
        return Err(KafkaError::InvalidOptions(
            "topic name must not be empty; name the topic the handler consumes".to_owned(),
        ));
    }
    Ok(())
}
