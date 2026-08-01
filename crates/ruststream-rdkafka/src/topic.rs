//! The subscription descriptor: one topic consumed through one consumer group.

use ruststream::SubscriptionSource;

use crate::broker::ConnectedKafkaBroker;
use crate::error::KafkaError;
use crate::retry::Retry;
use crate::subscriber::KafkaSubscriber;

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

/// A subscription to one Kafka topic through one consumer group.
///
/// Everything except the topic name is optional; unset options fall back to the librdkafka
/// defaults (this crate does not impose its own). The group can also come from
/// [`KafkaBroker::default_group`](crate::KafkaBroker::default_group); a subscription that ends up with no group at all is a
/// startup error, because Kafka cannot subscribe without one.
///
/// # Examples
///
/// ```
/// use ruststream_rdkafka::{Assignment, Commit, KafkaTopic, StartOffset};
///
/// let topic = KafkaTopic::new("orders")
///     .group("orders-svc")
///     .start(StartOffset::Earliest)
///     .commit(Commit::Tracked)
///     .assignment(Assignment::CooperativeSticky)
///     .config("fetch.min.bytes", "1024");
/// assert_eq!(topic.topic(), "orders");
/// ```
#[derive(Debug, Clone)]
pub struct KafkaTopic {
    /// The subscribed names; librdkafka treats entries starting with `^` as regex patterns.
    topics: Vec<String>,
    /// The handler-metadata name: the subscribed names joined with `,`.
    name: String,
    /// Set by [`pattern`](Self::pattern), which promises a `^`-anchored regex.
    requires_pattern: bool,
    group: Option<String>,
    start: StartOffset,
    commit: Commit,
    assignment: Option<Assignment>,
    lane_key: LaneKey,
    partitions: Vec<i32>,
    retry: Option<Retry>,
    max_deliveries: Option<u32>,
    dead_letter: Option<String>,
    config: Vec<(String, String)>,
}

impl KafkaTopic {
    fn with_first(first: String, requires_pattern: bool) -> Self {
        Self {
            name: first.clone(),
            topics: vec![first],
            requires_pattern,
            group: None,
            start: StartOffset::default(),
            commit: Commit::default(),
            assignment: None,
            lane_key: LaneKey::default(),
            partitions: Vec::new(),
            retry: None,
            max_deliveries: None,
            dead_letter: None,
            config: Vec::new(),
        }
    }

    /// Describes a subscription to `topic` with librdkafka defaults for everything else.
    #[must_use]
    pub fn new(topic: impl Into<String>) -> Self {
        Self::with_first(topic.into(), false)
    }

    /// Describes a subscription to every existing topic matching `pattern`.
    ///
    /// The pattern is a librdkafka topic regex and must start with `^` (that anchor is how
    /// librdkafka distinguishes a pattern from a literal name); subscribing fails with a clear
    /// error otherwise. Topics created after the group formed are picked up on the next
    /// metadata refresh.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rdkafka::KafkaTopic;
    ///
    /// let orders = KafkaTopic::pattern("^orders\\..*").group("orders-svc");
    /// assert_eq!(orders.topic(), "^orders\\..*");
    /// ```
    #[must_use]
    pub fn pattern(pattern: impl Into<String>) -> Self {
        Self::with_first(pattern.into(), true)
    }

    /// Adds another topic to the same subscription: one consumer, one group, several topics.
    ///
    /// All matched topics share the handler (and therefore its payload type). Entries starting
    /// with `^` are librdkafka regex patterns, exactly as in [`pattern`](Self::pattern).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rdkafka::KafkaTopic;
    ///
    /// let both = KafkaTopic::new("orders").and_topic("cancellations");
    /// assert_eq!(both.topic(), "orders,cancellations");
    /// ```
    #[must_use]
    pub fn and_topic(mut self, topic: impl Into<String>) -> Self {
        let topic = topic.into();
        self.name.push(',');
        self.name.push_str(&topic);
        self.topics.push(topic);
        self
    }

    /// The consumer group for this subscription, overriding
    /// [`KafkaBroker::default_group`](crate::KafkaBroker::default_group).
    #[must_use]
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Where the group starts when it has no committed offset (see [`StartOffset`]).
    #[must_use]
    pub fn start(mut self, start: StartOffset) -> Self {
        self.start = start;
        self
    }

    /// How processed deliveries are committed (see [`Commit`]).
    #[must_use]
    pub fn commit(mut self, commit: Commit) -> Self {
        self.commit = commit;
        self
    }

    /// The partition assignment strategy (see [`Assignment`]); unset means the librdkafka
    /// default (`range,roundrobin`).
    #[must_use]
    pub fn assignment(mut self, assignment: Assignment) -> Self {
        self.assignment = Some(assignment);
        self
    }

    /// What drives keyed worker lanes for this subscription (see [`LaneKey`]); the default
    /// lanes by the source partition, Kafka's native ordering unit.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rdkafka::{KafkaTopic, LaneKey};
    ///
    /// // Opt into finer, per-record-key lanes: one tenant never processes concurrently,
    /// // different tenants in one partition do.
    /// let topic = KafkaTopic::new("orders")
    ///     .group("orders-svc")
    ///     .lane_key(LaneKey::RecordKey);
    /// # let _ = topic;
    /// ```
    #[must_use]
    pub fn lane_key(mut self, lane_key: LaneKey) -> Self {
        self.lane_key = lane_key;
        self
    }

    /// Switches the subscription to manual partition assignment: the consumer `assign()`s
    /// exactly these partitions of the topic - no group membership, no rebalancing.
    ///
    /// Deliveries start per [`start`](Self::start); with a group also named the consumer
    /// commits into it without joining it (so `StartOffset::Committed` resumes from the
    /// group's positions), and without one commits are off and the start offset must be
    /// explicit. Does not combine with [`and_topic`](Self::and_topic) /
    /// [`pattern`](Self::pattern) (manual assignment names exact partitions of one topic) or
    /// with `Commit::Transactional`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rdkafka::{KafkaTopic, StartOffset};
    ///
    /// // An inspection reader pinned to partition 0, no group side effects.
    /// let topic = KafkaTopic::new("orders")
    ///     .partitions([0])
    ///     .start(StartOffset::Earliest);
    /// # let _ = topic;
    /// ```
    #[must_use]
    pub fn partitions(mut self, partitions: impl IntoIterator<Item = i32>) -> Self {
        self.partitions = partitions.into_iter().collect();
        self
    }

    /// What `nack(true)` does on this subscription (see [`Retry`]); unset keeps Kafka's native
    /// behavior - the offset stays unsettled and redelivers on the next fetch of the partition.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rdkafka::{KafkaTopic, Retry};
    ///
    /// let topic = KafkaTopic::new("orders")
    ///     .group("orders-svc")
    ///     .retry(Retry::Topic("orders.retry".into()))
    ///     .max_deliveries(5)
    ///     .dead_letter("orders.dlq");
    /// # let _ = topic;
    /// ```
    #[must_use]
    pub fn retry(mut self, retry: Retry) -> Self {
        self.retry = Some(retry);
        self
    }

    /// The poison cap: how many times a message may be delivered before `nack(true)` takes the
    /// drop path instead of retrying (the original delivery counts as one). Enforced by the
    /// [`retry`](Self::retry) policy - through [`RETRY_COUNT_HEADER`](crate::RETRY_COUNT_HEADER)
    /// for [`Retry::Topic`], and through an in-session counter for [`Retry::SeekBack`].
    ///
    /// The cap needs a [`retry`](Self::retry) policy or a [`dead_letter`](Self::dead_letter)
    /// topic to count against; on its own it could never apply, so subscribing rejects that
    /// combination with [`KafkaError::InvalidOptions`](crate::KafkaError::InvalidOptions).
    #[must_use]
    pub fn max_deliveries(mut self, max_deliveries: u32) -> Self {
        self.max_deliveries = Some(max_deliveries);
        self
    }

    /// The dead-letter topic for the drop path: `nack(false)` and an exhausted retry republish
    /// the message there (stamped with the `kafka-dlq-source-*` headers), then settle. Without
    /// it the drop path just settles.
    #[must_use]
    pub fn dead_letter(mut self, topic: impl Into<String>) -> Self {
        self.dead_letter = Some(topic.into());
        self
    }

    /// Raw librdkafka consumer property passthrough for anything not surfaced as a typed
    /// option, applied last (it wins over the typed options and the broker-wide config).
    #[must_use]
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.push((key.into(), value.into()));
        self
    }

    /// The subscribed name(s), joined with `,` when there are several (also the handler
    /// metadata name); a pattern subscription returns the pattern.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.name
    }

    pub(crate) fn subscribed_topics(&self) -> &[String] {
        &self.topics
    }

    pub(crate) fn validate(&self) -> Result<(), KafkaError> {
        if self.requires_pattern && !self.topics[0].starts_with('^') {
            return Err(KafkaError::InvalidOptions(format!(
                "pattern {:?} must start with '^' (librdkafka's anchor for topic regexes); \
                 without it the name would be subscribed literally",
                self.topics[0],
            )));
        }
        if !self.partitions.is_empty() && (self.topics.len() > 1 || self.requires_pattern) {
            return Err(KafkaError::InvalidOptions(
                "manual partition assignment names exact partitions of one topic; it does \
                 not combine with `and_topic` or `pattern`"
                    .to_owned(),
            ));
        }
        // Without a retry policy or a dead-letter topic no path ever reads the cap: nack(true)
        // is Kafka's native re-consumption, which this crate cannot count. Accepting the cap
        // would silently disarm a poison-message guard the user believes is in place.
        if self.max_deliveries.is_some() && self.retry.is_none() && self.dead_letter.is_none() {
            return Err(KafkaError::InvalidOptions(
                "max_deliveries needs a retry(..) policy or a dead_letter(..) topic to count \
                 against; alone it would never apply"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn group_or<'a>(&'a self, fallback: Option<&'a str>) -> Option<&'a str> {
        self.group.as_deref().or(fallback)
    }

    pub(crate) fn start_offset(&self) -> StartOffset {
        self.start
    }

    pub(crate) fn commit_mode(&self) -> &Commit {
        &self.commit
    }

    pub(crate) fn assignment_strategy(&self) -> Option<Assignment> {
        self.assignment
    }

    pub(crate) fn lane_key_choice(&self) -> LaneKey {
        self.lane_key
    }

    pub(crate) fn retry_policy(&self) -> Option<&Retry> {
        self.retry.as_ref()
    }

    pub(crate) fn max_deliveries_cap(&self) -> Option<u32> {
        self.max_deliveries
    }

    pub(crate) fn dead_letter_topic(&self) -> Option<&str> {
        self.dead_letter.as_deref()
    }

    pub(crate) fn assigned_partitions(&self) -> &[i32] {
        &self.partitions
    }

    pub(crate) fn config_entries(&self) -> &[(String, String)] {
        &self.config
    }
}

impl SubscriptionSource<ConnectedKafkaBroker> for KafkaTopic {
    type Subscriber = KafkaSubscriber;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedKafkaBroker,
    ) -> Result<Self::Subscriber, KafkaError> {
        connected.subscribe_with(self).await
    }
}

#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::ConnectedKafkaTestBroker> for KafkaTopic {
    type Subscriber = crate::testing::KafkaTestSubscriber;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        broker: &crate::testing::ConnectedKafkaTestBroker,
    ) -> Result<Self::Subscriber, KafkaError> {
        if !self.partitions.is_empty() {
            return Err(KafkaError::InvalidOptions(
                "the in-process test broker does not simulate partitions; manual partition \
                 assignment needs a real cluster"
                    .to_owned(),
            ));
        }
        broker.subscribe_topics(&self.topics).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_deliveries_alone_is_rejected() {
        let err = KafkaTopic::new("orders")
            .max_deliveries(3)
            .validate()
            .expect_err("a cap with nothing to count against must be rejected");
        assert!(matches!(err, KafkaError::InvalidOptions(_)));
    }

    #[test]
    fn max_deliveries_with_retry_or_dead_letter_passes() {
        KafkaTopic::new("orders")
            .retry(Retry::SeekBack)
            .max_deliveries(3)
            .validate()
            .expect("a cap with a retry policy is valid");
        KafkaTopic::new("orders")
            .dead_letter("orders.dlq")
            .max_deliveries(3)
            .validate()
            .expect("a cap with a dead-letter topic is valid");
    }
}
