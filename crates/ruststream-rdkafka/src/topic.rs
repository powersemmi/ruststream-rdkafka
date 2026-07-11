//! The subscription descriptor: one topic consumed through one consumer group.

use ruststream::SubscriptionSource;

use crate::broker::KafkaBroker;
use crate::error::KafkaError;
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

/// How processed deliveries are committed back to the consumer group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
}

/// A subscription to one Kafka topic through one consumer group.
///
/// Everything except the topic name is optional; unset options fall back to the librdkafka
/// defaults (this crate does not impose its own). The group can also come from
/// [`KafkaBroker::default_group`]; a subscription that ends up with no group at all is a
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
    /// [`KafkaBroker::default_group`].
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
        Ok(())
    }

    pub(crate) fn group_or<'a>(&'a self, fallback: Option<&'a str>) -> Option<&'a str> {
        self.group.as_deref().or(fallback)
    }

    pub(crate) fn start_offset(&self) -> StartOffset {
        self.start
    }

    pub(crate) fn commit_mode(&self) -> Commit {
        self.commit
    }

    pub(crate) fn assignment_strategy(&self) -> Option<Assignment> {
        self.assignment
    }

    pub(crate) fn config_entries(&self) -> &[(String, String)] {
        &self.config
    }
}

impl SubscriptionSource<KafkaBroker> for KafkaTopic {
    type Subscriber = KafkaSubscriber;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(self, broker: &KafkaBroker) -> Result<Self::Subscriber, KafkaError> {
        broker.subscribe(self).await
    }
}

#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::KafkaTestBroker> for KafkaTopic {
    type Subscriber = crate::testing::KafkaTestSubscriber;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        broker: &crate::testing::KafkaTestBroker,
    ) -> Result<Self::Subscriber, KafkaError> {
        broker.subscribe_topics(&self.topics).await
    }
}
