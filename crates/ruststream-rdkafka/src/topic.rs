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
/// use ruststream_rdkafka::{Commit, KafkaTopic, StartOffset};
///
/// let topic = KafkaTopic::new("orders")
///     .group("orders-svc")
///     .start(StartOffset::Earliest)
///     .commit(Commit::Tracked)
///     .config("fetch.min.bytes", "1024");
/// assert_eq!(topic.topic(), "orders");
/// ```
#[derive(Debug, Clone)]
pub struct KafkaTopic {
    topic: String,
    group: Option<String>,
    start: StartOffset,
    commit: Commit,
    config: Vec<(String, String)>,
}

impl KafkaTopic {
    /// Describes a subscription to `topic` with librdkafka defaults for everything else.
    #[must_use]
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            group: None,
            start: StartOffset::default(),
            commit: Commit::default(),
            config: Vec::new(),
        }
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

    /// Raw librdkafka consumer property passthrough for anything not surfaced as a typed
    /// option, applied last (it wins over the typed options and the broker-wide config).
    #[must_use]
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.push((key.into(), value.into()));
        self
    }

    /// The topic this subscription consumes.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
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

    pub(crate) fn config_entries(&self) -> &[(String, String)] {
        &self.config
    }
}

impl SubscriptionSource<KafkaBroker> for KafkaTopic {
    type Subscriber = KafkaSubscriber;

    fn name(&self) -> &str {
        &self.topic
    }

    async fn subscribe(self, broker: &KafkaBroker) -> Result<Self::Subscriber, KafkaError> {
        broker.subscribe(self).await
    }
}

#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::KafkaTestBroker> for KafkaTopic {
    type Subscriber = crate::testing::KafkaTestSubscriber;

    fn name(&self) -> &str {
        &self.topic
    }

    async fn subscribe(
        self,
        broker: &crate::testing::KafkaTestBroker,
    ) -> Result<Self::Subscriber, KafkaError> {
        broker.subscribe(&self.topic).await
    }
}
