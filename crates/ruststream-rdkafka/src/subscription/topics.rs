//! Several topics, or a topic regex, consumed as one subscription.

use std::future::{Future, ready};

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{NamedCopies, SubscriptionSource};

use super::{Assignment, Commit, GroupSettings, LaneKey, Reader, StartOffset, SubscriptionPlan};
use crate::broker::ConnectedKafkaBroker;
use crate::error::KafkaError;
use crate::subscriber::KafkaSubscriber;

/// One entry of the subscribed set: librdkafka reads a leading `^` as "this is a regex", so
/// which of the two a name is belongs to the name, not to a flag beside the list.
#[derive(Debug, Clone)]
enum Subscribed {
    Topic(String),
    Pattern(String),
}

impl Subscribed {
    fn as_str(&self) -> &str {
        match self {
            Self::Topic(name) | Self::Pattern(name) => name,
        }
    }

    fn validate(&self) -> Result<(), KafkaError> {
        super::reject_empty(self.as_str())?;
        match self {
            Self::Topic(name) => super::reject_pattern(name, "a topic of `KafkaTopics`"),
            Self::Pattern(pattern) if pattern.starts_with('^') => Ok(()),
            Self::Pattern(pattern) => Err(KafkaError::InvalidOptions(format!(
                "pattern {pattern:?} must start with '^' (librdkafka's anchor for topic \
                 regexes); without it the name would be subscribed literally"
            ))),
        }
    }
}

/// A subscription to several topics, or to every topic matching a regex, through one consumer
/// and one consumer group.
///
/// All matched topics share the handler, and therefore its payload type; each delivery still
/// reports the topic it came from. Topics created after the group formed are picked up on the
/// next metadata refresh of a pattern subscription.
///
/// One such subscription reads many destinations and a copy published to any one of them would
/// come back on the wrong topic, so this form leaves the retry destination to the mount site:
/// `.out_retry(policy).to("orders.retry")`, or a transform that names it per delivery. One
/// topic on its own is [`KafkaTopic`](crate::KafkaTopic), which addresses its own copies.
///
/// # Examples
///
/// ```
/// use ruststream_rdkafka::KafkaTopics;
///
/// let both = KafkaTopics::new(["orders", "cancellations"]).group("orders-svc");
/// assert_eq!(both.topics(), "orders,cancellations");
/// ```
#[derive(Debug, Clone)]
pub struct KafkaTopics {
    entries: Vec<Subscribed>,
    /// The subscribed names joined with `,`: the handler metadata name, kept as an owned string
    /// because the trait hands it out as a borrow.
    name: String,
    settings: GroupSettings,
}

impl KafkaTopics {
    fn from_entries(entries: Vec<Subscribed>) -> Self {
        let name = entries
            .iter()
            .map(Subscribed::as_str)
            .collect::<Vec<_>>()
            .join(",");
        Self {
            entries,
            name,
            settings: GroupSettings::default(),
        }
    }

    fn push(mut self, entry: Subscribed) -> Self {
        if !self.name.is_empty() {
            self.name.push(',');
        }
        self.name.push_str(entry.as_str());
        self.entries.push(entry);
        self
    }

    /// Describes a subscription to every one of `topics`, by literal name.
    #[must_use]
    pub fn new(topics: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::from_entries(
            topics
                .into_iter()
                .map(|topic| Subscribed::Topic(topic.into()))
                .collect(),
        )
    }

    /// Describes a subscription to every existing topic matching `pattern`.
    ///
    /// The pattern is a librdkafka topic regex and must start with `^` (that anchor is how
    /// librdkafka distinguishes a pattern from a literal name); subscribing fails with a clear
    /// error otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rdkafka::KafkaTopics;
    ///
    /// let audit = KafkaTopics::pattern("^audit\\..*").group("audit-svc");
    /// assert_eq!(audit.topics(), "^audit\\..*");
    /// ```
    #[must_use]
    pub fn pattern(pattern: impl Into<String>) -> Self {
        Self::from_entries(vec![Subscribed::Pattern(pattern.into())])
    }

    /// Adds another literal topic to the same subscription.
    #[must_use]
    pub fn and_topic(self, topic: impl Into<String>) -> Self {
        self.push(Subscribed::Topic(topic.into()))
    }

    /// Adds another `^`-anchored topic regex to the same subscription.
    #[must_use]
    pub fn and_pattern(self, pattern: impl Into<String>) -> Self {
        self.push(Subscribed::Pattern(pattern.into()))
    }

    /// The consumer group for this subscription, overriding
    /// [`KafkaBroker::default_group`](crate::KafkaBroker::default_group).
    #[must_use]
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.settings.group = Some(group.into());
        self
    }

    /// Where the group starts when it has no committed offset (see [`StartOffset`]).
    #[must_use]
    pub fn start(mut self, start: StartOffset) -> Self {
        self.settings.start = start;
        self
    }

    /// How processed deliveries are committed (see [`Commit`]).
    #[must_use]
    pub fn commit(mut self, commit: Commit) -> Self {
        self.settings.commit = commit;
        self
    }

    /// The partition assignment strategy (see [`Assignment`]); unset means the librdkafka
    /// default (`range,roundrobin`).
    #[must_use]
    pub fn assignment(mut self, assignment: Assignment) -> Self {
        self.settings.assignment = Some(assignment);
        self
    }

    /// What drives keyed worker lanes for this subscription (see [`LaneKey`]).
    #[must_use]
    pub fn lane_key(mut self, lane_key: LaneKey) -> Self {
        self.settings.lane_key = lane_key;
        self
    }

    /// Raw librdkafka consumer property passthrough, applied last (it wins over the typed
    /// options and the broker-wide config).
    ///
    /// A property the subscription's [`Commit`] mode owns is the exception; see
    /// [`KafkaTopic::config`](crate::KafkaTopic::config).
    #[must_use]
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.config.push((key.into(), value.into()));
        self
    }

    /// The subscribed names joined with `,`, which is also the handler metadata name.
    #[must_use]
    pub fn topics(&self) -> &str {
        &self.name
    }

    /// What this subscription adds to its `receive` operation: the consumer group it joins, and
    /// the client id where the raw passthrough names one.
    ///
    /// There is no channel binding to go with it: the Kafka binding's `topic` names one topic,
    /// and this subscription reads a set.
    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self) -> Bindings {
        crate::bindings::operation(self.settings.group.as_deref(), &self.settings.config)
    }

    fn into_plan(self) -> Result<SubscriptionPlan, KafkaError> {
        if self.entries.is_empty() {
            return Err(KafkaError::InvalidOptions(
                "`KafkaTopics` subscribes to at least one name; pass the topics the handler \
                 consumes"
                    .to_owned(),
            ));
        }
        for entry in &self.entries {
            entry.validate()?;
        }
        let name = self.name;
        super::reject_commit_mode_clash(&name, &self.settings.commit, &self.settings.config)?;
        let names = self
            .entries
            .into_iter()
            .map(|entry| match entry {
                Subscribed::Topic(name) | Subscribed::Pattern(name) => name,
            })
            .collect();
        Ok(SubscriptionPlan {
            name,
            reader: Reader::Subscribed(names),
            settings: self.settings,
        })
    }
}

impl SubscriptionSource<ConnectedKafkaBroker> for KafkaTopics {
    type Subscriber = KafkaSubscriber;
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        &self.name
    }

    fn subscribe(
        self,
        connected: &ConnectedKafkaBroker,
    ) -> impl Future<Output = Result<Self::Subscriber, KafkaError>> {
        ready(self.into_plan().and_then(|plan| connected.open(plan)))
    }

    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self) -> Bindings {
        Self::operation_bindings(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_list_subscribes_to_every_name() {
        let plan = KafkaTopics::new(["orders", "cancellations"])
            .group("orders-svc")
            .into_plan()
            .expect("two literal topics are a valid subscription");
        assert_eq!(plan.name, "orders,cancellations");
        assert!(
            matches!(plan.reader, Reader::Subscribed(names) if names == ["orders", "cancellations"])
        );
    }

    #[test]
    fn an_unanchored_pattern_is_refused() {
        let err = KafkaTopics::pattern("orders\\..*")
            .into_plan()
            .expect_err("an unanchored pattern would be subscribed literally");
        let KafkaError::InvalidOptions(message) = err else {
            panic!("a refused pattern must be an options error");
        };
        assert!(
            message.contains('^'),
            "the error must name the anchor, got: {message}"
        );
    }

    // A regex entry passed as a literal topic is the mistake the anchor rule exists for: it
    // would subscribe to every matching topic while the mount site believes it named one.
    #[test]
    fn a_regex_passed_as_a_literal_topic_is_refused() {
        let err = KafkaTopics::new(["orders"])
            .and_topic("^audit\\..*")
            .into_plan()
            .expect_err("a regex is not a literal topic name");
        assert!(matches!(err, KafkaError::InvalidOptions(_)));
    }

    #[test]
    fn an_empty_set_is_refused() {
        let err = KafkaTopics::new(Vec::<String>::new())
            .into_plan()
            .expect_err("a subscription to nothing is not a subscription");
        assert!(matches!(err, KafkaError::InvalidOptions(_)));
    }
}
