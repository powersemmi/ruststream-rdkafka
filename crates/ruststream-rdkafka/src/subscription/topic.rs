//! One topic consumed through one consumer group: the addressable subscription form.

use std::future::{Future, ready};

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{AddressedCopies, RedeliveryAddress, RedeliveryAddressed, SubscriptionSource};

use super::{Assignment, Commit, GroupSettings, LaneKey, Reader, StartOffset, SubscriptionPlan};
use crate::broker::ConnectedKafkaBroker;
use crate::error::KafkaError;
use crate::subscriber::KafkaSubscriber;

/// A subscription to one Kafka topic through one consumer group.
///
/// Everything except the topic name is optional; unset options fall back to the librdkafka
/// defaults (this crate does not impose its own). The group can also come from
/// [`KafkaBroker::default_group`](crate::KafkaBroker::default_group); a subscription that ends
/// up with no group at all is a startup error, because Kafka cannot subscribe without one.
///
/// This is the form a deferred `retry_after` copy can be published back to: one topic, and a
/// publish to it reaches every group reading it. Several topics at once take
/// [`KafkaTopics`](crate::KafkaTopics) and named partitions take
/// [`KafkaPartitions`](crate::KafkaPartitions); neither can be reached by a single publish, so
/// the mount site names where their copies go.
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
    topic: String,
    settings: GroupSettings,
}

impl KafkaTopic {
    /// Describes a subscription to `topic` with librdkafka defaults for everything else.
    #[must_use]
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            settings: GroupSettings::default(),
        }
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
        self.settings.lane_key = lane_key;
        self
    }

    /// Raw librdkafka consumer property passthrough for anything not surfaced as a typed
    /// option, applied last (it wins over the typed options and the broker-wide config).
    #[must_use]
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.config.push((key.into(), value.into()));
        self
    }

    /// The subscribed topic, which is also the handler metadata name.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// What this subscription adds to its channel in the generated `AsyncAPI` document: the
    /// Kafka topic behind it.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        crate::bindings::channel(&self.topic)
    }

    /// What this subscription adds to its `receive` operation: the consumer group it joins, and
    /// the client id where the raw passthrough names one.
    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self) -> Bindings {
        crate::bindings::operation(self.settings.group.as_deref(), &self.settings.config)
    }

    fn into_plan(self) -> Result<SubscriptionPlan, KafkaError> {
        super::reject_empty(&self.topic)?;
        super::reject_pattern(&self.topic, "`KafkaTopic`")?;
        Ok(SubscriptionPlan {
            name: self.topic.clone(),
            reader: Reader::Subscribed(vec![self.topic]),
            settings: self.settings,
        })
    }
}

impl SubscriptionSource<ConnectedKafkaBroker> for KafkaTopic {
    type Subscriber = KafkaSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.topic
    }

    // librdkafka joins the group in the background, so opening a subscription never awaits; the
    // trait is what shapes the signature.
    fn subscribe(
        self,
        connected: &ConnectedKafkaBroker,
    ) -> impl Future<Output = Result<Self::Subscriber, KafkaError>> {
        ready(self.into_plan().and_then(|plan| connected.open(plan)))
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        Self::channel_bindings(self)
    }

    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self) -> Bindings {
        Self::operation_bindings(self)
    }
}

impl RedeliveryAddressed<ConnectedKafkaBroker> for KafkaTopic {
    /// The topic itself: a record produced to it reaches every group reading it, this
    /// subscription included.
    ///
    /// Answered from the descriptor alone, so nothing is awaited.
    fn redelivery_address(
        &self,
        _connected: &ConnectedKafkaBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, KafkaError>> {
        ready(Ok(RedeliveryAddress::new(self.topic.clone())))
    }
}

#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::ConnectedKafkaTestBroker> for KafkaTopic {
    type Subscriber = crate::testing::KafkaTestSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.topic
    }

    // Returns a future without awaiting: opening an in-process subscription is synchronous, and
    // the trait is what shapes the signature.
    fn subscribe(
        self,
        broker: &crate::testing::ConnectedKafkaTestBroker,
    ) -> impl Future<Output = Result<Self::Subscriber, KafkaError>> {
        ready(self.into_plan().and_then(|plan| broker.open(plan)))
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        Self::channel_bindings(self)
    }

    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self) -> Bindings {
        Self::operation_bindings(self)
    }
}

#[cfg(feature = "testing")]
impl RedeliveryAddressed<crate::testing::ConnectedKafkaTestBroker> for KafkaTopic {
    fn redelivery_address(
        &self,
        _broker: &crate::testing::ConnectedKafkaTestBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, KafkaError>> {
        ready(Ok(RedeliveryAddress::new(self.topic.clone())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_subscription_plans_one_subscribed_topic() {
        let plan = KafkaTopic::new("orders")
            .group("orders-svc")
            .into_plan()
            .expect("a literal topic name is a valid single-topic subscription");
        assert_eq!(plan.name, "orders");
        assert!(matches!(plan.reader, Reader::Subscribed(names) if names == ["orders"]));
    }

    // The single-topic form promises a publish address, and a regex is not one, so it refuses
    // the name instead of reporting an address no record would arrive at.
    #[test]
    fn a_pattern_name_is_refused_and_points_at_the_pattern_form() {
        let err = KafkaTopic::new("^orders\\..*")
            .into_plan()
            .expect_err("a topic regex is not a single topic");
        let KafkaError::InvalidOptions(message) = err else {
            panic!("a refused name must be an options error");
        };
        assert!(
            message.contains("KafkaTopics::pattern"),
            "the error must name the form that does subscribe by pattern, got: {message}"
        );
    }

    #[test]
    fn an_empty_topic_name_is_refused() {
        let err = KafkaTopic::new("")
            .into_plan()
            .expect_err("an empty name subscribes to nothing");
        assert!(matches!(err, KafkaError::InvalidOptions(_)));
    }
}
