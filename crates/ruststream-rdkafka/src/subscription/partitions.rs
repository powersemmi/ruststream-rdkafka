//! Named partitions of one topic, assigned without joining a consumer group.

use std::future::{Future, ready};

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{NamedCopies, SubscriptionSource};

use super::{Commit, GroupSettings, LaneKey, Reader, StartOffset, SubscriptionPlan};
use crate::broker::ConnectedKafkaBroker;
use crate::error::KafkaError;
use crate::subscriber::KafkaSubscriber;

/// A subscription that takes exactly the partitions it names, without joining a consumer group
/// and without rebalancing.
///
/// That fits a reader pinned to a partition, an inspection or replay tool, and a
/// one-consumer-per-partition deployment. A group stays optional and changes only how offsets
/// are handled: with one named the consumer commits into the group without joining it, and
/// without one commits are off, so the start offset has to be explicit.
///
/// This form reads a subset of a topic, and the partitioner places a published record wherever
/// the key sends it, so no publish addresses it: a registration that retries names the
/// destination itself with `.out_retry(policy).to("orders.retry")`. Reading the whole topic is
/// [`KafkaTopic`](crate::KafkaTopic), which addresses its own copies.
///
/// # Examples
///
/// ```
/// use ruststream_rdkafka::{KafkaPartitions, StartOffset};
///
/// // An inspection reader pinned to partition 0, no group side effects.
/// let reader = KafkaPartitions::new("orders", [0]).start(StartOffset::Earliest);
/// assert_eq!(reader.topic(), "orders");
/// ```
#[derive(Debug, Clone)]
pub struct KafkaPartitions {
    topic: String,
    partitions: Vec<i32>,
    settings: GroupSettings,
}

impl KafkaPartitions {
    /// Describes a reader over exactly `partitions` of `topic`.
    #[must_use]
    pub fn new(topic: impl Into<String>, partitions: impl IntoIterator<Item = i32>) -> Self {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
            settings: GroupSettings::default(),
        }
    }

    /// The consumer group whose committed offsets this reader uses, overriding
    /// [`KafkaBroker::default_group`](crate::KafkaBroker::default_group). The reader commits
    /// into the group without ever joining it.
    #[must_use]
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.settings.group = Some(group.into());
        self
    }

    /// Where the reader starts (see [`StartOffset`]). Without a group,
    /// [`StartOffset::Committed`] has nothing to resume from and is a startup error.
    #[must_use]
    pub fn start(mut self, start: StartOffset) -> Self {
        self.settings.start = start;
        self
    }

    /// How processed deliveries are committed (see [`Commit`]).
    /// [`Commit::Transactional`] is a startup error here: an exactly-once pipeline commits
    /// through the consumer-group protocol this form does not take part in.
    #[must_use]
    pub fn commit(mut self, commit: Commit) -> Self {
        self.settings.commit = commit;
        self
    }

    /// What drives keyed worker lanes for this reader (see [`LaneKey`]). Under the default each
    /// assigned partition gets a lane of its own.
    #[must_use]
    pub fn lane_key(mut self, lane_key: LaneKey) -> Self {
        self.settings.lane_key = lane_key;
        self
    }

    /// Raw librdkafka consumer property passthrough, applied last (it wins over the typed
    /// options and the broker-wide config).
    #[must_use]
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.config.push((key.into(), value.into()));
        self
    }

    /// The topic these partitions belong to, which is also the handler metadata name.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The partitions this reader takes.
    #[must_use]
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    /// What this reader adds to its channel in the generated `AsyncAPI` document: the Kafka
    /// topic its partitions belong to.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        crate::bindings::channel(&self.topic)
    }

    /// What this reader adds to its `receive` operation: the group it commits into, and the
    /// client id where the raw passthrough names one.
    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self) -> Bindings {
        crate::bindings::operation(self.settings.group.as_deref(), &self.settings.config)
    }

    fn into_plan(self) -> Result<SubscriptionPlan, KafkaError> {
        super::reject_empty(&self.topic)?;
        super::reject_pattern(&self.topic, "`KafkaPartitions`")?;
        if self.partitions.is_empty() {
            return Err(KafkaError::InvalidOptions(format!(
                "manual assignment of {:?} names no partition, so the reader would take \
                 nothing; name the partitions it reads",
                self.topic,
            )));
        }
        Ok(SubscriptionPlan {
            name: self.topic.clone(),
            reader: Reader::Assigned {
                topic: self.topic,
                partitions: self.partitions,
            },
            settings: self.settings,
        })
    }
}

impl SubscriptionSource<ConnectedKafkaBroker> for KafkaPartitions {
    type Subscriber = KafkaSubscriber;
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        &self.topic
    }

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

#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::ConnectedKafkaTestBroker> for KafkaPartitions {
    type Subscriber = crate::testing::KafkaTestSubscriber;
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        &self.topic
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reader_plans_the_partitions_it_names() {
        let plan = KafkaPartitions::new("orders", [0, 2])
            .start(StartOffset::Earliest)
            .into_plan()
            .expect("two partitions of one topic are a valid assignment");
        assert_eq!(plan.name, "orders");
        let Reader::Assigned { topic, partitions } = plan.reader else {
            panic!("a manual assignment must plan an assigned reader");
        };
        assert_eq!(topic, "orders");
        assert_eq!(partitions, [0, 2]);
    }

    #[test]
    fn naming_no_partition_is_refused() {
        let err = KafkaPartitions::new("orders", [])
            .into_plan()
            .expect_err("a reader over no partition reads nothing");
        assert!(matches!(err, KafkaError::InvalidOptions(_)));
    }
}
