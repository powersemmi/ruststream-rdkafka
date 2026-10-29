//! One topic consumed through one consumer group: the addressable subscription form.

use std::future::{Future, ready};

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::runtime::IntoSource;
use ruststream::{AddressedCopies, RedeliveryAddress, RedeliveryAddressed, SubscriptionSource};

use super::{Assignment, Commit, GroupSettings, LaneKey, Reader, StartOffset, SubscriptionPlan};
use crate::broker::ConnectedKafkaBroker;
use crate::error::KafkaError;
#[cfg(feature = "schema-registry")]
use crate::schema::{Schema, SubjectAfterSchema};
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
/// use ruststream_rdkafka::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// #[subscriber(
///     KafkaTopic::new("orders")
///         .group("orders-svc")
///         .start(StartOffset::Earliest)
///         .commit(Commit::Tracked)
///         .assignment(Assignment::CooperativeSticky)
///         .config("fetch.min.bytes", "1024")
/// )]
/// async fn place(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(KafkaBroker::new(["localhost:9092"]), |b| {
///             b.include(place);
///         })
/// }
/// # let _ = app;
/// ```
#[derive(Debug, Clone)]
pub struct KafkaTopic {
    topic: String,
    settings: GroupSettings,
    /// What the topic opted into with `.schema(..)`; read once, while the subscription opens.
    #[cfg(feature = "schema-registry")]
    schema: Option<Schema>,
}

impl KafkaTopic {
    /// Describes a subscription to `topic` with librdkafka defaults for everything else.
    #[must_use]
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            settings: GroupSettings::default(),
            #[cfg(feature = "schema-registry")]
            schema: None,
        }
    }

    /// Turns the schema check on for this topic and names the namespace of its records.
    ///
    /// A string is the namespace, with the subjects the topic's name gives under
    /// `TopicNameStrategy`: `<topic>-value` for the payload, `<topic>-key` for the key.
    /// [`Schema::namespace`] names other subjects with `.value(..)` and `.key(..)`.
    ///
    /// When the subscription opens, the broker reads the newest schema of each subject from the
    /// registry it carries ([`KafkaBroker::schema_prefetch`](crate::KafkaBroker::schema_prefetch))
    /// and fails startup when the value subject is missing or declares no record of the
    /// namespace. The key subject is checked the same way when it exists, and must exist when
    /// `.key(..)` names it. A JSON Schema names no record, so a JSON subject is checked for
    /// presence only. The check reads the registry once per topic and adds nothing to a
    /// delivery.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rdkafka::prelude::*;
    /// use ruststream_rdkafka::{SchemaPrefetch, SchemaRegistry};
    /// # #[derive(serde::Deserialize)]
    /// # struct Order { id: i64 }
    ///
    /// #[subscriber(KafkaTopic::new("orders").schema("com.acme.orders"))]
    /// async fn on_order(order: &Order) -> HandlerOutcome {
    ///     tracing::info!(order.id, "accepted");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     let prefetch = SchemaPrefetch::new(SchemaRegistry::new("http://localhost:8081"));
    ///     let broker = KafkaBroker::new(["localhost:9092"])
    ///         .default_group("orders-svc")
    ///         .schema_prefetch(prefetch);
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
    ///         b.include(on_order);
    ///     })
    /// }
    /// # let _ = app;
    /// ```
    #[cfg(feature = "schema-registry")]
    #[must_use]
    pub fn schema(mut self, schema: impl Into<Schema>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    // The subject overrides belong to `Schema`, after the namespace they override. These two
    // exist only to turn the call a reader of the topic's other steps would try into an error
    // that names that fix; their bound has no implementation, so neither can be called.
    #[cfg(feature = "schema-registry")]
    #[doc(hidden)]
    #[must_use]
    pub fn value<Subject: SubjectAfterSchema>(self, subject: Subject) -> Self {
        drop(subject);
        self
    }

    #[cfg(feature = "schema-registry")]
    #[doc(hidden)]
    #[must_use]
    pub fn key<Subject: SubjectAfterSchema>(self, subject: Subject) -> Self {
        drop(subject);
        self
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
    /// Per-record-key lanes: one tenant's orders never run concurrently, different tenants in
    /// one partition do.
    ///
    /// ```
    /// use ruststream_rdkafka::prelude::*;
    /// # #[derive(serde::Deserialize)]
    /// # struct Order { id: u64 }
    ///
    /// #[subscriber(
    ///     KafkaTopic::new("orders")
    ///         .group("orders-svc")
    ///         .commit(Commit::Tracked)
    ///         .lane_key(LaneKey::RecordKey),
    ///     workers(8, by_key)
    /// )]
    /// async fn place(order: &Order) -> HandlerOutcome {
    ///     tracing::info!(order.id, "accepted");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .with_broker(KafkaBroker::new(["localhost:9092"]), |b| {
    ///             b.include(place);
    ///         })
    /// }
    /// # let _ = app;
    /// ```
    #[must_use]
    pub fn lane_key(mut self, lane_key: LaneKey) -> Self {
        self.settings.lane_key = lane_key;
        self
    }

    /// Raw librdkafka consumer property passthrough for anything not surfaced as a typed
    /// option, applied last (it wins over the typed options and the broker-wide config).
    ///
    /// The exception is a property the subscription's [`Commit`] mode owns
    /// (`enable.auto.offset.store`, `enable.auto.commit`) under
    /// [`Commit::Tracked`](Commit::Tracked) or
    /// [`Commit::Transactional`](Commit::Transactional): taking it back would leave the
    /// committed position to librdkafka while every ack decided nothing, so the subscription
    /// refuses to open instead. [`Commit::Auto`](Commit::Auto) owns neither and takes both.
    #[must_use]
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.config.set(key.into(), value.into());
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
        super::reject_commit_mode_clash(&self.topic, &self.settings.commit, &self.settings.config)?;
        Ok(SubscriptionPlan {
            name: self.topic.clone(),
            reader: Reader::Subscribed(vec![self.topic]),
            settings: self.settings,
        })
    }
}

// The manual path binds a `Handle` body to its source with `subscriber(source, body)`, which
// takes the descriptor as it is, the way the `#[subscriber(..)]` attribute does.
impl IntoSource for KafkaTopic {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedKafkaBroker> for KafkaTopic {
    type Subscriber = KafkaSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.topic
    }

    async fn subscribe(
        self,
        connected: &ConnectedKafkaBroker,
    ) -> Result<Self::Subscriber, KafkaError> {
        #[cfg(feature = "schema-registry")]
        let schema = self.schema.clone();
        let plan = self.into_plan()?;
        #[cfg(feature = "schema-registry")]
        if let Some(schema) = &schema {
            schema.validate(&plan.name)?;
            connected
                .check_schemas(&plan.name, [(plan.name.as_str(), schema)])
                .await?;
        }
        connected.open(plan).await
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
            message.contains("KafkaTopicPattern"),
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
