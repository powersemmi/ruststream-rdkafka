//! Several topics, named one by one, consumed as one subscription.

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::runtime::IntoSource;
use ruststream::{NamedCopies, SubscriptionSource};

use super::{Assignment, Commit, GroupSettings, LaneKey, Reader, StartOffset, SubscriptionPlan};
use crate::broker::ConnectedKafkaBroker;
use crate::error::KafkaError;
#[cfg(feature = "schema-registry")]
use crate::schema::{Schema, TopicSchemas};
use crate::subscriber::KafkaSubscriber;

/// A subscription to several topics, named one by one, through one consumer and one consumer
/// group.
///
/// All the topics share the handler, and therefore its payload type; each delivery still reports
/// the topic it came from. Every topic matching a regex is
/// [`KafkaTopicPattern`](crate::KafkaTopicPattern), which [`pattern`](Self::pattern) starts.
///
/// One such subscription reads many destinations and a copy published to any one of them would
/// come back on the wrong topic, so this form leaves the retry destination to the mount site:
/// `.out_retry(policy).to("orders.retry")`, or a transform that names it per delivery. One
/// topic on its own is [`KafkaTopic`](crate::KafkaTopic), which addresses its own copies.
///
/// # Examples
///
/// ```
/// use ruststream_rdkafka::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// #[subscriber(KafkaTopics::new(["orders-eu", "orders-us"]).group("orders-svc"))]
/// async fn place(order: &Order, Ctx(topic): Ctx<Topic>) -> HandlerOutcome {
///     tracing::info!(order.id, %topic, "accepted");
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
pub struct KafkaTopics {
    entries: Vec<String>,
    /// The subscribed names joined with `,`: the handler metadata name, kept as an owned string
    /// because the trait hands it out as a borrow.
    name: String,
    settings: GroupSettings,
    /// The topics that opted in with `.schema(..)`; read once, while the subscription opens.
    #[cfg(feature = "schema-registry")]
    schemas: TopicSchemas,
}

impl KafkaTopics {
    /// Describes a subscription to every one of `topics`, by literal name.
    #[must_use]
    pub fn new(topics: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let entries: Vec<String> = topics.into_iter().map(Into::into).collect();
        Self {
            name: entries.join(","),
            entries,
            settings: GroupSettings::default(),
            #[cfg(feature = "schema-registry")]
            schemas: TopicSchemas::default(),
        }
    }

    /// Describes a subscription to every topic matching `pattern`: a
    /// [`KafkaTopicPattern`](crate::KafkaTopicPattern), the form a regex subscription takes.
    ///
    /// The `#[subscriber(..)]` attribute reads the descriptor's type off the constructor it
    /// starts from, so the attribute names the pattern form directly with
    /// [`KafkaTopicPattern::new`](crate::KafkaTopicPattern::new); this constructor serves code
    /// that builds the descriptor as a value.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::future::{Future, ready};
    ///
    /// use ruststream::schemars::{self, JsonSchema};
    /// use ruststream_rdkafka::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize, JsonSchema)]
    /// struct Event {
    ///     kind: String,
    /// }
    ///
    /// /// Records every audit event, whichever `audit.*` topic carried it.
    /// struct Archive;
    ///
    /// impl Handle<Event> for Archive {
    ///     fn handle(
    ///         &self,
    ///         event: &Event,
    ///         _outs: &(),
    ///         _ctx: &mut Context<'_>,
    ///     ) -> impl Future<Output = Result<(), HandlerOutcome>> {
    ///         tracing::info!(kind = %event.kind, "recorded");
    ///         ready(Ok(()))
    ///     }
    /// }
    ///
    /// fn app() -> impl App {
    ///     let audit = KafkaTopics::pattern("^audit\\..*").group("audit-svc");
    ///     RustStream::new(AppInfo::new("audit", "0.1.0"))
    ///         .with_broker(KafkaBroker::new(["localhost:9092"]), |b| {
    ///             b.include(subscriber(audit, Archive).build());
    ///         })
    /// }
    /// # let _ = app;
    /// ```
    #[must_use]
    pub fn pattern(pattern: impl Into<String>) -> crate::KafkaTopicPattern {
        crate::KafkaTopicPattern::new(pattern)
    }

    /// Adds another literal topic to the same subscription.
    #[must_use]
    pub fn and_topic(mut self, topic: impl Into<String>) -> Self {
        let topic = topic.into();
        if !self.name.is_empty() {
            self.name.push(',');
        }
        self.name.push_str(&topic);
        self.entries.push(topic);
        self
    }

    /// Turns the schema check on for `topic`, one of this list, and names the namespace of its
    /// records.
    ///
    /// The topic is addressed by its name, so the list may change without moving the check to
    /// another topic. A string is the namespace with the topic's default subjects
    /// (`<topic>-value`, `<topic>-key`); [`Schema::namespace`] names other subjects. Each topic
    /// keeps its own subjects: a consumer decodes every record by the schema id it carries,
    /// whichever topic brought it. A name the list does not hold fails startup, naming the
    /// list, and so does a topic that opts in twice. What is checked is described on
    /// [`KafkaTopic::schema`](crate::KafkaTopic::schema).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rdkafka::prelude::*;
    /// use ruststream_rdkafka::schema::Schema;
    /// use ruststream_rdkafka::{SchemaPrefetch, SchemaRegistry};
    /// # #[derive(serde::Deserialize)]
    /// # struct Order { id: i64 }
    ///
    /// #[subscriber(
    ///     KafkaTopics::new(["orders", "returns"])
    ///         .schema("orders", "com.acme.orders")
    ///         .schema("returns", Schema::namespace("com.acme").value("returns-v2"))
    /// )]
    /// async fn on_order(order: &Order, Ctx(topic): Ctx<Topic>) -> HandlerOutcome {
    ///     tracing::info!(order.id, %topic, "accepted");
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
    pub fn schema(mut self, topic: impl Into<String>, schema: impl Into<Schema>) -> Self {
        self.schemas.push(topic.into(), schema.into());
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
        self.settings.config.set(key.into(), value.into());
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
            super::reject_empty(entry)?;
            super::reject_pattern(entry, "a topic of `KafkaTopics`")?;
        }
        let name = self.name;
        super::reject_commit_mode_clash(&name, &self.settings.commit, &self.settings.config)?;
        Ok(SubscriptionPlan {
            name,
            reader: Reader::Subscribed(self.entries),
            settings: self.settings,
        })
    }
}

// The manual path binds a `Handle` body to its source with `subscriber(source, body)`, which
// takes the descriptor as it is, the way the `#[subscriber(..)]` attribute does.
impl IntoSource for KafkaTopics {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedKafkaBroker> for KafkaTopics {
    type Subscriber = KafkaSubscriber;
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedKafkaBroker,
    ) -> Result<Self::Subscriber, KafkaError> {
        #[cfg(feature = "schema-registry")]
        let schemas = self.schemas.clone();
        let plan = self.into_plan()?;
        #[cfg(feature = "schema-registry")]
        if let Reader::Subscribed(topics) = &plan.reader {
            let listed: Vec<&str> = topics.iter().map(String::as_str).collect();
            let opted = schemas.resolve(&listed)?;
            connected.check_schemas(&plan.name, opted).await?;
        }
        connected.open(plan).await
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
