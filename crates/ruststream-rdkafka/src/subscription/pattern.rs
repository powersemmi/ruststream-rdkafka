//! Every topic matching a regex, consumed as one subscription.

#[cfg(feature = "schema-registry")]
use std::sync::Arc;

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::runtime::IntoSource;
use ruststream::{NamedCopies, SubscriptionSource};

use super::{Assignment, Commit, GroupSettings, LaneKey, Reader, StartOffset, SubscriptionPlan};
use crate::broker::ConnectedKafkaBroker;
use crate::error::KafkaError;
#[cfg(feature = "schema-registry")]
use crate::schema::{PatternSchemas, Schema, SchemaWatch};
use crate::subscriber::KafkaSubscriber;

/// A subscription to every topic matching a regex, through one consumer and one consumer group.
///
/// The pattern is a librdkafka topic regex and must start with `^`, the anchor librdkafka reads
/// as "this name is a pattern"; subscribing fails with a clear error otherwise. Topics created
/// after the group formed are picked up on the next metadata refresh. All matched topics share
/// the handler, and therefore its payload type; each delivery reports the topic it came from.
///
/// Like [`KafkaTopics`](crate::KafkaTopics), this form reads many destinations, so the mount
/// site names where a retried copy goes: `.out_retry(policy).to("audit.retry")`, or a transform
/// that names it per delivery.
///
/// # Examples
///
/// ```
/// use ruststream_rdkafka::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Event { kind: String }
///
/// #[subscriber(KafkaTopicPattern::new("^audit\\..*").group("audit-svc"))]
/// async fn archive(event: &Event, Ctx(topic): Ctx<Topic>) -> HandlerOutcome {
///     tracing::info!(kind = %event.kind, %topic, "recorded");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("audit", "0.1.0"))
///         .with_broker(KafkaBroker::new(["localhost:9092"]), |b| {
///             b.include(archive);
///         })
/// }
/// # let _ = app;
/// ```
#[derive(Debug, Clone)]
pub struct KafkaTopicPattern {
    patterns: Vec<String>,
    /// The patterns joined with `,`: the handler metadata name, kept as an owned string because
    /// the trait hands it out as a borrow.
    name: String,
    settings: GroupSettings,
    /// The schema sets, in the order they were written; read while the subscription opens and
    /// when the group assigns a topic, never per delivery.
    #[cfg(feature = "schema-registry")]
    schemas: PatternSchemas,
}

impl KafkaTopicPattern {
    /// Describes a subscription to every topic matching `pattern`.
    #[must_use]
    pub fn new(pattern: impl Into<String>) -> Self {
        let pattern = pattern.into();
        Self {
            name: pattern.clone(),
            patterns: vec![pattern],
            settings: GroupSettings::default(),
            #[cfg(feature = "schema-registry")]
            schemas: PatternSchemas::default(),
        }
    }

    /// Adds another `^`-anchored topic regex to the same subscription.
    #[must_use]
    pub fn and_pattern(mut self, pattern: impl Into<String>) -> Self {
        let pattern = pattern.into();
        self.name.push(',');
        self.name.push_str(&pattern);
        self.patterns.push(pattern);
        self
    }

    /// Turns the schema check on for the matched topics that `pattern` also matches, and names
    /// the namespace of their records.
    ///
    /// The sets narrow in the order they are written: a topic takes the first set whose pattern
    /// it matches, so a broad set may follow specific ones. A topic's default subjects come from
    /// its own name (`orders.eu-value`, `orders.eu-key`); an override of [`Schema::namespace`]
    /// names one subject for every topic of the set. A matched topic that no set takes is
    /// checked against no registry.
    ///
    /// The topics are known only at runtime. The ones the cluster holds when the subscription
    /// opens are checked then, and a mismatch fails startup. A topic created later is checked
    /// when the group first assigns it to this member, and a mismatch is logged. Each check reads
    /// the registry once per topic and adds nothing to a delivery; what it checks is described
    /// on [`KafkaTopic::schema`](crate::KafkaTopic::schema).
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
    ///     KafkaTopicPattern::new(r"^(orders|returns)\..+")
    ///         .schema(r"^orders\.", "com.acme.orders")
    ///         .schema(r"^returns\.", Schema::namespace("com.acme.returns").value("returns-value"))
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
    pub fn schema(mut self, pattern: impl Into<String>, schema: impl Into<Schema>) -> Self {
        self.schemas.push(pattern.into(), schema.into());
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

    /// The patterns joined with `,`, which is also the handler metadata name.
    #[must_use]
    pub fn patterns(&self) -> &str {
        &self.name
    }

    /// What this subscription adds to its `receive` operation: the consumer group it joins, and
    /// the client id where the raw passthrough names one.
    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self) -> Bindings {
        crate::bindings::operation(self.settings.group.as_deref(), &self.settings.config)
    }

    fn into_plan(self) -> Result<SubscriptionPlan, KafkaError> {
        for pattern in &self.patterns {
            super::reject_empty(pattern)?;
            if !pattern.starts_with('^') {
                return Err(KafkaError::InvalidOptions(format!(
                    "pattern {pattern:?} must start with '^' (librdkafka's anchor for topic \
                     regexes); without it the name would be subscribed literally"
                )));
            }
        }
        let name = self.name;
        super::reject_commit_mode_clash(&name, &self.settings.commit, &self.settings.config)?;
        Ok(SubscriptionPlan {
            name,
            reader: Reader::Subscribed(self.patterns),
            settings: self.settings,
        })
    }

    /// Opens the subscription and checks the schema sets against the topics the cluster holds.
    #[cfg(feature = "schema-registry")]
    async fn open_checked(
        mut self,
        connected: &ConnectedKafkaBroker,
    ) -> Result<KafkaSubscriber, KafkaError> {
        let schemas = std::mem::take(&mut self.schemas);
        let plan = self.into_plan()?;
        if schemas.is_empty() {
            return connected.open(plan).await;
        }
        let sets = schemas.compile()?;
        let registry = connected
            .check_registry(&format!("pattern subscription `{}`", plan.name))?
            .clone();
        // The subscription's own patterns, to tell which of the cluster's topics it reads; one
        // this dialect does not compile is librdkafka's to read, and its topics are checked as
        // they are assigned.
        let reads: Vec<regex::Regex> = match &plan.reader {
            Reader::Subscribed(patterns) => patterns
                .iter()
                .filter_map(|pattern| regex::Regex::new(pattern).ok())
                .collect(),
            Reader::Assigned { .. } => Vec::new(),
        };
        let watch = Arc::new(SchemaWatch::new(
            plan.name.clone(),
            sets,
            registry,
            connected.state().runtime().clone(),
        ));
        let subscriber = connected
            .open_watched(plan, Some(Arc::clone(&watch)))
            .await?;
        let known = connected
            .known_topics(&subscriber)
            .await?
            .into_iter()
            .filter(|topic| reads.iter().any(|pattern| pattern.is_match(topic)))
            .collect();
        watch.arm(known).await?;
        Ok(subscriber)
    }
}

// The manual path binds a `Handle` body to its source with `subscriber(source, body)`, which
// takes the descriptor as it is, the way the `#[subscriber(..)]` attribute does.
impl IntoSource for KafkaTopicPattern {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedKafkaBroker> for KafkaTopicPattern {
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
        return self.open_checked(connected).await;
        #[cfg(not(feature = "schema-registry"))]
        connected.open(self.into_plan()?).await
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
    fn a_pattern_subscribes_to_every_regex() {
        let plan = KafkaTopicPattern::new("^orders\\..*")
            .and_pattern("^returns\\..*")
            .into_plan()
            .expect("anchored patterns are a valid subscription");
        assert_eq!(plan.name, "^orders\\..*,^returns\\..*");
        assert!(
            matches!(plan.reader, Reader::Subscribed(names) if names == ["^orders\\..*", "^returns\\..*"])
        );
    }

    #[test]
    fn an_unanchored_pattern_is_refused() {
        let err = KafkaTopicPattern::new("orders\\..*")
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
}
