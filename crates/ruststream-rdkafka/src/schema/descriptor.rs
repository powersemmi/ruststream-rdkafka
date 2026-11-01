//! What a topic opts into with `.schema(..)`: the namespace of its records and the subjects they
//! are registered under.

use std::fmt;

use crate::error::KafkaError;
use crate::schema_registry::SubjectStrategy;

/// The registry subjects a topic is checked against: the namespace of its records, and the value
/// and key subjects when they differ from the topic's own name.
///
/// A string converts into it as the namespace with default subjects, which is what most topics
/// need. The subjects default to Confluent's `TopicNameStrategy`: `<topic>-value` for the payload
/// and `<topic>-key` for the key. [`value`](Self::value) and [`key`](Self::key) name others, and
/// they exist only here, after the namespace: a subject override with no namespace beside it has
/// nothing to check.
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
/// // The payload is registered under `orders-v2`, the key under the topic's default subject.
/// #[subscriber(
///     KafkaTopic::new("orders").schema(Schema::namespace("com.acme.orders").value("orders-v2"))
/// )]
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
///
/// A subject override names the subject of a namespace, so it is not a step of the topic:
///
/// ```compile_fail
/// use ruststream_rdkafka::KafkaTopic;
///
/// // `.value(..)` overrides a subject of `.schema(..)`; write
/// // `.schema(Schema::namespace("com.acme.orders").value("orders-v2"))`.
/// let topic = KafkaTopic::new("orders").value("orders-v2");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    namespace: String,
    value: Option<String>,
    key: Option<String>,
}

impl Schema {
    /// The namespace of the topic's records (`com.acme.orders` for `com.acme.orders.Order`), with
    /// the topic's default subjects.
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
    /// #[subscriber(KafkaTopic::new("orders").schema(Schema::namespace("com.acme.orders")))]
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
    #[must_use]
    pub fn namespace(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            value: None,
            key: None,
        }
    }

    /// The subject the payload is registered under, in place of `<topic>-value`.
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
    ///     KafkaTopic::new("orders")
    ///         .schema(Schema::namespace("com.acme.orders").value("orders-foo").key("orders-bar"))
    /// )]
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
    #[must_use]
    pub fn value(mut self, subject: impl Into<String>) -> Self {
        self.value = Some(subject.into());
        self
    }

    /// The subject the record key is registered under, in place of `<topic>-key`.
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
    ///     KafkaTopic::new("orders").schema(Schema::namespace("com.acme.orders").key("order-keys"))
    /// )]
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
    #[must_use]
    pub fn key(mut self, subject: impl Into<String>) -> Self {
        self.key = Some(subject.into());
        self
    }

    pub(crate) fn namespace_name(&self) -> &str {
        &self.namespace
    }

    /// Refuses a namespace no record could have: the namespace is a runtime string, so the
    /// check runs when the subscription opens.
    pub(crate) fn validate(&self, topic: &str) -> Result<(), KafkaError> {
        let legal = !self.namespace.is_empty()
            && self.namespace.split('.').all(|part| {
                let mut chars = part.chars();
                chars
                    .next()
                    .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
                    && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
            });
        if legal {
            return Ok(());
        }
        Err(KafkaError::InvalidOptions(format!(
            "topic {topic:?} names schema namespace {:?}, which no record can have: a namespace \
             is dot-separated names, each a letter or `_` followed by letters, digits or `_` \
             (`com.acme.orders`)",
            self.namespace
        )))
    }

    /// The subjects of `topic`: the overrides, or the topic's own name under
    /// `TopicNameStrategy`.
    pub(crate) fn subjects(&self, topic: &str) -> Subjects {
        let named = |subject: &Option<String>, default: String| {
            subject.as_ref().map_or(
                Subject {
                    name: default,
                    origin: Origin::TopicName,
                },
                |name| Subject {
                    name: name.clone(),
                    origin: Origin::Override,
                },
            )
        };
        Subjects {
            value: named(&self.value, SubjectStrategy::TopicName.subject(topic, "")),
            key: named(&self.key, format!("{topic}-key")),
        }
    }
}

impl From<&str> for Schema {
    fn from(namespace: &str) -> Self {
        Self::namespace(namespace)
    }
}

impl From<String> for Schema {
    fn from(namespace: String) -> Self {
        Self::namespace(namespace)
    }
}

/// Where a subject's name came from, which is what the fix of a mismatch depends on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    /// The topic's own name, under `TopicNameStrategy`.
    TopicName,
    /// A `.value(..)` or `.key(..)` override.
    Override,
}

/// One subject a topic is checked against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Subject {
    pub(crate) name: String,
    pub(crate) origin: Origin,
}

/// The two subjects of one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Subjects {
    pub(crate) value: Subject,
    pub(crate) key: Subject,
}

/// Which side of a record a subject describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    Value,
    Key,
}

impl Side {
    /// The step that overrides this side's subject.
    pub(crate) const fn step(self) -> &'static str {
        match self {
            Self::Value => ".value(..)",
            Self::Key => ".key(..)",
        }
    }
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TopicName => "from its name",
            Self::Override => "named by the descriptor",
        })
    }
}

/// The topics of a list that opted in, each addressed by its name.
#[derive(Debug, Clone, Default)]
pub(crate) struct TopicSchemas {
    entries: Vec<(String, Schema)>,
}

impl TopicSchemas {
    pub(crate) fn push(&mut self, topic: String, schema: Schema) {
        self.entries.push((topic, schema));
    }

    /// The opted-in topics, refused when one is not in `topics` or opts in twice: both are a
    /// name the mount site got wrong, and the list is a runtime value.
    pub(crate) fn resolve(&self, topics: &[&str]) -> Result<Vec<(&str, &Schema)>, KafkaError> {
        let mut resolved: Vec<(&str, &Schema)> = Vec::with_capacity(self.entries.len());
        for (topic, schema) in &self.entries {
            if !topics.contains(&topic.as_str()) {
                return Err(KafkaError::InvalidOptions(format!(
                    "`.schema({topic:?}, ..)` names a topic the subscription does not read; its \
                     topics are {topics:?}"
                )));
            }
            if resolved.iter().any(|(seen, _)| seen == topic) {
                return Err(KafkaError::InvalidOptions(format!(
                    "topic {topic:?} opts into schema checking twice; name its namespace and \
                     subjects in one `.schema({topic:?}, ..)`"
                )));
            }
            schema.validate(topic)?;
            resolved.push((topic, schema));
        }
        Ok(resolved)
    }
}

/// The schema sets of a pattern subscription, in the order the mount site wrote them.
#[derive(Debug, Clone, Default)]
pub(crate) struct PatternSchemas {
    sets: Vec<(String, Schema)>,
}

impl PatternSchemas {
    pub(crate) fn push(&mut self, pattern: String, schema: Schema) {
        self.sets.push((pattern, schema));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.sets.is_empty()
    }

    /// Compiles the sets, refusing a pattern that is not a regex and a namespace no record can
    /// have.
    pub(crate) fn compile(&self) -> Result<CompiledSets, KafkaError> {
        let mut sets = Vec::with_capacity(self.sets.len());
        for (pattern, schema) in &self.sets {
            let regex = regex::Regex::new(pattern).map_err(|err| {
                KafkaError::InvalidOptions(format!(
                    "`.schema({pattern:?}, ..)` is not a valid topic regex: {err}"
                ))
            })?;
            schema.validate(pattern)?;
            sets.push((regex, schema.clone()));
        }
        Ok(CompiledSets { sets })
    }
}

/// The schema sets of a pattern subscription, compiled once when it opens.
#[derive(Debug, Clone)]
pub(crate) struct CompiledSets {
    sets: Vec<(regex::Regex, Schema)>,
}

impl CompiledSets {
    /// The set that takes `topic`: the first whose pattern it matches, since each later set sees
    /// only what the earlier ones left. `None` leaves the topic unchecked.
    pub(crate) fn take(&self, topic: &str) -> Option<&Schema> {
        self.sets
            .iter()
            .find(|(pattern, _)| pattern.is_match(topic))
            .map(|(_, schema)| schema)
    }
}

/// The never-implemented bound behind the subject steps a topic descriptor does not have, so the
/// mistake reads as the fix instead of as a missing method.
mod sealed {
    #[diagnostic::on_unimplemented(
        message = "`.value(..)` and `.key(..)` override a subject of `.schema(..)`, and a topic \
                   descriptor has no subject of its own to override",
        label = "a subject override belongs to the namespace it overrides",
        note = "name the namespace and the subject together: \
                `.schema(Schema::namespace(\"com.acme.orders\").value(\"orders-v2\"))`"
    )]
    pub trait SubjectAfterSchema {}
}

pub(crate) use sealed::SubjectAfterSchema;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_string_is_the_namespace_with_the_topics_default_subjects() {
        let subjects = Schema::from("com.acme.orders").subjects("orders");
        assert_eq!(subjects.value.name, "orders-value");
        assert_eq!(subjects.value.origin, Origin::TopicName);
        assert_eq!(subjects.key.name, "orders-key");
        assert_eq!(subjects.key.origin, Origin::TopicName);
    }

    #[test]
    fn overrides_name_each_subject_on_their_own() {
        let subjects = Schema::namespace("com.acme.orders")
            .value("orders-foo")
            .subjects("orders");
        assert_eq!(subjects.value.name, "orders-foo");
        assert_eq!(subjects.value.origin, Origin::Override);
        assert_eq!(subjects.key.name, "orders-key", "the key keeps its default");
        let subjects = Schema::namespace("com.acme.orders")
            .key("orders-bar")
            .subjects("orders");
        assert_eq!(subjects.key.name, "orders-bar");
        assert_eq!(subjects.key.origin, Origin::Override);
    }

    #[test]
    fn a_namespace_no_record_can_have_is_refused() {
        for bad in ["", "com..acme", "com.1acme", "com.acme-orders", ".com"] {
            assert!(
                Schema::namespace(bad).validate("orders").is_err(),
                "{bad:?} must be refused"
            );
        }
        for good in ["com.acme.orders", "_x", "Orders"] {
            assert!(Schema::namespace(good).validate("orders").is_ok());
        }
    }

    #[test]
    fn a_list_topic_is_addressed_by_name() {
        let mut schemas = TopicSchemas::default();
        schemas.push("returns".into(), Schema::from("com.acme"));
        let topics = ["orders", "returns"];
        let resolved = schemas.resolve(&topics).expect("a listed topic opts in");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].0, "returns");
    }

    #[test]
    fn a_topic_the_list_does_not_read_is_refused_with_the_list() {
        let mut schemas = TopicSchemas::default();
        schemas.push("refunds".into(), Schema::from("com.acme"));
        let err = schemas
            .resolve(&["orders", "returns"])
            .expect_err("an unlisted topic is a mistyped name");
        let KafkaError::InvalidOptions(message) = err else {
            panic!("an unlisted topic is an options error");
        };
        assert!(message.contains("\"refunds\""), "{message}");
        assert!(message.contains("[\"orders\", \"returns\"]"), "{message}");
    }

    #[test]
    fn a_topic_that_opts_in_twice_is_refused() {
        let mut schemas = TopicSchemas::default();
        schemas.push("orders".into(), Schema::from("com.acme"));
        schemas.push("orders".into(), Schema::from("com.other"));
        assert!(schemas.resolve(&["orders"]).is_err());
    }

    // Four topics: the first set takes three, the second matches the fourth and one of the
    // three, and that one keeps the first set's namespace.
    #[test]
    fn sets_narrow_in_the_written_order() {
        let mut sets = PatternSchemas::default();
        sets.push(r"^orders\.".into(), Schema::from("com.acme.orders"));
        sets.push(r"(eu|archive)$".into(), Schema::from("com.acme.archive"));
        let sets = sets.compile().expect("valid sets");
        let namespace = |topic| sets.take(topic).map(Schema::namespace_name);
        assert_eq!(namespace("orders.us"), Some("com.acme.orders"));
        assert_eq!(namespace("orders.asia"), Some("com.acme.orders"));
        assert_eq!(namespace("orders.eu"), Some("com.acme.orders"));
        assert_eq!(namespace("returns.archive"), Some("com.acme.archive"));
        assert_eq!(
            namespace("audit.log"),
            None,
            "a topic no set takes is unchecked"
        );
    }

    #[test]
    fn a_set_names_one_override_for_all_its_topics_and_defaults_per_topic() {
        let mut sets = PatternSchemas::default();
        sets.push(r"^orders\.".into(), Schema::from("com.acme.orders"));
        sets.push(
            r"^returns\.".into(),
            Schema::namespace("com.acme.returns").value("returns-value"),
        );
        let sets = sets.compile().expect("valid sets");
        let subject = |topic: &str| sets.take(topic).map(|s| s.subjects(topic).value.name);
        assert_eq!(subject("orders.eu").as_deref(), Some("orders.eu-value"));
        assert_eq!(subject("returns.eu").as_deref(), Some("returns-value"));
        assert_eq!(subject("returns.us").as_deref(), Some("returns-value"));
    }

    #[test]
    fn a_set_that_is_not_a_regex_is_refused() {
        let mut sets = PatternSchemas::default();
        sets.push("^orders.(".into(), Schema::from("com.acme.orders"));
        assert!(sets.compile().is_err());
    }
}
