//! The startup check of a topic that opted in with `.schema(..)`: the subjects computed for it
//! exist in the registry and declare records of its namespace.
//!
//! It runs once per topic while the subscription opens, against the registry the broker carries,
//! and it caches what it fetched for the codecs that read the same subjects. Nothing here runs per
//! delivery.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;
use tokio::runtime::Handle;

use super::descriptor::{CompiledSets, Origin, Schema, Side, Subject};
use crate::error::KafkaError;
use crate::schema_registry::{SchemaRegistry, SchemaType};

/// The kinds of an enum input whose variants each carry a registry record: what a discriminator
/// reading a record's writer schema names.
///
/// The `Schema` derive implements it on the kind enum the core's `InMessages` derive generates,
/// one record full name per variant.
#[doc(hidden)]
pub trait RecordKind: Sized + Copy + 'static {
    /// The record full names of the set, one per variant, in the order the variants are written.
    const RECORDS: &'static [&'static str];

    /// The variant whose record has `full_name`, or `None` when no variant carries it.
    fn from_record(full_name: &str) -> Option<Self>;
}

/// Checks the subjects of one topic that opted in with `schema`.
///
/// The value subject must exist and declare a record of the namespace. The key subject is
/// optional unless the descriptor named it: a topic whose keys are plain strings registers no key
/// schema. A key subject that exists and declares records must declare one of the namespace too.
///
/// A JSON Schema names no record, so a JSON subject is checked for presence only.
pub(crate) async fn check_topic(
    registry: &SchemaRegistry,
    topic: &str,
    schema: &Schema,
) -> Result<(), KafkaError> {
    let subjects = schema.subjects(topic);
    check_subject(registry, topic, schema, Side::Value, &subjects.value).await?;
    check_subject(registry, topic, schema, Side::Key, &subjects.key).await
}

async fn check_subject(
    registry: &SchemaRegistry,
    topic: &str,
    schema: &Schema,
    side: Side,
    subject: &Subject,
) -> Result<(), KafkaError> {
    let Some(registered) = registry.latest(&subject.name).await? else {
        if side == Side::Key && subject.origin == Origin::TopicName {
            return Ok(());
        }
        return Err(mismatch(
            topic,
            subject,
            "which the registry does not hold",
            side,
            "register its migrations (`cargo kafka-schema migrate run`)",
        ));
    };
    let records = match registered.schema_type() {
        SchemaType::Avro => avro_records(registered.definition()),
        SchemaType::Protobuf => proto_records(registered.definition()),
        SchemaType::Json => return Ok(()),
    };
    let namespace = schema.namespace_name();
    if records.is_empty() && side == Side::Key {
        return Ok(());
    }
    if records
        .iter()
        .any(|record| record_namespace(record) == namespace)
    {
        return Ok(());
    }
    let mut declares = format!("which declares no record in namespace `{namespace}`");
    if !records.is_empty() {
        let _ = write!(
            declares,
            " (its newest schema declares `{}`)",
            records.join("`, `")
        );
    }
    Err(mismatch(topic, subject, &declares, side, "add a migration"))
}

/// The schema sets of one pattern subscription, checked as its topics become known.
///
/// The topics the cluster holds when the subscription opens are checked before it opens, and a
/// mismatch fails startup. A topic that appears later is checked when the group first assigns it
/// to this member, on the runtime and off the delivery path; a mismatch then is logged, since
/// the service is already running. Each topic is checked once.
pub(crate) struct SchemaWatch {
    subscription: String,
    sets: CompiledSets,
    registry: SchemaRegistry,
    runtime: Handle,
    state: Mutex<WatchState>,
}

#[derive(Default)]
struct WatchState {
    /// Set once the startup check is done; until then assigned topics wait in `pending`.
    armed: bool,
    seen: HashSet<String>,
    pending: Vec<String>,
}

impl std::fmt::Debug for SchemaWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaWatch")
            .field("subscription", &self.subscription)
            .finish_non_exhaustive()
    }
}

impl SchemaWatch {
    pub(crate) fn new(
        subscription: String,
        sets: CompiledSets,
        registry: SchemaRegistry,
        runtime: Handle,
    ) -> Self {
        Self {
            subscription,
            sets,
            registry,
            runtime,
            state: Mutex::new(WatchState::default()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, WatchState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The group assigned this member a partition of `topic`.
    pub(crate) fn appeared(self: &Arc<Self>, topic: &str) {
        let mut state = self.lock();
        if !state.armed {
            if !state.pending.iter().any(|seen| seen == topic) {
                state.pending.push(topic.to_owned());
            }
            return;
        }
        if self.sets.take(topic).is_none() || !state.seen.insert(topic.to_owned()) {
            return;
        }
        drop(state);
        let watch = Arc::clone(self);
        let topic = topic.to_owned();
        self.runtime.spawn(async move {
            watch.check_late(&topic).await;
        });
    }

    async fn check_late(&self, topic: &str) {
        let Some(schema) = self.sets.take(topic) else {
            return;
        };
        if let Err(err) = check_topic(&self.registry, topic, schema).await {
            tracing::error!(
                target: "ruststream_rdkafka",
                subscription = %self.subscription,
                topic,
                error = %err,
                "a topic that appeared after the pattern subscription opened does not match its \
                 schema set; its deliveries decode by the schema id each record carries",
            );
        }
    }

    /// The startup check: every topic of `known` a set takes, and every topic assigned while
    /// the subscription opened. Afterwards each new topic is checked as it appears.
    ///
    /// # Errors
    ///
    /// The first mismatch, as [`check_topic`] reports it.
    pub(crate) async fn arm(self: &Arc<Self>, known: Vec<String>) -> Result<(), KafkaError> {
        let mut topics = known;
        topics.append(&mut self.lock().pending);
        topics.sort_unstable();
        topics.dedup();
        for topic in &topics {
            if let Some(schema) = self.sets.take(topic) {
                check_topic(&self.registry, topic, schema).await?;
            }
        }
        let late = {
            let mut state = self.lock();
            state.seen.extend(topics);
            state.armed = true;
            std::mem::take(&mut state.pending)
        };
        for topic in late {
            self.appeared(&topic);
        }
        Ok(())
    }
}

fn mismatch(topic: &str, subject: &Subject, what: &str, side: Side, fix: &str) -> KafkaError {
    KafkaError::SchemaRegistry(
        format!(
            "topic `{topic}` uses subject `{}` ({}), {what}; {fix} or name the subject with `{}`",
            subject.name,
            subject.origin,
            side.step(),
        )
        .into(),
    )
}

/// The namespace of a record full name: everything before its last dot.
fn record_namespace(full_name: &str) -> &str {
    full_name
        .rsplit_once('.')
        .map_or("", |(namespace, _)| namespace)
}

/// The records an Avro schema declares at its top: the record itself, or each record of a
/// top-level union (inline, or referenced by full name as a union of subject references is).
fn avro_records(definition: &str) -> Vec<String> {
    let Ok(schema) = serde_json::from_str::<Value>(definition) else {
        return Vec::new();
    };
    let mut records = Vec::new();
    match &schema {
        Value::Array(members) => {
            for member in members {
                avro_named(member, &mut records);
            }
        }
        other => avro_named(other, &mut records),
    }
    records
}

fn avro_named(schema: &Value, records: &mut Vec<String>) {
    match schema {
        Value::Object(fields)
            if matches!(
                fields.get("type").and_then(Value::as_str),
                Some("record" | "error")
            ) =>
        {
            let Some(name) = fields.get("name").and_then(Value::as_str) else {
                return;
            };
            let full = match fields.get("namespace").and_then(Value::as_str) {
                Some(namespace) if !name.contains('.') && !namespace.is_empty() => {
                    format!("{namespace}.{name}")
                }
                _ => name.to_owned(),
            };
            records.push(full);
        }
        Value::String(reference) if reference.contains('.') => records.push(reference.clone()),
        _ => {}
    }
}

/// The messages a `.proto` source declares at its top, each with the file's package.
fn proto_records(source: &str) -> Vec<String> {
    let mut package = None;
    let mut messages = Vec::new();
    let mut depth = 0usize;
    let mut words = source
        .split(|c: char| c.is_whitespace() || c == ';')
        .flat_map(split_braces)
        .filter(|word| !word.is_empty());
    while let Some(word) = words.next() {
        match word {
            "{" => depth += 1,
            "}" => depth = depth.saturating_sub(1),
            "package" if depth == 0 => package = words.next().map(str::to_owned),
            "message" if depth == 0 => {
                if let Some(name) = words.next() {
                    messages.push(name.to_owned());
                }
            }
            _ => {}
        }
    }
    messages
        .into_iter()
        .map(|name| match &package {
            Some(package) => format!("{package}.{name}"),
            None => name,
        })
        .collect()
}

/// Splits a word around the braces it is glued to (`Order{` is `Order` and `{`).
fn split_braces(word: &str) -> impl Iterator<Item = &str> {
    let mut parts = Vec::new();
    let mut start = 0;
    for (index, c) in word.char_indices() {
        if c == '{' || c == '}' {
            parts.push(&word[start..index]);
            parts.push(&word[index..=index]);
            start = index + 1;
        }
    }
    parts.push(&word[start..]);
    parts.into_iter()
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::schema::PatternSchemas;

    /// A watch over one set, `^orders\.` in `com.acme.orders`, against `server`.
    fn watch(server: &MockServer) -> Arc<SchemaWatch> {
        let mut sets = PatternSchemas::default();
        sets.push(r"^orders\.".into(), Schema::from("com.acme.orders"));
        Arc::new(SchemaWatch::new(
            "^orders\\..*".into(),
            sets.compile().expect("valid sets"),
            SchemaRegistry::new(server.uri()),
            Handle::current(),
        ))
    }

    // A topic assigned while the subscription opened is checked by the startup check, so its
    // mismatch fails startup rather than reaching only a log line.
    #[tokio::test]
    async fn a_topic_assigned_before_the_startup_check_fails_it() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/orders.eu-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 1,
                "schema": r#"{"type":"record","name":"Order","namespace":"com.other","fields":[]}"#,
            })))
            .mount(&server)
            .await;
        let watch = watch(&server);
        watch.appeared("orders.eu");
        let err = watch
            .arm(Vec::new())
            .await
            .expect_err("the assigned topic's subject declares another namespace");
        assert!(err.to_string().contains("topic `orders.eu`"), "{err}");
    }

    #[tokio::test]
    async fn a_topic_no_set_takes_is_not_checked() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .expect(0)
            .mount(&server)
            .await;
        watch(&server)
            .arm(vec!["audit.log".into()])
            .await
            .expect("no set takes the topic");
    }

    #[test]
    fn an_avro_record_declares_its_full_name() {
        let records = avro_records(
            r#"{"type":"record","name":"Order","namespace":"com.acme.orders","fields":[]}"#,
        );
        assert_eq!(records, ["com.acme.orders.Order"]);
        let dotted = avro_records(r#"{"type":"record","name":"com.acme.Order","fields":[]}"#);
        assert_eq!(dotted, ["com.acme.Order"]);
    }

    #[test]
    fn an_avro_union_declares_each_member_record() {
        let records = avro_records(
            r#"["com.acme.orders.OrderCreated",
                {"type":"record","name":"OrderCancelled","namespace":"com.acme.orders","fields":[]},
                "null"]"#,
        );
        assert_eq!(
            records,
            [
                "com.acme.orders.OrderCreated",
                "com.acme.orders.OrderCancelled"
            ]
        );
    }

    #[test]
    fn a_primitive_avro_schema_declares_no_record() {
        assert_eq!(avro_records(r#""string""#), Vec::<String>::new());
        assert_eq!(avro_records("not json"), Vec::<String>::new());
    }

    #[test]
    fn a_proto_source_declares_its_top_level_messages_in_its_package() {
        let records = proto_records(
            "syntax = \"proto3\";\npackage com.acme.payments;\n\
             message Payment{ message Line { int64 qty = 1; } int64 id = 1; }\n\
             message Refund {\n int64 id = 1;\n}\n",
        );
        assert_eq!(
            records,
            ["com.acme.payments.Payment", "com.acme.payments.Refund"]
        );
    }

    #[test]
    fn a_record_namespace_is_everything_before_the_last_dot() {
        assert_eq!(record_namespace("com.acme.orders.Order"), "com.acme.orders");
        assert_eq!(record_namespace("Order"), "");
    }
}
