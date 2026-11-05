//! What a registry codec reads from a frame before decoding it: the record the payload's schema
//! names.
//!
//! A message set declared with `#[in_messages(by_codec)]` picks its variant by this record:
//! [`SchemaFramed`](super::SchemaFramed) and [`AvroCodec`](crate::avro::AvroCodec) read it, and
//! each answers for every model the `Schema` derive bound to a record. The names a schema can
//! give are worked out once per schema, so reading a frame costs a delivery a lookup of its
//! schema id and, for an Avro union, the branch index at the head of the datum.

#[cfg(feature = "schema-check")]
use ruststream::codec::{Codec, FrameNames};
use serde_json::Value;

#[cfg(feature = "schema-check")]
use super::SchemaFramed;
#[cfg(feature = "schema-check")]
use crate::schema::Record;

/// The record a delivery's schema names: the frame of the crate's registry codecs.
///
/// A `#[in_messages(by_codec)]` set decoded under one of them receives the variant whose model
/// carries this record. The record is the Avro record the schema declares (for a top-level
/// union, the branch the datum was written as), or the `title` of a JSON Schema.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "avro", feature = "schema-check"))]
/// # mod demo {
/// use ruststream_rdkafka::avro::{AvroCodec, AvroSchema};
/// use ruststream_rdkafka::prelude::*;
/// use ruststream_rdkafka::schema::Schema;
/// use serde::Deserialize;
///
/// #[derive(Deserialize, AvroSchema, Schema)]
/// #[avro(namespace = "com.acme.orders")]
/// pub struct OrderCreated {
///     pub id: i64,
/// }
///
/// #[derive(Deserialize, AvroSchema, Schema)]
/// #[avro(namespace = "com.acme.orders")]
/// pub struct OrderCancelled {
///     pub id: i64,
///     pub reason: String,
/// }
///
/// /// The variant is the record the delivery's schema id names.
/// #[derive(InMessages, Schema)]
/// #[in_messages(by_codec)]
/// pub enum OrderEvent {
///     Created(OrderCreated),
///     Cancelled(OrderCancelled),
/// }
///
/// #[subscriber("orders")]
/// async fn on_event(event: &OrderEvent) -> HandlerOutcome {
///     match event {
///         OrderEvent::Created(created) => tracing::info!(created.id, "created"),
///         OrderEvent::Cancelled(cancelled) => tracing::info!(cancelled.id, "cancelled"),
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> impl App {
///     let prefetch = SchemaPrefetch::new(SchemaRegistry::new("http://localhost:8081"));
///     let codec = AvroCodec::registry(&prefetch);
///     let broker = KafkaBroker::new(["localhost:9092"])
///         .default_group("orders-svc")
///         .schema_prefetch(prefetch);
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker_codec(broker, codec, |b| b.include(on_event))
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordFrame<'a> {
    record: Option<RecordName<'a>>,
}

/// A record's name as a schema gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordName<'a> {
    /// A full name, namespace included: what an Avro schema declares.
    Full(&'a str),
    /// A JSON Schema `title`. JSON Schema has no namespaces, so a title without a dot names a
    /// record by its simple name.
    Title(&'a str),
}

impl<'a> RecordFrame<'a> {
    /// The record's name as the schema gives it, or `None` when the schema names no record (a
    /// primitive, a union branch that is not a record, a JSON Schema without a `title`).
    #[must_use]
    pub const fn record(&self) -> Option<&'a str> {
        match self.record {
            Some(RecordName::Full(name) | RecordName::Title(name)) => Some(name),
            None => None,
        }
    }

    /// Whether the frame names the record `full_name`.
    ///
    /// An Avro name matches the full name exactly. A JSON Schema `title` matches it exactly, or,
    /// when the title carries no namespace, matches the full name's last segment: the derive
    /// names a JSON record `namespace.Type`, and the schema it registers is titled `Type`.
    #[must_use]
    pub fn names(&self, full_name: &str) -> bool {
        match self.record {
            Some(RecordName::Full(name)) => name == full_name,
            Some(RecordName::Title(title)) if title.contains('.') => title == full_name,
            Some(RecordName::Title(title)) => {
                full_name.rsplit_once('.').map_or(full_name, |(_, simple)| simple) == title
            }
            None => false,
        }
    }
}

/// A model the `Schema` derive bound to a record is named by a frame that carries that record.
#[cfg(feature = "schema-check")]
impl<C: Codec, Model: Record> FrameNames<Model> for SchemaFramed<C> {
    fn names(frame: &RecordFrame<'_>) -> bool {
        frame.names(Model::RECORD)
    }
}

/// The records one schema can name, worked out once per schema.
#[derive(Debug)]
pub(crate) enum SchemaNames {
    /// The schema names no record.
    Nothing,
    /// One record, whatever the datum.
    One(OwnedName),
    /// An Avro union at the top: the record each branch names, by branch index.
    Union(Box<[Option<Box<str>>]>),
}

/// A record name the schema owns.
#[derive(Debug)]
pub(crate) enum OwnedName {
    Full(Box<str>),
    Title(Box<str>),
}

impl SchemaNames {
    /// The names an Avro schema, given as its JSON text, declares at its top.
    pub(crate) fn of_avro(definition: &str) -> Self {
        let Ok(schema) = serde_json::from_str::<Value>(definition) else {
            return Self::Nothing;
        };
        match &schema {
            Value::Array(branches) => {
                Self::Union(branches.iter().map(avro_record).collect::<Vec<_>>().into())
            }
            other => avro_record(other).map_or(Self::Nothing, |name| Self::One(OwnedName::Full(name))),
        }
    }

    /// The name a JSON Schema, given as its JSON text, carries: its `title`.
    pub(crate) fn of_json(definition: &str) -> Self {
        serde_json::from_str::<Value>(definition)
            .ok()
            .as_ref()
            .and_then(|schema| schema.get("title"))
            .and_then(Value::as_str)
            .map_or(Self::Nothing, |title| Self::One(OwnedName::Title(title.into())))
    }

    /// The frame of a datum written with this schema.
    pub(crate) fn frame<'a>(&'a self, datum: &[u8]) -> RecordFrame<'a> {
        let record = match self {
            Self::Nothing => None,
            Self::One(OwnedName::Full(name)) => Some(RecordName::Full(name)),
            Self::One(OwnedName::Title(title)) => Some(RecordName::Title(title)),
            Self::Union(branches) => union_branch(datum)
                .and_then(|branch| branches.get(branch))
                .and_then(Option::as_deref)
                .map(RecordName::Full),
        };
        RecordFrame { record }
    }
}

/// The full name of the record an Avro schema node is, or references.
fn avro_record(schema: &Value) -> Option<Box<str>> {
    match schema {
        Value::Object(fields)
            if matches!(
                fields.get("type").and_then(Value::as_str),
                Some("record" | "error")
            ) =>
        {
            let name = fields.get("name").and_then(Value::as_str)?;
            Some(match fields.get("namespace").and_then(Value::as_str) {
                Some(namespace) if !name.contains('.') && !namespace.is_empty() => {
                    format!("{namespace}.{name}").into()
                }
                _ => name.into(),
            })
        }
        // A named reference (a union of subject references names each member by its full
        // name); the primitives name no record.
        Value::String(reference) if !is_avro_primitive(reference) => Some(reference.as_str().into()),
        _ => None,
    }
}

fn is_avro_primitive(name: &str) -> bool {
    matches!(
        name,
        "null" | "boolean" | "int" | "long" | "float" | "double" | "bytes" | "string"
    )
}

/// The branch index an Avro union datum starts with: a zig-zag encoded `long`.
fn union_branch(datum: &[u8]) -> Option<usize> {
    let mut value: u64 = 0;
    for (index, byte) in datum.iter().take(10).enumerate() {
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            // Zig-zag: the low bit is the sign.
            if value & 1 == 1 {
                return None;
            }
            return usize::try_from(value >> 1).ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORDER_CREATED: &str = "com.acme.orders.OrderCreated";
    const ORDER_CANCELLED: &str = "com.acme.orders.OrderCancelled";

    #[test]
    fn an_avro_record_names_its_full_name() {
        let names = SchemaNames::of_avro(
            r#"{"type":"record","name":"OrderCreated","namespace":"com.acme.orders","fields":[]}"#,
        );
        let frame = names.frame(&[]);
        assert_eq!(frame.record(), Some(ORDER_CREATED));
        assert!(frame.names(ORDER_CREATED));
        assert!(!frame.names("OrderCreated"));
        assert!(!frame.names(ORDER_CANCELLED));
    }

    #[test]
    fn an_avro_union_names_the_branch_the_datum_was_written_as() {
        let names = SchemaNames::of_avro(&format!(r#"["null", "{ORDER_CREATED}", "{ORDER_CANCELLED}"]"#));
        // Branch indexes are zig-zag longs: 1 is written 0x02, 2 is 0x04.
        assert!(names.frame(&[0x04, 0x2a]).names(ORDER_CANCELLED));
        assert!(names.frame(&[0x02, 0x2a]).names(ORDER_CREATED));
        assert_eq!(names.frame(&[0x00]).record(), None);
        assert_eq!(names.frame(&[0x06]).record(), None);
        assert_eq!(names.frame(&[]).record(), None);
    }

    #[test]
    fn a_json_title_names_the_record_by_its_simple_name() {
        let frame_of = |title: &str| {
            SchemaNames::of_json(&format!(r#"{{"title":"{title}","type":"object"}}"#))
        };
        assert!(frame_of("OrderCreated").frame(&[]).names(ORDER_CREATED));
        assert!(!frame_of("OrderCreated").frame(&[]).names(ORDER_CANCELLED));
        assert!(frame_of(ORDER_CREATED).frame(&[]).names(ORDER_CREATED));
        assert!(!frame_of("com.other.OrderCreated").frame(&[]).names(ORDER_CREATED));
    }

    #[test]
    fn a_schema_without_a_record_names_nothing() {
        for names in [
            SchemaNames::of_avro(r#""string""#),
            SchemaNames::of_avro("not json"),
            SchemaNames::of_json(r#"{"type":"object"}"#),
        ] {
            assert_eq!(names.frame(&[0x02]).record(), None);
            assert!(!names.frame(&[0x02]).names(ORDER_CREATED));
        }
    }

    #[test]
    fn a_branch_index_reads_a_multi_byte_varint() {
        // 64 zig-zags to 128, which takes two bytes.
        assert_eq!(union_branch(&[0x80, 0x01]), Some(64));
        assert_eq!(union_branch(&[0x80]), None);
        assert_eq!(union_branch(&[0x01]), None, "a negative branch index");
    }
}
