//! A migration's Avro schema as a tree with resolved full names: what the derive compares a
//! type with.
//!
//! A named type used again later in the schema is expanded where it is used, so both sides of
//! the comparison are written out in full. A record that refers to itself is marked recursive:
//! its form would be an infinite constant, so the whole-schema comparison skips it. A name the
//! schema uses without defining it is looked up through the subject's references.

use std::collections::HashMap;
use std::rc::Rc;

use serde_json::{Map, Value};

use crate::json_text;

/// An Avro type with resolved names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Avro {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array(Box<Self>),
    Map(Box<Self>),
    /// `["null", T]`.
    Nullable(Box<Self>),
    Union(Vec<Self>),
    Record(Rc<Record>),
    Enum(Rc<Enum>),
    Fixed(Rc<Fixed>),
    /// A reference to a record from inside its own definition.
    Recursive(String),
}

/// A named type's resolved name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FullName {
    pub(crate) namespace: String,
    pub(crate) name: String,
}

impl FullName {
    pub(crate) fn full(&self) -> String {
        if self.namespace.is_empty() {
            self.name.clone()
        } else {
            format!("{}.{}", self.namespace, self.name)
        }
    }

    fn resolve(name: &str, namespace: Option<&str>, enclosing: &str) -> Self {
        match name.rsplit_once('.') {
            Some((space, name)) => Self {
                namespace: space.to_owned(),
                name: name.to_owned(),
            },
            None => Self {
                namespace: namespace.unwrap_or(enclosing).to_owned(),
                name: name.to_owned(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record {
    pub(crate) name: FullName,
    pub(crate) fields: Vec<Field>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Field {
    pub(crate) name: String,
    pub(crate) ty: Avro,
    /// Minified JSON with sorted keys.
    pub(crate) default: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Enum {
    pub(crate) name: FullName,
    pub(crate) symbols: Vec<String>,
    pub(crate) default: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Fixed {
    pub(crate) name: FullName,
    pub(crate) size: u64,
}

impl Avro {
    /// True when the type holds a record that refers to itself.
    pub(crate) fn is_recursive(&self) -> bool {
        match self {
            Self::Recursive(_) => true,
            Self::Array(inner) | Self::Map(inner) | Self::Nullable(inner) => inner.is_recursive(),
            Self::Union(branches) => branches.iter().any(Self::is_recursive),
            Self::Record(record) => record.fields.iter().any(|field| field.ty.is_recursive()),
            _ => false,
        }
    }

    /// The named type with this full name, wherever the tree defines it.
    pub(crate) fn find(&self, full: &str) -> Option<&Self> {
        match self {
            Self::Record(record) if record.name.full() == full => Some(self),
            Self::Enum(def) if def.name.full() == full => Some(self),
            Self::Fixed(def) if def.name.full() == full => Some(self),
            Self::Record(record) => record.fields.iter().find_map(|field| field.ty.find(full)),
            Self::Array(inner) | Self::Map(inner) | Self::Nullable(inner) => inner.find(full),
            Self::Union(branches) => branches.iter().find_map(|branch| branch.find(full)),
            _ => None,
        }
    }

    /// What the per-field bound names: `Long`, `Array<Long>`, `Nullable<Record>`.
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Null => "null".to_owned(),
            Self::Boolean => "boolean".to_owned(),
            Self::Int => "int".to_owned(),
            Self::Long => "long".to_owned(),
            Self::Float => "float".to_owned(),
            Self::Double => "double".to_owned(),
            Self::Bytes => "bytes".to_owned(),
            Self::String => "string".to_owned(),
            Self::Array(_) => "array".to_owned(),
            Self::Map(_) => "map".to_owned(),
            Self::Nullable(_) | Self::Union(_) => "union".to_owned(),
            Self::Record(record) => format!("record `{}`", record.name.full()),
            Self::Enum(def) => format!("enum `{}`", def.name.full()),
            Self::Fixed(def) => format!("fixed `{}`", def.name.full()),
            Self::Recursive(name) => format!("record `{name}`"),
        }
    }
}

/// Parses one migration. `external` finds a named type the schema uses without defining it,
/// in the subjects `subject.toml` references.
///
/// # Errors
///
/// A schema that is not JSON, not Avro, or that names a type it neither defines nor
/// references.
pub(crate) fn parse(text: &str, external: &dyn Fn(&str) -> Option<Avro>) -> Result<Avro, String> {
    let value: Value = serde_json::from_str(text).map_err(|err| format!("not JSON: {err}"))?;
    let mut parser = Parser {
        defined: HashMap::new(),
        open: Vec::new(),
        external,
    };
    parser.parse(&value, "")
}

struct Parser<'a> {
    defined: HashMap<String, Avro>,
    /// The records whose definitions are being read.
    open: Vec<String>,
    external: &'a dyn Fn(&str) -> Option<Avro>,
}

impl Parser<'_> {
    fn parse(&mut self, value: &Value, enclosing: &str) -> Result<Avro, String> {
        match value {
            Value::String(name) => self.named(name, enclosing),
            Value::Array(branches) => {
                let branches = branches
                    .iter()
                    .map(|branch| self.parse(branch, enclosing))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(match branches.as_slice() {
                    [Avro::Null, other] => Avro::Nullable(Box::new(other.clone())),
                    _ => Avro::Union(branches),
                })
            }
            Value::Object(object) => self.object(object, enclosing),
            other => Err(format!("`{other}` is not an Avro schema")),
        }
    }

    fn named(&self, name: &str, enclosing: &str) -> Result<Avro, String> {
        if let Some(primitive) = primitive(name) {
            return Ok(primitive);
        }
        let full = FullName::resolve(name, None, enclosing).full();
        if self.open.contains(&full) {
            return Ok(Avro::Recursive(full));
        }
        if let Some(found) = self.defined.get(&full).or_else(|| self.defined.get(name)) {
            return Ok(found.clone());
        }
        (self.external)(&full)
            .or_else(|| (self.external)(name))
            .ok_or_else(|| {
                format!(
                    "the schema uses the type `{full}`, which it does not define and no \
                     reference of `subject.toml` provides"
                )
            })
    }

    fn object(&mut self, object: &Map<String, Value>, enclosing: &str) -> Result<Avro, String> {
        let kind = object.get("type");
        let kind_name = match kind {
            Some(Value::String(kind)) => kind.as_str(),
            Some(other) => return self.parse(other, enclosing),
            None => return Err("a schema object has no `type`".to_owned()),
        };
        let name = || {
            let name = object
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("a `{kind_name}` has no `name`"))?;
            Ok::<_, String>(FullName::resolve(
                name,
                object.get("namespace").and_then(Value::as_str),
                enclosing,
            ))
        };
        match kind_name {
            "record" | "error" => {
                let name = name()?;
                let full = name.full();
                self.open.push(full.clone());
                let fields = object
                    .get("fields")
                    .and_then(Value::as_array)
                    .ok_or_else(|| format!("record `{full}` has no `fields`"))?
                    .iter()
                    .map(|field| self.field(field, &name.namespace, &full))
                    .collect::<Result<Vec<_>, _>>();
                self.open.pop();
                let record = Avro::Record(Rc::new(Record {
                    name,
                    fields: fields?,
                }));
                self.defined.insert(full, record.clone());
                Ok(record)
            }
            "enum" => {
                let name = name()?;
                let symbols = object
                    .get("symbols")
                    .and_then(Value::as_array)
                    .ok_or_else(|| format!("enum `{}` has no `symbols`", name.full()))?
                    .iter()
                    .map(|symbol| symbol.as_str().map(str::to_owned))
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| {
                        format!("enum `{}` has a symbol that is not a string", name.full())
                    })?;
                let default = object
                    .get("default")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let full = name.full();
                let def = Avro::Enum(Rc::new(Enum {
                    name,
                    symbols,
                    default,
                }));
                self.defined.insert(full, def.clone());
                Ok(def)
            }
            "fixed" => {
                let name = name()?;
                let size = object
                    .get("size")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| format!("fixed `{}` has no `size`", name.full()))?;
                let full = name.full();
                let def = Avro::Fixed(Rc::new(Fixed { name, size }));
                self.defined.insert(full, def.clone());
                Ok(def)
            }
            "array" => {
                let items = object
                    .get("items")
                    .ok_or_else(|| "an array has no `items`".to_owned())?;
                Ok(Avro::Array(Box::new(self.parse(items, enclosing)?)))
            }
            "map" => {
                let values = object
                    .get("values")
                    .ok_or_else(|| "a map has no `values`".to_owned())?;
                Ok(Avro::Map(Box::new(self.parse(values, enclosing)?)))
            }
            // A primitive in object form; a logical type is compared by the migrations tool.
            other => self.named(other, enclosing),
        }
    }

    fn field(&mut self, field: &Value, namespace: &str, record: &str) -> Result<Field, String> {
        let name = field
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("a field of record `{record}` has no `name`"))?;
        let ty = field
            .get("type")
            .ok_or_else(|| format!("field `{name}` of record `{record}` has no `type`"))?;
        Ok(Field {
            name: name.to_owned(),
            ty: self.parse(ty, namespace)?,
            default: field.get("default").map(json_text),
        })
    }
}

fn primitive(name: &str) -> Option<Avro> {
    Some(match name {
        "null" => Avro::Null,
        "boolean" => Avro::Boolean,
        "int" => Avro::Int,
        "long" => Avro::Long,
        "float" => Avro::Float,
        "double" => Avro::Double,
        "bytes" => Avro::Bytes,
        "string" => Avro::String,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn none(_: &str) -> Option<Avro> {
        None
    }

    #[test]
    fn nested_records_take_the_enclosing_namespace_and_a_second_use_is_expanded() {
        let schema = parse(
            r#"{"type":"record","name":"Order","namespace":"com.acme.orders","fields":[
                {"name":"item","type":{"type":"record","name":"Item","fields":[
                    {"name":"sku","type":"string"}]}},
                {"name":"spare","type":["null","Item"],"default":null},
                {"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}}]}"#,
            &none,
        )
        .expect("the schema parses");
        let Avro::Record(order) = &schema else {
            panic!("the top level is a record");
        };
        assert_eq!(order.name.full(), "com.acme.orders.Order");
        let Avro::Record(item) = &order.fields[0].ty else {
            panic!("the first field is a record");
        };
        assert_eq!(item.name.full(), "com.acme.orders.Item");
        assert_eq!(
            order.fields[1].ty,
            Avro::Nullable(Box::new(order.fields[0].ty.clone()))
        );
        assert_eq!(order.fields[1].default.as_deref(), Some("null"));
        assert_eq!(order.fields[2].ty, Avro::Long);
        assert!(schema.find("com.acme.orders.Item").is_some());
        assert!(!schema.is_recursive());
    }

    #[test]
    fn a_record_that_refers_to_itself_is_recursive() {
        let schema = parse(
            r#"{"type":"record","name":"Tree","fields":[
                {"name":"children","type":{"type":"array","items":"Tree"}}]}"#,
            &none,
        )
        .expect("the schema parses");
        assert!(schema.is_recursive());
    }

    #[test]
    fn an_undefined_name_is_looked_up_in_the_references() {
        let created = || {
            parse(
                r#"{"type":"record","name":"Created","namespace":"com.acme","fields":[]}"#,
                &none,
            )
            .ok()
        };
        let lookup = |name: &str| (name == "com.acme.Created").then(created).flatten();
        let schema = parse(r#"["com.acme.Created"]"#, &lookup).expect("the reference resolves");
        assert!(matches!(&schema, Avro::Union(branches) if branches.len() == 1));
        let err = parse(r#"["com.acme.Cancelled"]"#, &lookup).expect_err("nothing provides it");
        assert!(err.contains("`com.acme.Cancelled`"), "{err}");
    }
}
