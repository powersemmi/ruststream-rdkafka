//! A subject's JSON Schema as the check compares it: `$ref` resolved, `required` folded into the
//! properties, every construct the check does not prove kept as a rejection at its JSON pointer.

use std::collections::BTreeSet;
use std::path::PathBuf;

use proc_macro2::TokenStream;
use quote::quote;
use serde_json::{Map, Value};

use super::private;
use crate::migrations::{Format, Subject};

/// Keywords that carry no constraint.
const ANNOTATIONS: &[&str] = &[
    "title",
    "description",
    "default",
    "examples",
    "deprecated",
    "readOnly",
    "writeOnly",
    "$comment",
    "$id",
    "id",
    "$schema",
    "$defs",
    "definitions",
];

/// Keywords the check reads.
const STRUCTURAL: &[&str] = &[
    "type",
    "format",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "enum",
    "const",
    "oneOf",
    "anyOf",
    "allOf",
    "$ref",
    "minimum",
    "maximum",
];

/// One schema, as the check reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Node {
    Any,
    Never,
    Null,
    Bool,
    Integer {
        min: i128,
        max: i128,
    },
    Number {
        format: Option<String>,
    },
    String {
        format: Option<String>,
    },
    Strings(Vec<String>),
    Const {
        value: String,
        pointer: String,
    },
    Nullable(Box<Self>),
    Array(Box<Self>),
    Object(Object),
    OneOf {
        branches: Vec<Self>,
        pointer: String,
    },
    Rejected {
        keyword: String,
        pointer: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Object {
    pub(crate) properties: Vec<Property>,
    pub(crate) additional: Additional,
    pub(crate) pointer: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Property {
    pub(crate) name: String,
    pub(crate) node: Node,
    pub(crate) required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Additional {
    Open,
    Closed,
    Schema(Box<Node>),
}

/// The document a pointer is read in: the subject's newest migration.
#[derive(Clone)]
struct Document<'a> {
    subject: &'a Subject,
    root: Value,
    /// The prefix of pointers into it: empty for the checked subject, the subject's name for a
    /// referenced one.
    label: String,
}

/// Reads subjects' schemas into [`Node`]s.
pub(crate) struct Reader<'a> {
    catalog: &'a [Subject],
    /// The migrations read, which the generated code includes so an edit rebuilds the crate.
    pub(crate) files: Vec<PathBuf>,
    /// The `$ref` targets being read, to reject a cycle.
    stack: Vec<(String, String)>,
}

impl<'a> Reader<'a> {
    pub(crate) const fn new(catalog: &'a [Subject]) -> Self {
        Self {
            catalog,
            files: Vec::new(),
            stack: Vec::new(),
        }
    }

    /// The newest migration of `subject`.
    ///
    /// # Errors
    ///
    /// A migration that is not JSON, a `$ref` that resolves to nothing, a reference to a
    /// subject `subject.toml` does not declare or the catalog does not hold.
    pub(crate) fn read(&mut self, subject: &'a Subject) -> Result<Node, String> {
        let document = self.document(subject, String::new())?;
        let root = document.root.clone();
        // The root is a `$ref` target too (`"$ref": "#"`), so reading it again is a cycle.
        self.stack.push((String::new(), String::new()));
        let node = self.node(&document, &root, "");
        self.stack.pop();
        node
    }

    fn document(&mut self, subject: &'a Subject, label: String) -> Result<Document<'a>, String> {
        let newest = subject
            .newest()
            .ok_or_else(|| format!("subject `{}` has no migration", subject.name))?;
        let root = serde_json::from_str(&newest.content)
            .map_err(|err| format!("migration `{}` is not JSON: {err}", newest.path.display()))?;
        self.files.push(newest.path.clone());
        Ok(Document {
            subject,
            root,
            label,
        })
    }

    fn node(&mut self, doc: &Document<'a>, value: &Value, pointer: &str) -> Result<Node, String> {
        let map = match value {
            Value::Bool(true) => return Ok(Node::Any),
            Value::Bool(false) => return Ok(Node::Never),
            Value::Object(map) => map,
            _ => {
                return Err(format!(
                    "`{}` is not a schema: a schema is an object or a boolean",
                    at(doc, pointer)
                ));
            }
        };
        if let Some(keyword) = map
            .keys()
            .find(|key| !ANNOTATIONS.contains(&key.as_str()) && !STRUCTURAL.contains(&key.as_str()))
        {
            return Ok(rejected(doc, keyword, &child(pointer, keyword)));
        }
        if let Some(reference) = map.get("$ref") {
            let target = self.reference(doc, reference, &child(pointer, "$ref"))?;
            return self.beside(doc, map, "$ref", target, pointer);
        }
        if let Some(all) = map.get("allOf") {
            let keyword_at = child(pointer, "allOf");
            let target = match all.as_array().map(Vec::as_slice) {
                Some([only]) => self.node(doc, only, &child(&keyword_at, "0"))?,
                _ => return Ok(rejected(doc, "allOf", &keyword_at)),
            };
            return self.beside(doc, map, "allOf", target, pointer);
        }
        if let Some(any) = map.get("anyOf") {
            return self.any_of(doc, map, any, pointer);
        }
        if let Some(one) = map.get("oneOf") {
            return self.one_of(doc, map, one, pointer);
        }
        self.typed(doc, map, pointer)
    }

    /// The schema `keyword` resolved to, combined with the keywords beside it.
    fn beside(
        &mut self,
        doc: &Document<'a>,
        map: &Map<String, Value>,
        keyword: &str,
        target: Node,
        pointer: &str,
    ) -> Result<Node, String> {
        let rest: Map<String, Value> = map
            .iter()
            .filter(|(key, _)| key.as_str() != keyword && !ANNOTATIONS.contains(&key.as_str()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if rest.is_empty() {
            return Ok(target);
        }
        let sibling = self.node(doc, &Value::Object(rest), pointer)?;
        Ok(merge(target, sibling)
            .unwrap_or_else(|| rejected(doc, keyword, &child(pointer, keyword))))
    }

    fn any_of(
        &mut self,
        doc: &Document<'a>,
        map: &Map<String, Value>,
        any: &Value,
        pointer: &str,
    ) -> Result<Node, String> {
        let keyword_at = child(pointer, "anyOf");
        if has_siblings(map, "anyOf") {
            return Ok(rejected(doc, "anyOf", &keyword_at));
        }
        if let Some([first, second]) = any.as_array().map(Vec::as_slice) {
            let (inner, index) = match (is_null(first), is_null(second)) {
                (false, true) => (first, "0"),
                (true, false) => (second, "1"),
                _ => return Ok(rejected(doc, "anyOf", &keyword_at)),
            };
            let inner = self.node(doc, inner, &child(&keyword_at, index))?;
            return Ok(nullable(inner));
        }
        Ok(rejected(doc, "anyOf", &keyword_at))
    }

    fn one_of(
        &mut self,
        doc: &Document<'a>,
        map: &Map<String, Value>,
        one: &Value,
        pointer: &str,
    ) -> Result<Node, String> {
        let keyword_at = child(pointer, "oneOf");
        let allowed_beside = map.iter().all(|(key, value)| {
            key == "oneOf"
                || ANNOTATIONS.contains(&key.as_str())
                || (key == "type" && value == "object")
        });
        let Some(branches) = one.as_array().filter(|_| allowed_beside) else {
            return Ok(rejected(doc, "oneOf", &keyword_at));
        };
        let branches = branches
            .iter()
            .enumerate()
            .map(|(index, branch)| self.node(doc, branch, &child(&keyword_at, &index.to_string())))
            .collect::<Result<_, _>>()?;
        Ok(Node::OneOf {
            branches,
            pointer: at(doc, &keyword_at),
        })
    }

    fn typed(
        &mut self,
        doc: &Document<'a>,
        map: &Map<String, Value>,
        pointer: &str,
    ) -> Result<Node, String> {
        let mut types = match map.get("type") {
            None => Vec::new(),
            Some(Value::String(ty)) => vec![ty.as_str()],
            Some(Value::Array(list)) => {
                let mut types = Vec::new();
                for ty in list {
                    match ty.as_str() {
                        Some(ty) => types.push(ty),
                        None => return Ok(rejected(doc, "type", &child(pointer, "type"))),
                    }
                }
                types
            }
            Some(_) => return Ok(rejected(doc, "type", &child(pointer, "type"))),
        };
        let null = types.contains(&"null");
        types.retain(|ty| *ty != "null");
        if let Some(values) = map.get("enum") {
            let Some(strings) = enum_strings(values) else {
                return Ok(rejected(doc, "enum", &child(pointer, "enum")));
            };
            let (strings, null_listed) = strings;
            let node = Node::Strings(strings);
            return Ok(if null || null_listed {
                nullable(node)
            } else {
                node
            });
        }
        if let Some(value) = map.get("const") {
            let Some(value) = value.as_str() else {
                return Ok(rejected(doc, "const", &child(pointer, "const")));
            };
            return Ok(Node::Const {
                value: value.to_owned(),
                pointer: at(doc, &child(pointer, "const")),
            });
        }
        let kind = match types.as_slice() {
            [] if null => "null",
            [] => infer(map),
            [one] => one,
            _ => return Ok(rejected(doc, "type", &child(pointer, "type"))),
        };
        let node = match kind {
            "null" => Node::Null,
            "boolean" => Node::Bool,
            "integer" => integer(doc, map, pointer),
            "number" => number(doc, map, pointer),
            "string" => Node::String {
                format: map.get("format").and_then(Value::as_str).map(str::to_owned),
            },
            "array" => self.array(doc, map, pointer)?,
            "object" => self.object(doc, map, pointer)?,
            "any" => {
                if map.contains_key("minimum") {
                    return Ok(rejected(doc, "minimum", &child(pointer, "minimum")));
                }
                if map.contains_key("maximum") {
                    return Ok(rejected(doc, "maximum", &child(pointer, "maximum")));
                }
                Node::Any
            }
            other => {
                return Err(format!(
                    "`{}` declares the unknown type `{other}`",
                    at(doc, pointer)
                ));
            }
        };
        Ok(if null && kind != "null" {
            nullable(node)
        } else {
            node
        })
    }

    fn array(
        &mut self,
        doc: &Document<'a>,
        map: &Map<String, Value>,
        pointer: &str,
    ) -> Result<Node, String> {
        let items = match map.get("items") {
            None => Node::Any,
            Some(Value::Array(_)) => return Ok(rejected(doc, "items", &child(pointer, "items"))),
            Some(items) => self.node(doc, items, &child(pointer, "items"))?,
        };
        Ok(Node::Array(Box::new(items)))
    }

    fn object(
        &mut self,
        doc: &Document<'a>,
        map: &Map<String, Value>,
        pointer: &str,
    ) -> Result<Node, String> {
        let required: BTreeSet<&str> = match map.get("required") {
            None => BTreeSet::new(),
            Some(Value::Array(names)) => names.iter().filter_map(Value::as_str).collect(),
            Some(_) => return Ok(rejected(doc, "required", &child(pointer, "required"))),
        };
        let mut properties = Vec::new();
        if let Some(declared) = map.get("properties") {
            let Some(declared) = declared.as_object() else {
                return Ok(rejected(doc, "properties", &child(pointer, "properties")));
            };
            let at_properties = child(pointer, "properties");
            for (name, schema) in declared {
                properties.push(Property {
                    name: name.clone(),
                    node: self.node(doc, schema, &child(&at_properties, name))?,
                    required: required.contains(name.as_str()),
                });
            }
        }
        for name in &required {
            if !properties.iter().any(|property| property.name == *name) {
                properties.push(Property {
                    name: (*name).to_owned(),
                    node: Node::Any,
                    required: true,
                });
            }
        }
        let additional = match map.get("additionalProperties") {
            None | Some(Value::Bool(true)) => Additional::Open,
            Some(Value::Bool(false)) => Additional::Closed,
            Some(schema) => Additional::Schema(Box::new(self.node(
                doc,
                schema,
                &child(pointer, "additionalProperties"),
            )?)),
        };
        Ok(Node::Object(Object {
            properties,
            additional,
            pointer: at(doc, pointer),
        }))
    }

    /// The schema a `$ref` names: a pointer into the same migration, or into the newest
    /// migration of a subject the `[[references]]` of `subject.toml` name.
    fn reference(
        &mut self,
        doc: &Document<'a>,
        reference: &Value,
        pointer: &str,
    ) -> Result<Node, String> {
        let Some(reference) = reference.as_str() else {
            return Err(format!("`{}` is not a string", at(doc, pointer)));
        };
        let (name, fragment) = reference.split_once('#').unwrap_or((reference, ""));
        let target_doc = if name.is_empty() {
            doc.clone()
        } else {
            let declared = doc
                .subject
                .settings
                .references
                .iter()
                .find(|declared| declared.name == name)
                .ok_or_else(|| {
                    format!(
                        "`{}` refers to `{name}`, which the `[[references]]` of subject `{}` do \
                         not name",
                        at(doc, pointer),
                        doc.subject.name
                    )
                })?;
            let subject = self
                .catalog
                .iter()
                .find(|subject| subject.name == declared.subject)
                .ok_or_else(|| {
                    format!(
                        "subject `{}` refers to subject `{}`, which `schemas/` does not hold",
                        doc.subject.name, declared.subject
                    )
                })?;
            if subject.settings.format != Format::Json {
                return Err(format!(
                    "subject `{}` refers to subject `{}`, which is not JSON Schema",
                    doc.subject.name, subject.name
                ));
            }
            self.document(subject, subject.name.clone())?
        };
        let key = (target_doc.label.clone(), fragment.to_owned());
        if self.stack.contains(&key) {
            return Ok(rejected(doc, "$ref", pointer));
        }
        let target = target_doc.root.pointer(fragment).cloned().ok_or_else(|| {
            format!(
                "`{}` refers to `{reference}`, which resolves to nothing",
                at(doc, pointer)
            )
        })?;
        self.stack.push(key);
        let node = self.node(&target_doc, &target, fragment);
        self.stack.pop();
        node
    }
}

/// The JSON pointer of `key` inside `pointer`.
fn child(pointer: &str, key: &str) -> String {
    format!("{pointer}/{}", key.replace('~', "~0").replace('/', "~1"))
}

/// The pointer as a message shows it: `#/properties/note` in the checked subject,
/// `items-value#/properties/sku` in a referenced one.
fn at(doc: &Document<'_>, pointer: &str) -> String {
    format!("{}#{pointer}", doc.label)
}

fn rejected(doc: &Document<'_>, keyword: &str, pointer: &str) -> Node {
    Node::Rejected {
        keyword: keyword.to_owned(),
        pointer: at(doc, pointer),
    }
}

fn nullable(node: Node) -> Node {
    match node {
        Node::Nullable(_) | Node::Null | Node::Any => node,
        node => Node::Nullable(Box::new(node)),
    }
}

fn has_siblings(map: &Map<String, Value>, keyword: &str) -> bool {
    map.keys()
        .any(|key| key != keyword && !ANNOTATIONS.contains(&key.as_str()))
}

fn is_null(schema: &Value) -> bool {
    schema.as_object().is_some_and(|map| {
        map.get("type").and_then(Value::as_str) == Some("null") && !has_siblings(map, "type")
    })
}

/// The strings of an `enum`, and whether it lists `null` too; `None` when it lists anything else.
fn enum_strings(values: &Value) -> Option<(Vec<String>, bool)> {
    let mut strings = Vec::new();
    let mut null = false;
    for value in values.as_array()? {
        match value {
            Value::String(text) => strings.push(text.clone()),
            Value::Null => null = true,
            _ => return None,
        }
    }
    Some((strings, null))
}

/// The type an untyped schema describes by its keywords.
fn infer(map: &Map<String, Value>) -> &'static str {
    if ["properties", "required", "additionalProperties"]
        .iter()
        .any(|key| map.contains_key(*key))
    {
        "object"
    } else if map.contains_key("items") {
        "array"
    } else {
        "any"
    }
}

fn integer(doc: &Document<'_>, map: &Map<String, Value>, pointer: &str) -> Node {
    let (mut min, mut max) = match map.get("format").and_then(Value::as_str) {
        None | Some("int64" | "int") => (i128::from(i64::MIN), i128::from(i64::MAX)),
        Some("int8") => (i128::from(i8::MIN), i128::from(i8::MAX)),
        Some("int16") => (i128::from(i16::MIN), i128::from(i16::MAX)),
        Some("int32") => (i128::from(i32::MIN), i128::from(i32::MAX)),
        Some("uint8") => (0, i128::from(u8::MAX)),
        Some("uint16") => (0, i128::from(u16::MAX)),
        Some("uint32") => (0, i128::from(u32::MAX)),
        Some("uint64" | "uint") => (0, i128::from(u64::MAX)),
        Some(_) => return rejected(doc, "format", &child(pointer, "format")),
    };
    for (keyword, lower) in [("minimum", true), ("maximum", false)] {
        let Some(bound) = map.get(keyword) else {
            continue;
        };
        let Some(bound) = whole(bound) else {
            return rejected(doc, keyword, &child(pointer, keyword));
        };
        if lower {
            min = min.max(bound);
        } else {
            max = max.min(bound);
        }
    }
    Node::Integer { min, max }
}

/// A JSON number that is a whole number.
fn whole(value: &Value) -> Option<i128> {
    if let Some(value) = value.as_i64() {
        return Some(i128::from(value));
    }
    if let Some(value) = value.as_u64() {
        return Some(i128::from(value));
    }
    let value = value.as_f64()?;
    // Bounds past 2^63 lose nothing a 64-bit integer type could tell apart.
    #[allow(clippy::cast_possible_truncation)]
    let whole = value as i128;
    // Exact equality is the question: whether the bound has no fractional part.
    #[allow(clippy::cast_precision_loss, clippy::float_cmp)]
    let exact = whole as f64 == value;
    exact.then_some(whole)
}

fn number(doc: &Document<'_>, map: &Map<String, Value>, pointer: &str) -> Node {
    for keyword in ["minimum", "maximum"] {
        if map.contains_key(keyword) {
            return rejected(doc, keyword, &child(pointer, keyword));
        }
    }
    match map.get("format").and_then(Value::as_str) {
        None => Node::Number { format: None },
        Some(format @ ("float" | "double")) => Node::Number {
            format: Some(format.to_owned()),
        },
        Some(_) => rejected(doc, "format", &child(pointer, "format")),
    }
}

/// Two schemas that hold at once: `$ref` or a single `allOf` beside other keywords. Objects
/// combine when neither restricts additional properties; anything else does not.
fn merge(left: Node, right: Node) -> Option<Node> {
    match (left, right) {
        (Node::Any, node) | (node, Node::Any) => Some(node),
        (Node::Object(left), Node::Object(right)) => {
            if left.additional != Additional::Open || right.additional != Additional::Open {
                return None;
            }
            let mut properties = right.properties;
            for property in left.properties {
                match properties
                    .iter_mut()
                    .find(|other| other.name == property.name)
                {
                    Some(other) if other.node == Node::Any => {
                        other.node = property.node;
                        other.required |= property.required;
                    }
                    Some(_) => return None,
                    None => properties.push(property),
                }
            }
            Some(Node::Object(Object {
                properties,
                additional: Additional::Open,
                pointer: right.pointer,
            }))
        }
        _ => None,
    }
}

impl Node {
    /// The constant of this schema.
    pub(crate) fn tokens(&self) -> TokenStream {
        let private = private();
        match self {
            Self::Any => quote!(#private::Node::Any),
            Self::Never => quote!(#private::Node::Never),
            Self::Null => quote!(#private::Node::Null),
            Self::Bool => quote!(#private::Node::Bool),
            Self::Integer { min, max } => {
                quote!(#private::Node::Integer { min: #min, max: #max })
            }
            Self::Number { format } => {
                let format = option(format.as_deref());
                quote!(#private::Node::Number { format: #format })
            }
            Self::String { format } => {
                let format = option(format.as_deref());
                quote!(#private::Node::String { format: #format })
            }
            Self::Strings(values) => quote!(#private::Node::Strings(&[#(#values),*])),
            Self::Const { value, pointer } => {
                quote!(#private::Node::Const { value: #value, pointer: #pointer })
            }
            Self::Nullable(inner) => {
                let inner = inner.tokens();
                quote!(#private::Node::Nullable(&#inner))
            }
            Self::Array(items) => {
                let items = items.tokens();
                quote!(#private::Node::Array(&#items))
            }
            Self::Object(object) => {
                let properties = object.properties.iter().map(|property| {
                    let name = &property.name;
                    let node = property.node.tokens();
                    let required = property.required;
                    quote!(#private::Property { name: #name, node: &#node, required: #required })
                });
                let additional = match &object.additional {
                    Additional::Open => quote!(#private::Additional::Open),
                    Additional::Closed => quote!(#private::Additional::Closed),
                    Additional::Schema(node) => {
                        let node = node.tokens();
                        quote!(#private::Additional::Schema(&#node))
                    }
                };
                let pointer = &object.pointer;
                quote! {
                    #private::Node::Object(&#private::Object {
                        properties: &[#(#properties),*],
                        additional: #additional,
                        pointer: #pointer,
                    })
                }
            }
            Self::OneOf { branches, pointer } => {
                let branches = branches.iter().map(Self::tokens);
                quote!(#private::Node::OneOf { branches: &[#(#branches),*], pointer: #pointer })
            }
            Self::Rejected { keyword, pointer } => {
                quote!(#private::Node::Rejected { keyword: #keyword, pointer: #pointer })
            }
        }
    }
}

fn option(text: Option<&str>) -> TokenStream {
    text.map_or_else(
        || quote!(::core::option::Option::None),
        |text| quote!(::core::option::Option::Some(#text)),
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::migrations::{Migration, Owner, Reference, Settings};

    fn subject(name: &str, schema: &str, references: Vec<Reference>) -> Subject {
        Subject {
            name: name.to_owned(),
            settings: Settings {
                format: Format::Json,
                compatibility: None,
                owner: Owner::Service,
                record: Some("com.acme.Order".to_owned()),
                source: None,
                references,
            },
            migrations: vec![Migration {
                number: 1,
                stem: "0001_init".to_owned(),
                path: PathBuf::from(format!("{name}/0001_init.json")),
                content: schema.to_owned(),
            }],
        }
    }

    fn read(schema: &str) -> Result<Node, String> {
        let catalog = [subject("orders-value", schema, Vec::new())];
        Reader::new(&catalog).read(&catalog[0])
    }

    fn property(node: &Node, name: &str) -> Node {
        let Node::Object(object) = node else {
            panic!("an object: {node:?}");
        };
        object
            .properties
            .iter()
            .find(|property| property.name == name)
            .map_or_else(
                || panic!("property `{name}` in {object:?}"),
                |property| property.node.clone(),
            )
    }

    #[test]
    fn a_ref_beside_properties_joins_them() {
        let node = read(
            r##"{"type": "object", "properties": {"kind": {"const": "Store"}}, "required": ["kind"],
                "$ref": "#/$defs/Store",
                "$defs": {"Store": {"type": "object", "properties": {"id": {"type": "integer", "format": "uint16"}}, "required": ["id"]}}}"##,
        )
        .expect("the schema reads");
        assert_eq!(property(&node, "id"), Node::Integer { min: 0, max: 65535 });
        assert!(matches!(property(&node, "kind"), Node::Const { .. }));
    }

    #[test]
    fn a_ref_beside_a_closed_object_is_rejected() {
        let node = read(
            r##"{"type": "object", "properties": {"kind": {"const": "Store"}},
                "$ref": "#/definitions/Store",
                "definitions": {"Store": {"type": "object", "properties": {}, "additionalProperties": false}}}"##,
        )
        .expect("the schema reads");
        assert_eq!(
            node,
            Node::Rejected {
                keyword: "$ref".to_owned(),
                pointer: "#/$ref".to_owned()
            }
        );
    }

    #[test]
    fn null_joins_a_type_through_three_spellings() {
        let node = read(
            r#"{"type": "object", "properties": {
                "a": {"type": ["string", "null"]},
                "b": {"anyOf": [{"type": "null"}, {"type": "boolean"}]},
                "c": {"enum": ["x", null]}}}"#,
        )
        .expect("the schema reads");
        assert_eq!(
            property(&node, "a"),
            Node::Nullable(Box::new(Node::String { format: None }))
        );
        assert_eq!(property(&node, "b"), Node::Nullable(Box::new(Node::Bool)));
        assert_eq!(
            property(&node, "c"),
            Node::Nullable(Box::new(Node::Strings(vec!["x".to_owned()])))
        );
    }

    #[test]
    fn an_integer_range_narrows_by_minimum_and_maximum() {
        let node = read(
            r#"{"properties": {
                "a": {"type": "integer", "format": "uint32", "minimum": 1},
                "b": {"type": "integer", "maximum": 10.0},
                "c": {"type": "integer", "minimum": 0.5},
                "d": {"type": "integer", "format": "int128"}}}"#,
        )
        .expect("the schema reads");
        assert_eq!(
            property(&node, "a"),
            Node::Integer {
                min: 1,
                max: i128::from(u32::MAX)
            }
        );
        assert_eq!(
            property(&node, "b"),
            Node::Integer {
                min: i128::from(i64::MIN),
                max: 10
            }
        );
        assert_eq!(
            property(&node, "c"),
            Node::Rejected {
                keyword: "minimum".to_owned(),
                pointer: "#/properties/c/minimum".to_owned()
            }
        );
        assert_eq!(
            property(&node, "d"),
            Node::Rejected {
                keyword: "format".to_owned(),
                pointer: "#/properties/d/format".to_owned()
            }
        );
    }

    #[test]
    fn constraints_and_unknown_keywords_are_rejected_where_they_stand() {
        let node = read(
            r#"{"properties": {
                "a": {"type": "number", "minimum": 0},
                "b": {"type": "array", "items": [{"type": "string"}]},
                "c": {"type": "string", "x-custom": true},
                "d": {"type": ["string", "integer"]}},
                "required": ["e"]}"#,
        )
        .expect("the schema reads");
        for (name, keyword) in [
            ("a", "minimum"),
            ("b", "items"),
            ("c", "x-custom"),
            ("d", "type"),
        ] {
            assert_eq!(
                property(&node, name),
                Node::Rejected {
                    keyword: keyword.to_owned(),
                    pointer: format!("#/properties/{name}/{keyword}")
                },
                "{name}"
            );
        }
        assert_eq!(
            property(&node, "e"),
            Node::Any,
            "a required name alone admits any value"
        );
    }

    #[test]
    fn a_ref_that_resolves_to_nothing_is_an_error() {
        let err = read(r##"{"properties": {"a": {"$ref": "#/$defs/Missing"}}}"##)
            .expect_err("the target is missing");
        assert!(
            err.contains("`#/$defs/Missing`, which resolves to nothing"),
            "{err}"
        );
    }

    #[test]
    fn a_ref_to_another_subject_needs_a_declared_reference() {
        let err = read(r#"{"properties": {"a": {"$ref": "item.json"}}}"#)
            .expect_err("the reference is not declared");
        assert!(
            err.contains("which the `[[references]]` of subject `orders-value` do not name"),
            "{err}"
        );
        let catalog = [
            subject(
                "orders-value",
                r#"{"properties": {"a": {"$ref": "item.json#/properties/sku"}}}"#,
                vec![Reference {
                    name: "item.json".to_owned(),
                    subject: "items-value".to_owned(),
                }],
            ),
            subject(
                "items-value",
                r#"{"properties": {"sku": {"type": "string", "pattern": "x"}}}"#,
                Vec::new(),
            ),
        ];
        let mut reader = Reader::new(&catalog);
        let node = reader.read(&catalog[0]).expect("the reference resolves");
        assert_eq!(
            property(&node, "a"),
            Node::Rejected {
                keyword: "pattern".to_owned(),
                pointer: "items-value#/properties/sku/pattern".to_owned()
            }
        );
        assert_eq!(
            reader.files.len(),
            2,
            "both migrations rebuild the crate when they change"
        );
    }
}
