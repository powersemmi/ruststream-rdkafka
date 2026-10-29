//! The comparison of a type's serde shape with a subject's schema, as a `const fn` the compiler
//! evaluates: the first difference is the error, with the path to it.

use self::object::object_value;
use self::variants::enum_value;
use super::message::{Message, Path, Step};
use super::node::{Additional, Node, Object, str_eq};
use super::shape::{Field, Shape, Tagging};

/// Stops at the first `Err`: the `?` of a `const fn`.
macro_rules! check {
    ($result:expr) => {
        if let Err(message) = $result {
            return Err(message);
        }
    };
}

mod object;
mod variants;

/// Which claims the check proves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Direction {
    /// Every value the type serializes is valid under the schema.
    pub writes: bool,
    /// Every value the schema admits deserializes into the type.
    pub reads: bool,
}

impl Direction {
    /// An owned subject: the service writes and reads it.
    pub const OWNED: Self = Self {
        writes: true,
        reads: true,
    };

    /// An external subject: the service only reads it.
    pub const EXTERNAL: Self = Self {
        writes: false,
        reads: true,
    };

    const fn narrow(self, writes: bool, reads: bool) -> Self {
        Self {
            writes: self.writes && writes,
            reads: self.reads && reads,
        }
    }

    const fn reads_only(self) -> Self {
        self.narrow(false, true)
    }

    const fn is_none(self) -> bool {
        !self.writes && !self.reads
    }
}

/// What is checked against what: the names the message carries and the claims to prove.
#[derive(Debug, Clone, Copy)]
pub struct Check {
    /// The checked type.
    pub type_name: &'static str,
    /// The subject whose newest migration is the schema.
    pub subject: &'static str,
    /// That migration (`0001_create_order`).
    pub migration: &'static str,
    /// The claims.
    pub direction: Direction,
}

/// Compares `shape` with `schema`; the error is the whole message, ready for `panic!`.
///
/// # Errors
///
/// The first difference, with the path from the checked type to it.
pub const fn compare(shape: &Shape, schema: &Node, check: &Check) -> Result<(), Message> {
    let ctx = Ctx { check };
    let path = Path::root(check.type_name);
    value(&ctx, shape, schema, check.direction, &path)
}

struct Ctx<'a> {
    check: &'a Check,
}

impl Ctx<'_> {
    /// The start of a message about `path`.
    const fn at(&self, path: &Path<'_>) -> Message {
        let message = Message::new()
            .code(self.check.type_name)
            .push(" does not match subject ")
            .code(self.check.subject)
            .push(" (migration ")
            .push(self.check.migration)
            .push("): ");
        path.write(message).push(": ")
    }
}

/// The fields of one JSON object as serde writes and reads it.
#[derive(Clone, Copy)]
struct Fields<'a> {
    /// The type the object belongs to, for messages.
    owner: &'a str,
    /// The tag property of an internally tagged variant.
    tag: Option<&'a str>,
    list: &'a [Field],
    /// `#[serde(deny_unknown_fields)]` of the type.
    denies_unknown: bool,
}

const fn value(
    ctx: &Ctx<'_>,
    shape: &Shape,
    node: &Node,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    if dir.is_none() {
        return Ok(());
    }
    match shape {
        Shape::Unchecked { .. } => return Ok(()),
        Shape::Opaque(reason) => {
            return Err(ctx
                .at(path)
                .push("its serde shape is not visible to the check (")
                .push(reason)
                .push("); mark the field #[schema(unchecked)] to check only its presence and optionality"));
        }
        _ => {}
    }
    if let Node::Rejected { keyword, pointer } = node {
        return Err(rejected(ctx, path, keyword, pointer));
    }
    if let Node::Const { pointer, .. } = node {
        return Err(rejected(ctx, path, "const", pointer));
    }
    if let Shape::Enum(shape) = shape
        && matches!(shape.tagging, Tagging::Untagged)
    {
        return Err(ctx
            .at(path)
            .code(shape.name)
            .push(" is #[serde(untagged)]: serde takes the first variant that fits, while oneOf demands exactly one match, so the two can disagree on valid data; tag the enum or mark the field #[schema(unchecked)]"));
    }
    kinds(ctx, shape, node, dir, path)
}

/// The value against the schema once neither is unchecked, opaque or rejected.
const fn kinds(
    ctx: &Ctx<'_>,
    shape: &Shape,
    node: &Node,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    match (shape, node) {
        (Shape::Any, _) => {
            if dir.writes && !admits_anything(node) {
                return Err(ctx
                    .at(path)
                    .push("the field holds any JSON value (`serde_json::Value`), and the schema admits only ")
                    .push(node_kind(node)));
            }
            Ok(())
        }
        (_, Node::Any) => {
            if dir.reads {
                return Err(ctx
                    .at(path)
                    .push("the schema admits any value, and the field reads only ")
                    .push(shape_kind(shape)));
            }
            Ok(())
        }
        (_, Node::Never) => {
            if dir.writes {
                return Err(ctx
                    .at(path)
                    .push("the schema admits no value (`false`), and the field writes one"));
            }
            Ok(())
        }
        (Shape::Option(inner), Node::Nullable(node)) => value(ctx, inner, node, dir, path),
        (Shape::Option(inner), _) => {
            if dir.writes {
                return Err(ctx.at(path).push(
                    "the field is an Option and writes null, which the schema does not admit",
                ));
            }
            value(ctx, inner, node, dir, path)
        }
        (_, Node::Nullable(node)) => {
            if dir.reads {
                return Err(ctx
                    .at(path)
                    .push("the schema admits null, and the field is not an Option"));
            }
            value(ctx, shape, node, dir, path)
        }
        (
            Shape::Bool | Shape::Integer { .. } | Shape::Number { .. } | Shape::String { .. },
            Node::Bool | Node::Integer { .. } | Node::Number { .. } | Node::String { .. },
        ) => scalar(ctx, shape, node, dir, path),
        (Shape::String { .. }, Node::Strings(_)) => {
            if dir.writes {
                return Err(ctx
                    .at(path)
                    .push("the field writes any string, and the schema admits only the strings of its enum"));
            }
            Ok(())
        }
        (Shape::Enum(shape), _) => enum_value(ctx, shape, node, dir, path),
        (Shape::Seq(inner), Node::Array(items)) => {
            value(ctx, inner, items, dir, &path.then(Step::Item))
        }
        (Shape::Map(inner), Node::Object(object)) => map_value(ctx, inner, object, dir, path),
        (Shape::Struct(shape), Node::Object(object)) => {
            let fields = Fields {
                owner: shape.name,
                tag: None,
                list: shape.fields,
                denies_unknown: shape.deny_unknown_fields,
            };
            object_value(ctx, fields, object, dir, path)
        }
        _ => Err(mismatch(ctx, shape, node, path)),
    }
}

/// Booleans, integers, floats and strings.
const fn scalar(
    ctx: &Ctx<'_>,
    shape: &Shape,
    node: &Node,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    match (shape, node) {
        (Shape::Bool, Node::Bool) => Ok(()),
        (
            Shape::Integer { name, min, max },
            Node::Integer {
                min: low,
                max: high,
            },
        ) => {
            if dir.writes && (*min < *low || *max > *high) {
                return Err(range(
                    ctx.at(path)
                        .push("the integer type writes values outside the schema's range ("),
                    name,
                    (*min, *max),
                    (*low, *high),
                ));
            }
            if dir.reads && (*low < *min || *high > *max) {
                return Err(range(
                    ctx.at(path)
                        .push("the schema admits integers outside the integer type's range ("),
                    name,
                    (*min, *max),
                    (*low, *high),
                ));
            }
            Ok(())
        }
        (Shape::Number { format }, Node::Number { format: declared }) => {
            let declared = match declared {
                Some(declared) => declared,
                None => "double",
            };
            if !str_eq(format, declared) {
                return Err(ctx
                    .at(path)
                    .push("the float type has the number format ")
                    .code(format)
                    .push(", and the schema declares ")
                    .code(declared));
            }
            Ok(())
        }
        (Shape::String { format }, Node::String { format: declared }) => {
            if !option_eq(*format, *declared) {
                let message = ctx
                    .at(path)
                    .push("the string format differs: the type has ");
                let message = format_name(message, *format).push(", the schema declares ");
                return Err(format_name(message, *declared));
            }
            Ok(())
        }
        _ => Err(mismatch(ctx, shape, node, path)),
    }
}

const fn mismatch(ctx: &Ctx<'_>, shape: &Shape, node: &Node, path: &Path<'_>) -> Message {
    ctx.at(path)
        .push("the field's type is ")
        .push(shape_kind(shape))
        .push(", and the schema declares ")
        .push(node_kind(node))
}

const fn rejected(ctx: &Ctx<'_>, path: &Path<'_>, keyword: &str, pointer: &str) -> Message {
    ctx.at(path)
        .push("the schema uses ")
        .code(keyword)
        .push(" at ")
        .code(pointer)
        .push(", which a Rust type cannot promise; mark the field #[schema(unchecked)] to check only its presence and optionality")
}

const fn range(message: Message, name: &str, own: (i128, i128), declared: (i128, i128)) -> Message {
    message
        .code(name)
        .push(" holds ")
        .int(own.0)
        .push("..=")
        .int(own.1)
        .push(", the schema admits ")
        .int(declared.0)
        .push("..=")
        .int(declared.1)
        .push(")")
}

const fn map_value(
    ctx: &Ctx<'_>,
    inner: &Shape,
    object: &Object,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    if !object.properties.is_empty() {
        return Err(ctx
            .at(path)
            .push("the field is a map, and the schema's object at ")
            .code(object.pointer)
            .push(" declares properties; a map is checked against an object that declares none"));
    }
    let values = path.then(Step::Value);
    match object.additional {
        Additional::Schema(node) => value(ctx, inner, node, dir, &values),
        Additional::Open => value(ctx, inner, &Node::Any, dir, &values),
        Additional::Closed => {
            if dir.writes {
                return Err(ctx
                    .at(path)
                    .push("the schema's object is closed and declares no properties, and the map writes some"));
            }
            Ok(())
        }
    }
}

const fn contains(names: &[&str], name: &str) -> bool {
    let mut index = 0;
    while index < names.len() {
        if str_eq(names[index], name) {
            return true;
        }
        index += 1;
    }
    false
}

const fn option_eq(left: Option<&str>, right: Option<&str>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => str_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

const fn format_name(message: Message, format: Option<&str>) -> Message {
    match format {
        Some(format) => message.code(format),
        None => message.push("no format"),
    }
}

const fn admits_anything(node: &Node) -> bool {
    match node {
        Node::Any => true,
        Node::Nullable(inner) => admits_anything(inner),
        _ => false,
    }
}

const fn shape_kind(shape: &Shape) -> &str {
    match shape {
        Shape::Bool => "a boolean",
        Shape::Integer { name, .. } => name,
        Shape::Number { .. } => "a float",
        Shape::String { .. } => "a string",
        Shape::Any => "any JSON value",
        Shape::Option(_) => "an Option",
        Shape::Seq(_) => "a sequence",
        Shape::Map(_) => "a map",
        Shape::Struct(shape) => shape.name,
        Shape::Enum(shape) => shape.name,
        Shape::Opaque(_) | Shape::Unchecked { .. } => "unchecked",
    }
}

const fn node_kind(node: &Node) -> &'static str {
    match node {
        Node::Any => "any value",
        Node::Never => "no value",
        Node::Null => "null",
        Node::Bool => "a boolean",
        Node::Integer { .. } => "an integer",
        Node::Number { .. } => "a number",
        Node::String { .. } => "a string",
        Node::Strings(_) | Node::Const { .. } => "an enum of strings",
        Node::Nullable(_) => "a nullable value",
        Node::Array(_) => "an array",
        Node::Object(_) => "an object",
        Node::OneOf { .. } => "a oneOf",
        Node::Rejected { .. } => "a rejected construct",
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::schema::json::node::{Additional, Object, Property};

    /// A direction that proves the first claim alone, which no subject asks for and every
    /// branch still has to answer.
    const WRITES: Direction = Direction {
        writes: true,
        reads: false,
    };

    /// The message of comparing `shape` with `node`, or `None` when they match.
    pub(in crate::schema::json) fn run(
        shape: &Shape,
        node: &Node,
        direction: Direction,
    ) -> Option<String> {
        let check = Check {
            type_name: "Order",
            subject: "orders-value",
            migration: "0001_create_order",
            direction,
        };
        compare(shape, node, &check)
            .err()
            .map(|message| message.as_str().to_owned())
    }

    /// Asserts a mismatch whose message holds `part`.
    pub(in crate::schema::json) fn fails(
        shape: &Shape,
        node: &Node,
        direction: Direction,
        part: &str,
    ) {
        let message = run(shape, node, direction).expect("the shapes differ");
        assert!(message.contains(part), "{message}");
    }

    const STRING: Shape = Shape::String { format: None };
    const I64: Shape = Shape::integer("i64", -9_223_372_036_854_775_808, 9_223_372_036_854_775_807);

    #[test]
    fn a_message_names_the_type_the_subject_the_migration_and_the_path() {
        fails(
            &Shape::Bool,
            &Node::String { format: None },
            Direction::OWNED,
            "`Order` does not match subject `orders-value` (migration 0001_create_order): Order: \
             the field's type is a boolean, and the schema declares a string",
        );
    }

    #[test]
    fn any_value_writes_what_a_typed_schema_refuses() {
        fails(
            &Shape::Any,
            &Node::String { format: None },
            Direction::OWNED,
            "holds any JSON value",
        );
        assert_eq!(
            run(
                &Shape::Any,
                &Node::String { format: None },
                Direction::EXTERNAL
            ),
            None
        );
        assert_eq!(run(&Shape::Any, &Node::Any, Direction::OWNED), None);
    }

    #[test]
    fn a_schema_that_admits_anything_is_read_by_any_value_only() {
        fails(&STRING, &Node::Any, Direction::EXTERNAL, "admits any value");
        assert_eq!(run(&STRING, &Node::Any, WRITES), None);
    }

    #[test]
    fn a_false_schema_admits_nothing_to_write() {
        fails(&STRING, &Node::Never, Direction::OWNED, "admits no value");
        assert_eq!(run(&STRING, &Node::Never, Direction::EXTERNAL), None);
    }

    #[test]
    fn null_follows_the_direction() {
        let option = Shape::Option(&I64);
        let integer = Node::Integer {
            min: -9_223_372_036_854_775_808,
            max: 9_223_372_036_854_775_807,
        };
        fails(&option, &integer, Direction::OWNED, "writes null");
        assert_eq!(run(&option, &integer, Direction::EXTERNAL), None);
        let nullable = Node::Nullable(&Node::Integer {
            min: -9_223_372_036_854_775_808,
            max: 9_223_372_036_854_775_807,
        });
        fails(&I64, &nullable, Direction::EXTERNAL, "admits null");
        assert_eq!(run(&I64, &nullable, WRITES), None);
        assert_eq!(run(&option, &nullable, Direction::OWNED), None);
    }

    #[test]
    fn a_string_writes_more_than_an_enum_admits() {
        let names = Node::Strings(&["new", "done"]);
        fails(
            &STRING,
            &names,
            Direction::OWNED,
            "only the strings of its enum",
        );
        assert_eq!(run(&STRING, &names, Direction::EXTERNAL), None);
    }

    #[test]
    fn a_const_outside_an_enum_tag_is_rejected() {
        let node = Node::Const {
            value: "web",
            pointer: "#/properties/channel/const",
        };
        fails(
            &STRING,
            &node,
            Direction::EXTERNAL,
            "uses `const` at `#/properties/channel/const`",
        );
    }

    #[test]
    fn an_opaque_shape_names_its_reason() {
        fails(
            &Shape::Opaque("#[serde(with = ..)]"),
            &Node::Any,
            Direction::OWNED,
            "not visible to the check (#[serde(with = ..)])",
        );
    }

    #[test]
    fn a_number_without_a_format_reads_as_a_double() {
        let number = Node::Number { format: None };
        assert_eq!(
            run(
                &Shape::Number { format: "double" },
                &number,
                Direction::OWNED
            ),
            None
        );
        fails(
            &Shape::Number { format: "float" },
            &number,
            Direction::OWNED,
            "the number format `float`, and the schema declares `double`",
        );
    }

    #[test]
    fn a_map_meets_an_object_that_declares_no_properties() {
        let map = Shape::Map(&STRING);
        let declared = Node::Object(&Object {
            properties: &[Property {
                name: "a",
                node: &Node::String { format: None },
                required: false,
            }],
            additional: Additional::Open,
            pointer: "#/properties/tags",
        });
        fails(&map, &declared, Direction::OWNED, "declares properties");
        let open = Node::Object(&Object {
            properties: &[],
            additional: Additional::Open,
            pointer: "#/properties/tags",
        });
        fails(
            &map,
            &open,
            Direction::EXTERNAL,
            "Order{}: the schema admits any value",
        );
        assert_eq!(run(&Shape::Map(&Shape::Any), &open, Direction::OWNED), None);
        let closed = Node::Object(&Object {
            properties: &[],
            additional: Additional::Closed,
            pointer: "#/properties/tags",
        });
        fails(
            &map,
            &closed,
            Direction::OWNED,
            "closed and declares no properties",
        );
        assert_eq!(run(&map, &closed, Direction::EXTERNAL), None);
    }
}
