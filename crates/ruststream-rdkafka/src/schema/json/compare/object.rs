//! A struct's fields against an object schema: the properties both ways, `required`, flattened
//! structs and maps, and `additionalProperties`.

use super::{Ctx, Direction, Fields, shape_kind, value};
use crate::schema::json::message::{Message, Path, Step};
use crate::schema::json::node::{Additional, Object, str_eq};
use crate::schema::json::shape::{Field, Read, Shape, Write};

pub(super) const fn object_value(
    ctx: &Ctx<'_>,
    fields: Fields<'_>,
    object: &Object,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    check!(each_field(ctx, fields, fields.list, object, dir, path));
    let extra = extra_values(fields.list);
    let hidden = hidden_properties(fields.list);
    let mut index = 0;
    while index < object.properties.len() {
        let property = &object.properties[index];
        index += 1;
        let is_tag = match fields.tag {
            Some(tag) => str_eq(tag, property.name),
            None => false,
        };
        if is_tag || hidden || find_field(fields.list, property.name).is_some() {
            continue;
        }
        let at = path.then(Step::Property(property.name));
        if dir.writes {
            return Err(ctx
                .at(&at)
                .code(fields.owner)
                .push(" has no field ")
                .code(property.name)
                .push(", which the schema declares"));
        }
        match extra {
            Some(inner) => check!(value(ctx, inner, property.node, dir, &at)),
            None => {
                if fields.denies_unknown {
                    return Err(ctx
                        .at(&at)
                        .push("the schema declares this property, and ")
                        .code(fields.owner)
                        .push(" denies unknown fields and has no field for it"));
                }
            }
        }
    }
    additional(ctx, fields, object, extra, dir, path)
}

const fn additional(
    ctx: &Ctx<'_>,
    fields: Fields<'_>,
    object: &Object,
    extra: Option<&Shape>,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    match object.additional {
        Additional::Open => {
            if dir.reads && fields.denies_unknown {
                return Err(ctx
                    .at(path)
                    .push("the schema's object at ")
                    .code(object.pointer)
                    .push(" is open (additionalProperties absent or true), and ")
                    .code(fields.owner)
                    .push(" denies unknown fields"));
            }
            if let Some(inner) = extra
                && dir.reads
                && !matches!(inner, Shape::Any | Shape::Unchecked { .. })
            {
                return Err(ctx.at(&path.then(Step::Value)).push(
                    "the schema's object is open, so a flattened map must hold `serde_json::Value`",
                ));
            }
        }
        Additional::Closed => {
            if dir.writes && extra.is_some() {
                return Err(ctx
                    .at(path)
                    .push("the schema's object at ")
                    .code(object.pointer)
                    .push(" is closed (additionalProperties: false), and the flattened map writes properties it does not declare"));
            }
        }
        Additional::Schema(node) => {
            if let Some(inner) = extra {
                return value(ctx, inner, node, dir, &path.then(Step::Value));
            }
            if dir.reads && fields.denies_unknown {
                return Err(ctx
                    .at(path)
                    .push("the schema's object at ")
                    .code(object.pointer)
                    .push(" admits additional properties, and ")
                    .code(fields.owner)
                    .push(" denies unknown fields"));
            }
        }
    }
    Ok(())
}

/// Checks every field of `list` and of the structs it flattens against `object`.
const fn each_field(
    ctx: &Ctx<'_>,
    fields: Fields<'_>,
    list: &[Field],
    object: &Object,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    let mut index = 0;
    while index < list.len() {
        let field = &list[index];
        index += 1;
        if field.flatten {
            match field.shape {
                Shape::Struct(inner) => {
                    check!(each_field(ctx, fields, inner.fields, object, dir, path));
                }
                Shape::Map(_) | Shape::Any | Shape::Unchecked { .. } => {}
                _ => {
                    return Err(ctx
                        .at(&path.then(Step::Property(field.name)))
                        .push("the check flattens structs and maps, and this field is ")
                        .push(shape_kind(field.shape))
                        .push("; mark it #[schema(unchecked)]"));
                }
            }
            continue;
        }
        check!(one_field(ctx, fields, field, object, dir, path));
    }
    Ok(())
}

const fn one_field(
    ctx: &Ctx<'_>,
    fields: Fields<'_>,
    field: &Field,
    object: &Object,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    let at = path.then(Step::Property(field.name));
    let written = !matches!(field.write, Write::Never);
    let read = !matches!(field.read, Read::Never);
    let absent_ok = match field.read {
        Read::Default => true,
        Read::Plain => field.shape.is_option(),
        Read::Strict | Read::Never => false,
    };
    let Some(property) = object.property(field.name) else {
        if dir.writes {
            return Err(ctx
                .at(&at)
                .push("field ")
                .code(field.name)
                .push(" of ")
                .code(fields.owner)
                .push(" is not in the schema's object at ")
                .code(object.pointer));
        }
        if dir.reads && read && !absent_ok {
            return Err(absent(ctx, &at));
        }
        return Ok(());
    };
    if dir.writes && property.required && !matches!(field.write, Write::Always) {
        return Err(ctx
            .at(&at)
            .push("the schema requires it, and ")
            .code(fields.owner)
            .push(" does not write it on every value (skip_serializing_if or skip_serializing)"));
    }
    if dir.reads && read && !property.required && !absent_ok {
        return Err(absent(ctx, &at));
    }
    value(
        ctx,
        field.shape,
        property.node,
        dir.narrow(written, read),
        &at,
    )
}

const fn absent(ctx: &Ctx<'_>, path: &Path<'_>) -> Message {
    ctx.at(path).push(
        "the schema lets it be absent and the field has no default (make it an Option or add #[serde(default)])",
    )
}

/// The field called `name` in `list` or in a struct it flattens.
const fn find_field<'a>(list: &'a [Field], name: &str) -> Option<&'a Field> {
    let mut index = 0;
    while index < list.len() {
        let field = &list[index];
        index += 1;
        if field.flatten {
            if let Shape::Struct(inner) = field.shape
                && let Some(found) = find_field(inner.fields, name)
            {
                return Some(found);
            }
        } else if str_eq(field.name, name) {
            return Some(field);
        }
    }
    None
}

/// The value shape of a flattened map in `list` or in a struct it flattens.
const fn extra_values(list: &[Field]) -> Option<&'static Shape> {
    let mut index = 0;
    while index < list.len() {
        let field = &list[index];
        index += 1;
        if !field.flatten {
            continue;
        }
        match field.shape {
            Shape::Map(inner) => return Some(inner),
            Shape::Any => return Some(&Shape::Any),
            Shape::Struct(inner) => {
                if let Some(found) = extra_values(inner.fields) {
                    return Some(found);
                }
            }
            _ => {}
        }
    }
    None
}

/// Whether a flattened field is unchecked: its properties are then not known.
const fn hidden_properties(list: &[Field]) -> bool {
    let mut index = 0;
    while index < list.len() {
        let field = &list[index];
        index += 1;
        if !field.flatten {
            continue;
        }
        match field.shape {
            Shape::Unchecked { .. } => return true,
            Shape::Struct(inner) if hidden_properties(inner.fields) => return true,
            _ => {}
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::super::tests::{fails, run};
    use super::*;
    use crate::schema::json::node::{Node, Property};
    use crate::schema::json::shape::Struct;

    const STRING: Shape = Shape::String { format: None };
    const NODE_STRING: Node = Node::String { format: None };

    macro_rules! field {
        ($name:literal, $shape:expr $(, $key:ident: $value:expr)*) => {
            Field {
                $($key: $value,)*
                ..Field {
                    name: $name,
                    shape: $shape,
                    write: Write::Always,
                    read: Read::Plain,
                    flatten: false,
                }
            }
        };
    }

    macro_rules! order {
        ($deny:expr, $($field:expr),*) => {
            Shape::Struct(&Struct {
                name: "Order",
                fields: &[$($field),*],
                deny_unknown_fields: $deny,
            })
        };
    }

    macro_rules! object {
        ($additional:expr, $($property:expr),*) => {
            Node::Object(&Object {
                properties: &[$($property),*],
                additional: $additional,
                pointer: "#",
            })
        };
    }

    const SKU: Property = Property {
        name: "sku",
        node: &NODE_STRING,
        required: true,
    };

    const NOTE: Property = Property {
        name: "note",
        node: &NODE_STRING,
        required: false,
    };

    #[test]
    fn a_declared_property_without_a_field_follows_the_direction() {
        const ORDER: Shape = order!(false, field!("sku", &STRING));
        const DENYING: Shape = order!(true, field!("sku", &STRING));
        const OPEN: Node = object!(Additional::Open, SKU, NOTE);
        const CLOSED: Node = object!(Additional::Closed, SKU, NOTE);
        fails(
            &ORDER,
            &OPEN,
            Direction::OWNED,
            "`Order` has no field `note`",
        );
        assert_eq!(run(&ORDER, &CLOSED, Direction::EXTERNAL), None);
        fails(
            &DENYING,
            &CLOSED,
            Direction::EXTERNAL,
            "denies unknown fields and has no field for it",
        );
    }

    #[test]
    fn a_flattened_map_reads_the_undeclared_properties() {
        const WITH_MAP: Shape = order!(
            false,
            field!("sku", &STRING),
            field!("extra", &Shape::Map(&Shape::Bool), flatten: true)
        );
        // A declared property the struct has no field for lands in the map.
        const DECLARED: Node = object!(Additional::Closed, SKU, NOTE);
        const OPEN: Node = object!(Additional::Open, SKU);
        const TYPED: Node = object!(Additional::Schema(&Node::Bool), SKU);
        fails(
            &WITH_MAP,
            &DECLARED,
            Direction::EXTERNAL,
            "Order.note: the field's type is a boolean",
        );
        fails(
            &WITH_MAP,
            &OPEN,
            Direction::EXTERNAL,
            "must hold `serde_json::Value`",
        );
        assert_eq!(run(&WITH_MAP, &TYPED, Direction::OWNED), None);
    }

    #[test]
    fn typed_additional_properties_reach_a_type_that_denies_unknown_fields() {
        const DENYING: Shape = order!(true, field!("sku", &STRING));
        const TYPED: Node = object!(Additional::Schema(&Node::Bool), SKU);
        fails(
            &DENYING,
            &TYPED,
            Direction::EXTERNAL,
            "admits additional properties",
        );
    }

    #[test]
    fn only_structs_and_maps_flatten() {
        const FLAT_LIST: Shape =
            order!(false, field!("items", &Shape::Seq(&STRING), flatten: true));
        const EMPTY: Node = object!(Additional::Open,);
        fails(
            &FLAT_LIST,
            &EMPTY,
            Direction::OWNED,
            "flattens structs and maps",
        );
    }

    #[test]
    fn an_unchecked_flattened_field_hides_the_properties_it_may_hold() {
        const HIDDEN: Shape = order!(
            false,
            field!("sku", &STRING),
            field!("audit", &Shape::Unchecked { option: false }, flatten: true)
        );
        const SCHEMA: Node = object!(Additional::Open, SKU, NOTE);
        assert_eq!(run(&HIDDEN, &SCHEMA, Direction::OWNED), None);
    }

    #[test]
    fn a_field_read_through_a_function_is_never_absent() {
        const STRICT: Shape = order!(
            false,
            field!("note", &Shape::Option(&STRING), read: Read::Strict)
        );
        const SCHEMA: Node = object!(
            Additional::Open,
            Property {
                node: &Node::Nullable(&NODE_STRING),
                ..NOTE
            }
        );
        fails(
            &STRICT,
            &SCHEMA,
            Direction::EXTERNAL,
            "Order.note: the schema lets it be absent",
        );
    }

    #[test]
    fn a_required_property_is_written_on_every_value() {
        const SKIPPED: Shape = order!(false, field!("sku", &STRING, write: Write::Never));
        const SCHEMA: Node = object!(Additional::Open, SKU);
        fails(
            &SKIPPED,
            &SCHEMA,
            Direction::OWNED,
            "does not write it on every value",
        );
        // Reading alone, a field serde never writes is no concern.
        assert_eq!(run(&SKIPPED, &SCHEMA, Direction::EXTERNAL), None);
    }
}
