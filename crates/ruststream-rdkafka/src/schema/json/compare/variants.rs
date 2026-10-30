//! Enums against string enums and `oneOf`: which branch admits which variant, under each
//! serde tagging, and the variant's payload against its branch.

use super::object::object_value;
use super::{Ctx, Direction, Fields, contains, node_kind, shape_kind, value};
use crate::schema::json::message::{Message, Path, Step};
use crate::schema::json::node::{Additional, Node, Object, str_eq};
use crate::schema::json::shape::{Enum, Field, Payload, Read, Shape, Tagging, Variant, Write};

const fn branch_mismatch(ctx: &Ctx<'_>, branch: &Node, path: &Path<'_>) -> Message {
    ctx.at(path)
        .push("the variant is written as an object, and its oneOf branch is ")
        .push(node_kind(branch))
}

pub(super) const fn enum_value(
    ctx: &Ctx<'_>,
    shape: &Enum,
    node: &Node,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    match node {
        Node::Strings(names) => string_enum(ctx, shape, names, dir, path),
        Node::OneOf { branches, pointer } => one_of(ctx, shape, branches, pointer, dir, path),
        _ => Err(ctx
            .at(path)
            .push("the field's type is the enum ")
            .code(shape.name)
            .push(", and the schema declares ")
            .push(node_kind(node))),
    }
}

const fn string_enum(
    ctx: &Ctx<'_>,
    shape: &Enum,
    names: &[&str],
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    let external = matches!(shape.tagging, Tagging::External);
    if dir.writes {
        let mut index = 0;
        while index < shape.variants.len() {
            let variant = &shape.variants[index];
            index += 1;
            if !variant.write {
                continue;
            }
            let at = path.then(Step::Variant(variant.name));
            if !external || !matches!(variant.payload, Payload::Unit) {
                return Err(ctx
                    .at(&at)
                    .code(shape.name)
                    .push(" writes this variant as an object, and the schema admits only the strings of its enum"));
            }
            if !contains(names, variant.name) {
                return Err(ctx
                    .at(&at)
                    .code(shape.name)
                    .push(" writes this variant, and the schema's enum does not list it"));
            }
        }
    }
    if dir.reads {
        let mut index = 0;
        while index < names.len() {
            let name = names[index];
            index += 1;
            let reads = match find_variant(shape, name) {
                Some(variant) => {
                    variant.read && external && matches!(variant.payload, Payload::Unit)
                }
                None => false,
            };
            if !reads {
                return Err(ctx
                    .at(&path.then(Step::Variant(name)))
                    .push("the schema's enum admits this string, and ")
                    .code(shape.name)
                    .push(" has no unit variant that reads it"));
            }
        }
    }
    Ok(())
}

/// The variant names one `oneOf` branch admits under a tagging.
#[derive(Clone, Copy)]
enum Admits {
    Names(&'static [&'static str]),
    One(&'static str),
    Nothing,
}

impl Admits {
    const fn len(self) -> usize {
        match self {
            Self::Names(names) => names.len(),
            Self::One(_) => 1,
            Self::Nothing => 0,
        }
    }

    const fn get(self, index: usize) -> &'static str {
        match self {
            Self::Names(names) => names[index],
            Self::One(name) => name,
            Self::Nothing => "",
        }
    }

    const fn has(self, name: &str) -> bool {
        let mut index = 0;
        while index < self.len() {
            if str_eq(self.get(index), name) {
                return true;
            }
            index += 1;
        }
        false
    }
}

const fn admits(branch: &Node, tagging: Tagging) -> Admits {
    match tagging {
        Tagging::External => match branch {
            Node::Strings(names) => Admits::Names(names),
            Node::Const { value, .. } => Admits::One(value),
            Node::Object(object) if object.properties.len() == 1 => {
                Admits::One(object.properties[0].name)
            }
            _ => Admits::Nothing,
        },
        Tagging::Internal { tag } | Tagging::Adjacent { tag, .. } => match branch {
            Node::Object(object) => match object.property(tag) {
                Some(property) => match property.node.tag_value() {
                    Some(name) => Admits::One(name),
                    None => Admits::Nothing,
                },
                None => Admits::Nothing,
            },
            _ => Admits::Nothing,
        },
        Tagging::Untagged => Admits::Nothing,
    }
}

const fn one_of(
    ctx: &Ctx<'_>,
    shape: &Enum,
    branches: &[Node],
    pointer: &str,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    if dir.writes {
        let mut index = 0;
        while index < shape.variants.len() {
            let variant = &shape.variants[index];
            index += 1;
            if !variant.write {
                continue;
            }
            let at = path.then(Step::Variant(variant.name));
            let mut found = None;
            let mut branch = 0;
            while branch < branches.len() {
                if admits(&branches[branch], shape.tagging).has(variant.name) {
                    if found.is_some() {
                        return Err(ctx
                            .at(&at)
                            .push("two oneOf branches admit this variant, and oneOf demands exactly one match"));
                    }
                    found = Some(&branches[branch]);
                }
                branch += 1;
            }
            let Some(branch) = found else {
                return Err(ctx
                    .at(&at)
                    .code(shape.name)
                    .push(" writes this variant and no oneOf branch admits it"));
            };
            let pair = dir.narrow(true, variant.read);
            check!(variant_value(ctx, shape, variant, branch, pair, &at));
        }
    }
    if dir.reads {
        let mut branch = 0;
        while branch < branches.len() {
            let node = &branches[branch];
            branch += 1;
            let names = admits(node, shape.tagging);
            if names.len() == 0 {
                return Err(ctx
                    .at(path)
                    .push("a branch of the oneOf at ")
                    .code(pointer)
                    .push(" is no form ")
                    .code(shape.name)
                    .push("'s tagging writes"));
            }
            let mut index = 0;
            while index < names.len() {
                let name = names.get(index);
                index += 1;
                let at = path.then(Step::Variant(name));
                let Some(variant) = find_variant(shape, name) else {
                    return Err(ctx
                        .at(&at)
                        .push("the schema admits this variant, and ")
                        .code(shape.name)
                        .push(" has none by that name"));
                };
                if !variant.read {
                    return Err(ctx
                        .at(&at)
                        .push("the schema admits this variant, and ")
                        .code(shape.name)
                        .push(" skips it when deserializing"));
                }
                // A variant both written and read was compared both ways above.
                if !(dir.writes && variant.write) {
                    check!(variant_value(
                        ctx,
                        shape,
                        variant,
                        node,
                        dir.reads_only(),
                        &at
                    ));
                }
            }
        }
    }
    Ok(())
}

const fn variant_value(
    ctx: &Ctx<'_>,
    shape: &Enum,
    variant: &Variant,
    branch: &Node,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    match shape.tagging {
        Tagging::External => external_variant(ctx, shape, variant, branch, dir, path),
        Tagging::Internal { tag } => {
            let Node::Object(object) = branch else {
                return Err(branch_mismatch(ctx, branch, path));
            };
            check!(tag_required(ctx, object, tag, dir, path));
            let fields = Fields {
                owner: shape.name,
                tag: Some(tag),
                list: &[],
                denies_unknown: shape.deny_unknown_fields,
            };
            match variant.payload {
                Payload::Unit => object_value(ctx, fields, object, dir, path),
                Payload::Struct(list) => {
                    let fields = Fields { list, ..fields };
                    object_value(ctx, fields, object, dir, path)
                }
                Payload::Newtype(inner) => match inner {
                    Shape::Struct(inner) => {
                        let fields = Fields {
                            list: inner.fields,
                            denies_unknown: inner.deny_unknown_fields,
                            ..fields
                        };
                        object_value(ctx, fields, object, dir, path)
                    }
                    Shape::Map(_) | Shape::Any | Shape::Unchecked { .. } => {
                        let flat = [Field {
                            name: variant.name,
                            shape: inner,
                            write: Write::Always,
                            read: Read::Plain,
                            flatten: true,
                        }];
                        let fields = Fields {
                            list: &flat,
                            ..fields
                        };
                        object_value(ctx, fields, object, dir, path)
                    }
                    _ => Err(ctx
                        .at(path)
                        .push("an internally tagged variant carries a struct or a map, and this one carries ")
                        .push(shape_kind(inner))),
                },
            }
        }
        Tagging::Adjacent { tag, content } => {
            let Node::Object(object) = branch else {
                return Err(branch_mismatch(ctx, branch, path));
            };
            adjacent_variant(ctx, shape, variant, object, (tag, content), dir, path)
        }
        Tagging::Untagged => Ok(()),
    }
}

const fn external_variant(
    ctx: &Ctx<'_>,
    shape: &Enum,
    variant: &Variant,
    branch: &Node,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    let object = match (variant.payload, branch) {
        (Payload::Unit, Node::Strings(_) | Node::Const { .. }) => return Ok(()),
        (Payload::Unit, _) => {
            return Err(ctx
                .at(path)
                .push("the variant carries nothing and is written as a string, and its oneOf branch is ")
                .push(node_kind(branch)));
        }
        (_, Node::Object(object)) => object,
        _ => {
            return Err(ctx
                .at(path)
                .push("the variant is written as an object, and its oneOf branch is ")
                .push(node_kind(branch)));
        }
    };
    let Some(property) = object.property(variant.name) else {
        return Err(branch_mismatch(ctx, branch, path));
    };
    if dir.reads && !property.required {
        return Err(ctx
            .at(path)
            .push("the oneOf branch at ")
            .code(object.pointer)
            .push(" lets the variant's property be absent, which serde refuses"));
    }
    if dir.reads && !matches!(object.additional, Additional::Closed) {
        return Err(ctx
            .at(path)
            .push("the oneOf branch at ")
            .code(object.pointer)
            .push(" lets other properties stand beside the variant's, which serde refuses; close it with additionalProperties: false"));
    }
    match variant.payload {
        Payload::Newtype(inner) => value(ctx, inner, property.node, dir, path),
        Payload::Struct(list) => {
            let Node::Object(inner) = property.node else {
                return Err(ctx
                    .at(path)
                    .push("the variant carries named fields, and the schema declares ")
                    .push(node_kind(property.node)));
            };
            let fields = Fields {
                owner: shape.name,
                tag: None,
                list,
                denies_unknown: shape.deny_unknown_fields,
            };
            object_value(ctx, fields, inner, dir, path)
        }
        Payload::Unit => Ok(()),
    }
}

const fn adjacent_variant(
    ctx: &Ctx<'_>,
    shape: &Enum,
    variant: &Variant,
    object: &Object,
    (tag, content): (&str, &str),
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    check!(tag_required(ctx, object, tag, dir, path));
    let mut index = 0;
    while index < object.properties.len() {
        let property = &object.properties[index];
        index += 1;
        let carried = !matches!(variant.payload, Payload::Unit) && str_eq(property.name, content);
        if str_eq(property.name, tag) || carried {
            continue;
        }
        if dir.writes || shape.deny_unknown_fields {
            return Err(ctx
                .at(path)
                .push("the schema declares property ")
                .code(property.name)
                .push(", which ")
                .code(shape.name)
                .push(" does not write for this variant"));
        }
    }
    if matches!(variant.payload, Payload::Unit) {
        return Ok(());
    }
    let Some(property) = object.property(content) else {
        return Err(ctx
            .at(path)
            .push("the oneOf branch at ")
            .code(object.pointer)
            .push(" has no content property ")
            .code(content));
    };
    if dir.reads && !property.required {
        return Err(ctx
            .at(path)
            .push("the schema lets the content property ")
            .code(content)
            .push(" be absent, which serde refuses"));
    }
    match variant.payload {
        Payload::Newtype(inner) => value(ctx, inner, property.node, dir, path),
        Payload::Struct(list) => {
            let Node::Object(inner) = property.node else {
                return Err(ctx
                    .at(path)
                    .push("the variant carries named fields, and the schema declares ")
                    .push(node_kind(property.node)));
            };
            let fields = Fields {
                owner: shape.name,
                tag: None,
                list,
                denies_unknown: shape.deny_unknown_fields,
            };
            object_value(ctx, fields, inner, dir, path)
        }
        Payload::Unit => Ok(()),
    }
}

const fn tag_required(
    ctx: &Ctx<'_>,
    object: &Object,
    tag: &str,
    dir: Direction,
    path: &Path<'_>,
) -> Result<(), Message> {
    if let Some(property) = object.property(tag)
        && dir.reads
        && !property.required
    {
        return Err(ctx
            .at(path)
            .push("the schema lets the tag ")
            .code(tag)
            .push(" be absent, which serde refuses"));
    }
    Ok(())
}

const fn find_variant<'a>(shape: &'a Enum, name: &str) -> Option<&'a Variant> {
    let mut index = 0;
    while index < shape.variants.len() {
        if str_eq(shape.variants[index].name, name) {
            return Some(&shape.variants[index]);
        }
        index += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::super::tests::{fails, run};
    use super::*;
    use crate::schema::json::node::Property;

    const STRING: Shape = Shape::String { format: None };
    const NODE_STRING: Node = Node::String { format: None };

    macro_rules! payment {
        ($tagging:expr, $($variant:expr),*) => {
            Shape::Enum(&Enum {
                name: "Payment",
                tagging: $tagging,
                variants: &[$($variant),*],
                deny_unknown_fields: false,
            })
        };
    }

    macro_rules! one_of {
        ($($branch:expr),*) => {
            Node::OneOf {
                branches: &[$($branch),*],
                pointer: "#/properties/payment/oneOf",
            }
        };
    }

    macro_rules! object {
        ($additional:expr, $($property:expr),*) => {
            Node::Object(&Object {
                properties: &[$($property),*],
                additional: $additional,
                pointer: "#/properties/payment/oneOf/1",
            })
        };
    }

    macro_rules! property {
        ($name:literal, $node:expr, $required:expr) => {
            Property {
                name: $name,
                node: $node,
                required: $required,
            }
        };
    }

    macro_rules! tag {
        ($name:literal, $value:literal, $required:expr) => {
            property!(
                $name,
                &Node::Const {
                    value: $value,
                    pointer: "#/const",
                },
                $required
            )
        };
    }

    const fn unit(name: &'static str) -> Variant {
        Variant {
            name,
            payload: Payload::Unit,
            write: true,
            read: true,
        }
    }

    const CARD: Variant = Variant {
        name: "Card",
        payload: Payload::Newtype(&STRING),
        write: true,
        read: true,
    };

    const CARD_BRANCH: Node = object!(Additional::Closed, property!("Card", &NODE_STRING, true));

    #[test]
    fn a_string_enum_lists_exactly_the_unit_variants() {
        const STATUS: Shape = payment!(Tagging::External, unit("New"));
        const MIXED: Shape = payment!(Tagging::External, unit("New"), CARD);
        fails(
            &STATUS,
            &Node::Strings(&["New", "Done"]),
            Direction::EXTERNAL,
            "Order<Done>: the schema's enum admits this string",
        );
        fails(
            &MIXED,
            &Node::Strings(&["New", "Card"]),
            Direction::OWNED,
            "Order<Card>: `Payment` writes this variant as an object",
        );
    }

    #[test]
    fn one_branch_admits_each_variant() {
        const PAYMENT: Shape = payment!(Tagging::External, CARD);
        const TWICE: Node = one_of!(CARD_BRANCH, CARD_BRANCH);
        const ONCE: Node = one_of!(CARD_BRANCH);
        fails(
            &PAYMENT,
            &TWICE,
            Direction::OWNED,
            "two oneOf branches admit this variant",
        );
        assert_eq!(run(&PAYMENT, &ONCE, Direction::OWNED), None);
    }

    #[test]
    fn every_branch_reads_into_a_variant() {
        const PAYMENT: Shape = payment!(Tagging::External, CARD);
        const SKIPPED: Shape = payment!(
            Tagging::External,
            Variant {
                read: false,
                ..CARD
            }
        );
        const UNKNOWN: Node = one_of!(CARD_BRANCH, Node::Strings(&["Cash"]));
        const SHAPELESS: Node = one_of!(CARD_BRANCH, Node::Bool);
        const ONCE: Node = one_of!(CARD_BRANCH);
        fails(
            &PAYMENT,
            &UNKNOWN,
            Direction::EXTERNAL,
            "Order<Cash>: the schema admits this variant, and `Payment` has none by that name",
        );
        fails(
            &PAYMENT,
            &SHAPELESS,
            Direction::EXTERNAL,
            "is no form `Payment`'s tagging writes",
        );
        fails(
            &SKIPPED,
            &ONCE,
            Direction::EXTERNAL,
            "skips it when deserializing",
        );
    }

    #[test]
    fn an_externally_tagged_branch_holds_the_variant_alone() {
        const PAYMENT: Shape = payment!(Tagging::External, CARD);
        const CASH: Shape = payment!(Tagging::External, unit("Card"));
        const OPEN: Node = one_of!(object!(
            Additional::Open,
            property!("Card", &NODE_STRING, true)
        ));
        const OPTIONAL: Node = one_of!(object!(
            Additional::Closed,
            property!("Card", &NODE_STRING, false)
        ));
        const ONCE: Node = one_of!(CARD_BRANCH);
        fails(
            &PAYMENT,
            &OPEN,
            Direction::EXTERNAL,
            "lets other properties stand beside",
        );
        fails(
            &PAYMENT,
            &OPTIONAL,
            Direction::EXTERNAL,
            "lets the variant's property be absent",
        );
        fails(
            &CASH,
            &ONCE,
            Direction::OWNED,
            "the variant carries nothing",
        );
    }

    #[test]
    fn an_internally_tagged_variant_carries_a_struct_or_a_map() {
        const KIND: Tagging = Tagging::Internal { tag: "kind" };
        const FLAG: Shape = payment!(
            KIND,
            Variant {
                payload: Payload::Newtype(&Shape::Bool),
                ..unit("Flag")
            }
        );
        const MAP: Shape = payment!(
            KIND,
            Variant {
                payload: Payload::Newtype(&Shape::Map(&Shape::Any)),
                ..unit("Flag")
            }
        );
        const BRANCH: Node = one_of!(object!(Additional::Open, tag!("kind", "Flag", true)));
        const OPTIONAL: Node = one_of!(object!(Additional::Open, tag!("kind", "Flag", false)));
        fails(
            &FLAG,
            &BRANCH,
            Direction::OWNED,
            "carries a struct or a map, and this one carries a boolean",
        );
        assert_eq!(run(&MAP, &BRANCH, Direction::OWNED), None);
        fails(
            &MAP,
            &OPTIONAL,
            Direction::EXTERNAL,
            "lets the tag `kind` be absent",
        );
    }

    #[test]
    fn an_adjacently_tagged_branch_holds_the_tag_and_the_content() {
        const PAYMENT: Shape = payment!(
            Tagging::Adjacent {
                tag: "t",
                content: "c",
            },
            CARD
        );
        const MISSING: Node = one_of!(object!(Additional::Open, tag!("t", "Card", true)));
        const OPTIONAL: Node = one_of!(object!(
            Additional::Open,
            tag!("t", "Card", true),
            property!("c", &NODE_STRING, false)
        ));
        const EXTRA: Node = one_of!(object!(
            Additional::Open,
            tag!("t", "Card", true),
            property!("c", &NODE_STRING, true),
            property!("x", &NODE_STRING, false)
        ));
        fails(
            &PAYMENT,
            &MISSING,
            Direction::OWNED,
            "has no content property `c`",
        );
        fails(
            &PAYMENT,
            &OPTIONAL,
            Direction::EXTERNAL,
            "lets the content property `c` be absent",
        );
        fails(
            &PAYMENT,
            &EXTRA,
            Direction::OWNED,
            "declares property `x`, which `Payment` does not write",
        );
        assert_eq!(run(&PAYMENT, &EXTRA, Direction::EXTERNAL), None);
    }
}
