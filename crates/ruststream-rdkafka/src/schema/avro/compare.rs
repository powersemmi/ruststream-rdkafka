//! The whole-schema comparison the `Schema` derive evaluates in a constant: the type's composed
//! form against a migration's, as equality for an owned subject and under Avro's resolution
//! rules for an external one. The first difference panics, which fails the build with a
//! message naming the nested record and the field that differ.
//!
//! Every function on the way to that panic is `#[track_caller]`: the compiler leaves such frames
//! out of the error, so the message points at the derive alone instead of tracing through this
//! module into the standard library's `panic!`, whose source not every toolchain installs.

use super::form::{EnumForm, FieldForm, FixedForm, Form, Member, Name, RecordForm, Shape};

/// Where a comparison runs: the type, the subject and the migration the message names.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Site {
    /// The Rust type being checked.
    pub type_name: &'static str,
    pub subject: &'static str,
    /// The migration's file stem (`0002_add_item`).
    pub migration: &'static str,
}

/// Fails the build unless the type's form equals the migration's: the rule of an owned subject.
#[doc(hidden)]
#[track_caller]
pub const fn assert_equals(ty: &Form, migration: &Form, site: Site) {
    let at = At::root(site);
    same(ty, migration, &at);
}

/// Fails the build unless the type reads what the migration writes under Avro's resolution
/// rules: the rule of an external subject, checked once per pulled version.
#[doc(hidden)]
#[track_caller]
pub const fn assert_reads(ty: &Form, migration: &Form, site: Site) {
    let at = At::root(site);
    reads(ty, migration, &at);
}

/// The position of a comparison: the innermost record of each side and the field within it.
#[derive(Clone, Copy)]
struct At {
    site: Site,
    /// The namespace a type-side name without one takes.
    enclosing: Option<&'static str>,
    /// The migration's record this position is inside, with its namespace and name.
    record: Option<Name>,
    field: Option<&'static str>,
    reading: bool,
    /// A direct field of the checked type: a per-field bound already reports a different
    /// kind there, so the comparison only descends into the named types it holds.
    bounded: bool,
    /// The position is the checked type's own record.
    root_record: bool,
}

impl At {
    const fn root(site: Site) -> Self {
        Self {
            site,
            enclosing: None,
            record: None,
            field: None,
            reading: false,
            bounded: false,
            root_record: false,
        }
    }

    const fn record(&self, enclosing: Option<&'static str>, record: Name) -> Self {
        Self {
            site: self.site,
            enclosing,
            record: Some(record),
            field: None,
            reading: self.reading,
            bounded: false,
            root_record: self.record.is_none(),
        }
    }

    const fn field(&self, field: &'static str) -> Self {
        Self {
            site: self.site,
            enclosing: self.enclosing,
            record: self.record,
            field: Some(field),
            reading: self.reading,
            bounded: self.root_record,
            root_record: false,
        }
    }

    /// Fails the build with `problem` at this position.
    #[track_caller]
    const fn fail(&self, problem: &[&str]) -> ! {
        let mut msg = Message::new();
        msg.push("`");
        msg.push(self.site.type_name);
        msg.push(if self.reading {
            "` cannot read subject `"
        } else {
            "` does not match subject `"
        });
        msg.push(self.site.subject);
        msg.push("` (migration ");
        msg.push(self.site.migration);
        msg.push(")");
        if let Some(record) = self.record {
            msg.push(": record `");
            push_name(&mut msg, record);
            msg.push("`");
            if let Some(field) = self.field {
                msg.push(", field `");
                msg.push(field);
                msg.push("`");
            }
        }
        msg.push(": ");
        let mut index = 0;
        while index < problem.len() {
            msg.push(problem[index]);
            index += 1;
        }
        panic!("{}", msg.as_str());
    }
}

const fn push_name(msg: &mut Message, name: Name) {
    if let Some(namespace) = name.namespace
        && !namespace.is_empty()
    {
        msg.push(namespace);
        msg.push(".");
    }
    msg.push(name.name);
}

/// A panic message built in a constant.
struct Message {
    buf: [u8; 1024],
    len: usize,
}

impl Message {
    const fn new() -> Self {
        Self {
            buf: [0; 1024],
            len: 0,
        }
    }

    const fn push(&mut self, text: &str) {
        let bytes = text.as_bytes();
        let mut index = 0;
        while index < bytes.len() && self.len < self.buf.len() {
            self.buf[self.len] = bytes[index];
            self.len += 1;
            index += 1;
        }
        // A cut inside a character drops the character's leading bytes as well.
        if index < bytes.len() {
            while self.len > 0 && self.buf[self.len - 1] & 0xC0 == 0x80 {
                self.len -= 1;
            }
            if self.len > 0 && self.buf[self.len - 1] >= 0xC0 {
                self.len -= 1;
            }
        }
    }

    const fn as_str(&self) -> &str {
        let (head, _) = self.buf.split_at(self.len);
        match core::str::from_utf8(head) {
            Ok(text) => text,
            Err(_) => "the schemas differ",
        }
    }
}

const fn str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut index = 0;
    while index < a.len() {
        if a[index] != b[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// The namespace a type-side name resolves to at this position.
const fn namespace_of(name: Name, enclosing: Option<&'static str>) -> &'static str {
    match name.namespace {
        Some(namespace) => namespace,
        None => match enclosing {
            Some(namespace) => namespace,
            None => "",
        },
    }
}

/// Full names are equal: the migration's names are resolved, the type's take the enclosing
/// namespace when they name none.
const fn same_name(ty: Name, migration: Name, enclosing: Option<&'static str>) -> bool {
    let migration_namespace = match migration.namespace {
        Some(namespace) => namespace,
        None => "",
    };
    str_eq(ty.name, migration.name) && str_eq(namespace_of(ty, enclosing), migration_namespace)
}

const fn describe(form: &Form) -> &'static str {
    match form {
        Form::Null => "`null`",
        Form::Boolean => "`boolean`",
        Form::Int => "`int`",
        Form::Long => "`long`",
        Form::Float => "`float`",
        Form::Double => "`double`",
        Form::Bytes => "`bytes`",
        Form::String => "`string`",
        Form::Array(_) => "an array",
        Form::Map(_) => "a map",
        Form::Nullable(_) => "a union with `null` (an `Option`)",
        Form::Union(_) => "a union",
        Form::Record(_) => "a record",
        Form::Enum(_) => "an enum",
        Form::Fixed(_) => "a fixed",
    }
}

#[track_caller]
const fn differ(ty: &Form, migration: &Form, at: &At) {
    if at.bounded {
        return;
    }
    at.fail(&[
        "the type writes ",
        describe(ty),
        " where the migration declares ",
        describe(migration),
    ])
}

#[track_caller]
const fn same(ty: &Form, migration: &Form, at: &At) {
    match (ty, migration) {
        (Form::Null, Form::Null)
        | (Form::Boolean, Form::Boolean)
        | (Form::Int, Form::Int)
        | (Form::Long, Form::Long)
        | (Form::Float, Form::Float)
        | (Form::Double, Form::Double)
        | (Form::Bytes, Form::Bytes)
        | (Form::String, Form::String) => {}
        (Form::Array(ty), Form::Array(migration))
        | (Form::Map(ty), Form::Map(migration))
        | (Form::Nullable(ty), Form::Nullable(migration)) => same(ty, migration, at),
        (Form::Union(ty), Form::Union(migration)) => {
            if ty.len() != migration.len() {
                if at.bounded {
                    return;
                }
                at.fail(&["the unions have a different number of branches"]);
            }
            let mut index = 0;
            while index < ty.len() {
                same(ty[index], migration[index], at);
                index += 1;
            }
        }
        (Form::Record(ty), Form::Record(migration)) => same_record(ty, migration, at),
        (Form::Enum(ty), Form::Enum(migration)) => same_enum(ty, migration, at, false),
        (Form::Fixed(ty), Form::Fixed(migration)) => same_fixed(ty, migration, at),
        _ => differ(ty, migration, at),
    }
}

/// The two forms have the same structure, names aside.
const fn shapes_match(ty: &Form, migration: &Form) -> bool {
    match (ty, migration) {
        (Form::Array(ty), Form::Array(migration))
        | (Form::Map(ty), Form::Map(migration))
        | (Form::Nullable(ty), Form::Nullable(migration)) => shapes_match(ty, migration),
        (Form::Union(ty), Form::Union(migration)) => {
            if ty.len() != migration.len() {
                return false;
            }
            let mut index = 0;
            while index < ty.len() {
                if !shapes_match(ty[index], migration[index]) {
                    return false;
                }
                index += 1;
            }
            true
        }
        (Form::Null, Form::Null)
        | (Form::Boolean, Form::Boolean)
        | (Form::Int, Form::Int)
        | (Form::Long, Form::Long)
        | (Form::Float, Form::Float)
        | (Form::Double, Form::Double)
        | (Form::Bytes, Form::Bytes)
        | (Form::String, Form::String)
        | (Form::Record(_), Form::Record(_))
        | (Form::Enum(_), Form::Enum(_))
        | (Form::Fixed(_), Form::Fixed(_)) => true,
        _ => false,
    }
}

#[track_caller]
const fn named_differ(what: &'static str, ty: Name, migration: Name, at: &At) -> ! {
    let mut name = Message::new();
    push_name(
        &mut name,
        Name {
            namespace: Some(namespace_of(ty, at.enclosing)),
            name: ty.name,
        },
    );
    let mut expected = Message::new();
    push_name(&mut expected, migration);
    at.fail(&[
        "the type is the ",
        what,
        " `",
        name.as_str(),
        "` where the migration declares `",
        expected.as_str(),
        "`",
    ])
}

#[track_caller]
const fn same_fixed(ty: &FixedForm, migration: &FixedForm, at: &At) {
    if !same_name(ty.name, migration.name, at.enclosing) {
        named_differ("fixed", ty.name, migration.name, at);
    }
    if ty.size != migration.size {
        at.fail(&["the fixed sizes differ"]);
    }
}

#[track_caller]
const fn same_enum(ty: &EnumForm, migration: &EnumForm, at: &At, reading: bool) {
    if reading {
        if !str_eq(ty.name.name, migration.name.name) {
            named_differ("enum", ty.name, migration.name, at);
        }
    } else if !same_name(ty.name, migration.name, at.enclosing) {
        named_differ("enum", ty.name, migration.name, at);
    }
    let mut index = 0;
    while index < migration.symbols.len() {
        let symbol = migration.symbols[index];
        if reading {
            if !contains(ty.symbols, symbol) {
                at.fail(&["the type has no variant for the symbol `", symbol, "`"]);
            }
        } else if index >= ty.symbols.len() || !str_eq(ty.symbols[index], symbol) {
            at.fail(&[
                "the symbols differ at `",
                symbol,
                "`: an owned enum lists the migration's symbols in their order",
            ]);
        }
        index += 1;
    }
    if !reading && ty.symbols.len() != migration.symbols.len() {
        at.fail(&["the type has symbols the migration lacks"]);
    }
}

const fn contains(symbols: &[&str], symbol: &str) -> bool {
    let mut index = 0;
    while index < symbols.len() {
        if str_eq(symbols[index], symbol) {
            return true;
        }
        index += 1;
    }
    false
}

const fn has_flatten(fields: &[FieldForm]) -> bool {
    let mut index = 0;
    while index < fields.len() {
        if let Shape::Flatten(_) = fields[index].shape {
            return true;
        }
        index += 1;
    }
    false
}

const fn find(fields: &[FieldForm], name: &str) -> Option<usize> {
    let mut index = 0;
    while index < fields.len() {
        if str_eq(fields[index].name, name) {
            return Some(index);
        }
        index += 1;
    }
    None
}

/// The field a flattened type of the record provides under `name`.
const fn flattened(fields: &[FieldForm], name: &str) -> bool {
    let mut index = 0;
    while index < fields.len() {
        if let Shape::Flatten(members) = fields[index].shape
            && has_member(members, name)
        {
            return true;
        }
        index += 1;
    }
    false
}

const fn has_member(members: &[Member], name: &str) -> bool {
    let mut index = 0;
    while index < members.len() {
        match members[index] {
            Member::Field(field) if str_eq(field, name) => return true,
            Member::Flatten(inner) if has_member(inner, name) => return true,
            _ => {}
        }
        index += 1;
    }
    false
}

/// Every field a flattened type contributes is one the migration declares.
#[track_caller]
const fn members_declared(members: &[Member], migration: &[FieldForm], at: &At) {
    let mut index = 0;
    while index < members.len() {
        match members[index] {
            Member::Field(name) => {
                if find(migration, name).is_none() {
                    at.field(name)
                        .fail(&["a flattened type writes the field and the migration has none"]);
                }
            }
            Member::Flatten(inner) => members_declared(inner, migration, at),
        }
        index += 1;
    }
}

/// Every field a flattened type contributes is one the writer writes: a flattened field has
/// no default of its own.
#[track_caller]
const fn members_written(members: &[Member], writer: &RecordForm, at: &At) {
    let mut index = 0;
    while index < members.len() {
        match members[index] {
            Member::Field(name) => {
                if find(writer.fields, name).is_none() {
                    at.field(name).fail(&[
                        "the writer has no such field, and a field of a flattened type takes                          no default",
                    ]);
                }
            }
            Member::Flatten(inner) => members_written(inner, writer, at),
        }
        index += 1;
    }
}

#[track_caller]
const fn same_default(ty: &FieldForm, migration: &FieldForm, at: &At) {
    match (ty.default, migration.default) {
        (None, None) => {}
        (Some(ty), Some(migration)) if str_eq(ty, migration) => {}
        (Some(ty), Some(migration)) => at.fail(&[
            "the type's default is `",
            ty,
            "` where the migration declares `",
            migration,
            "`",
        ]),
        (Some(ty), None) => at.fail(&[
            "the type gives the default `",
            ty,
            "` and the migration declares none",
        ]),
        (None, Some(migration)) => at.fail(&[
            "the migration declares the default `",
            migration,
            "` and the type gives none",
        ]),
    }
}

#[track_caller]
const fn same_record(ty: &RecordForm, migration: &RecordForm, outer: &At) {
    if !same_name(ty.name, migration.name, outer.enclosing) {
        named_differ("record", ty.name, migration.name, outer);
    }
    let enclosing = Some(namespace_of(ty.name, outer.enclosing));
    let at = outer.record(enclosing, migration.name);
    if has_flatten(ty.fields) {
        return same_fields_by_presence(ty.fields, migration.fields, &at);
    }
    let mut index = 0;
    while index < migration.fields.len() {
        let expected = &migration.fields[index];
        let here = at.field(expected.name);
        if index >= ty.fields.len() {
            here.fail(&["the type has no such field"]);
        }
        let field = &ty.fields[index];
        if !str_eq(field.name, expected.name) {
            if find(ty.fields, expected.name).is_some() {
                here.fail(&[
                    "the field is in another position; Avro encodes fields in the order the \
                     schema lists them",
                ]);
            }
            here.fail(&["the type has no such field"]);
        }
        if let (Shape::Typed(form), Shape::Typed(expected_form)) = (field.shape, expected.shape) {
            same(form, expected_form, &here);
            // A field of another kind is reported by its bound; its default follows its kind.
            if shapes_match(form, expected_form) {
                same_default(field, expected, &here);
            }
        }
        index += 1;
    }
    if ty.fields.len() > migration.fields.len() {
        let extra = at.field(ty.fields[migration.fields.len()].name);
        extra.fail(&["the migration has no such field"]);
    }
}

#[track_caller]
const fn same_fields_by_presence(ty: &[FieldForm], migration: &[FieldForm], at: &At) {
    let mut index = 0;
    while index < migration.len() {
        let expected = &migration[index];
        let here = at.field(expected.name);
        match find(ty, expected.name) {
            Some(found) => {
                if let (Shape::Typed(form), Shape::Typed(expected_form)) =
                    (ty[found].shape, expected.shape)
                {
                    same(form, expected_form, &here);
                    same_default(&ty[found], expected, &here);
                }
            }
            None => {
                if !flattened(ty, expected.name) {
                    here.fail(&["neither the type nor a type it flattens has such a field"]);
                }
            }
        }
        index += 1;
    }
    let mut index = 0;
    while index < ty.len() {
        match ty[index].shape {
            Shape::Flatten(members) => members_declared(members, migration, at),
            Shape::Typed(_) | Shape::With => {
                if find(migration, ty[index].name).is_none() {
                    at.field(ty[index].name)
                        .fail(&["the migration has no such field"]);
                }
            }
        }
        index += 1;
    }
}

/// Avro's resolution rules, with the type as the reader and the migration as the writer.
#[track_caller]
const fn reads(reader: &Form, writer: &Form, at: &At) {
    let at = &At {
        reading: true,
        ..*at
    };
    match (reader, writer) {
        (Form::Null | Form::Nullable(_), Form::Null)
        | (Form::Boolean, Form::Boolean)
        | (Form::Int, Form::Int)
        | (Form::Long, Form::Int | Form::Long)
        | (Form::Float, Form::Int | Form::Long | Form::Float)
        | (Form::Double, Form::Int | Form::Long | Form::Float | Form::Double)
        | (Form::Bytes | Form::String, Form::Bytes | Form::String) => {}
        (Form::Array(reader), Form::Array(writer)) | (Form::Map(reader), Form::Map(writer)) => {
            reads(reader, writer, at);
        }
        (Form::Nullable(reader), Form::Nullable(writer)) => reads(reader, writer, at),
        (Form::Nullable(reader), _) => reads(reader, writer, at),
        (_, Form::Nullable(_)) if at.bounded => {}
        (_, Form::Nullable(_)) => at.fail(&[
            "the writer may write `null` and the type cannot hold it (make the field an `Option`)",
        ]),
        (_, Form::Union(branches)) => {
            let mut index = 0;
            while index < branches.len() {
                reads(reader, branches[index], at);
                index += 1;
            }
        }
        (Form::Record(reader), Form::Record(writer)) => reads_record(reader, writer, at),
        (Form::Enum(reader), Form::Enum(writer)) => same_enum(reader, writer, at, true),
        (Form::Fixed(reader), Form::Fixed(writer)) => {
            if !str_eq(reader.name.name, writer.name.name) {
                named_differ("fixed", reader.name, writer.name, at);
            }
            if reader.size != writer.size {
                at.fail(&["the fixed sizes differ"]);
            }
        }
        _ if at.bounded => {}
        _ => at.fail(&[
            "the type reads ",
            describe(reader),
            " and the writer writes ",
            describe(writer),
            ", which Avro does not resolve",
        ]),
    }
}

#[track_caller]
const fn reads_record(reader: &RecordForm, writer: &RecordForm, outer: &At) {
    // Avro matches a reader's record to the writer's by the unqualified name.
    if !str_eq(reader.name.name, writer.name.name) {
        named_differ("record", reader.name, writer.name, outer);
    }
    let enclosing = Some(namespace_of(reader.name, outer.enclosing));
    let at = outer.record(enclosing, writer.name);
    let mut index = 0;
    while index < reader.fields.len() {
        let field = &reader.fields[index];
        let here = at.field(field.name);
        match field.shape {
            Shape::Flatten(members) => members_written(members, writer, &at),
            Shape::Typed(form) => match find(writer.fields, field.name) {
                Some(found) => {
                    if let Shape::Typed(written) = writer.fields[found].shape {
                        reads(form, written, &here);
                    }
                }
                None => absent(field, &here),
            },
            Shape::With => {
                if find(writer.fields, field.name).is_none() {
                    absent(field, &here);
                }
            }
        }
        index += 1;
    }
}

#[track_caller]
const fn absent(field: &FieldForm, at: &At) {
    if field.default.is_none() {
        at.fail(&[
            "the writer has no such field and the type gives it no default (add \
             `#[avro(default = ..)]` or make it an `Option`)",
        ]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SITE: Site = Site {
        type_name: "Order",
        subject: "orders-value",
        migration: "0002_add_item",
    };

    const ORDER_NAME: Name = Name {
        namespace: Some("com.acme.orders"),
        name: "Order",
    };

    macro_rules! order {
        ($($field:expr),*) => {
            &Form::Record(&RecordForm {
                name: ORDER_NAME,
                fields: &[$($field),*],
            })
        };
    }

    const ID_LONG: FieldForm = FieldForm {
        name: "id",
        shape: Shape::Typed(&Form::Long),
        default: None,
    };

    const ID_INT: FieldForm = FieldForm {
        name: "id",
        shape: Shape::Typed(&Form::Int),
        default: None,
    };

    #[test]
    fn equal_forms_pass_and_the_reader_rules_promote_int_to_long() {
        assert_equals(order!(ID_LONG), order!(ID_LONG), SITE);
        assert_reads(order!(ID_LONG), order!(ID_INT), SITE);
    }

    macro_rules! item {
        ($form:expr) => {
            FieldForm {
                name: "item",
                shape: Shape::Typed($form),
                default: None,
            }
        };
    }

    macro_rules! item_with_sku {
        ($sku:expr) => {
            &Form::Record(&RecordForm {
                name: Name {
                    namespace: Some("com.acme.orders"),
                    name: "Item",
                },
                fields: &[FieldForm {
                    name: "sku",
                    shape: Shape::Typed($sku),
                    default: None,
                }],
            })
        };
    }

    #[test]
    #[should_panic(
        expected = "`Order` does not match subject `orders-value` (migration 0002_add_item): \
                    record `com.acme.orders.Item`, field `sku`: the type writes `int` where the \
                    migration declares `long`"
    )]
    fn a_different_kind_in_a_nested_record_names_the_record_and_the_field() {
        assert_equals(
            order!(item!(item_with_sku!(&Form::Int))),
            order!(item!(item_with_sku!(&Form::Long))),
            SITE,
        );
    }

    #[test]
    fn a_different_kind_of_a_direct_field_is_left_to_its_bound() {
        assert_equals(order!(ID_INT), order!(ID_LONG), SITE);
    }

    #[test]
    #[should_panic(expected = "the writer has no such field and the type gives it no default")]
    fn a_reader_field_the_writer_lacks_needs_a_default() {
        assert_reads(order!(ID_LONG), order!(), SITE);
    }

    #[test]
    #[should_panic(expected = "the type reads `int` and the writer writes `long`")]
    fn a_reader_does_not_narrow() {
        assert_reads(
            order!(item!(item_with_sku!(&Form::Int))),
            order!(item!(item_with_sku!(&Form::Long))),
            SITE,
        );
    }

    #[test]
    fn a_long_message_is_cut_at_a_character() {
        let mut msg = Message::new();
        let mut index = 0;
        while index < 1023 {
            msg.push("a");
            index += 1;
        }
        msg.push("\u{e9}");
        assert_eq!(msg.as_str().len(), 1023);
    }
}
