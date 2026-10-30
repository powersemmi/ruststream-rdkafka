//! The canonical form of a type as compile-time data: what `AvroSchema::get_schema()` builds
//! at run time, written as constants the compiler composes from each field's own form.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// The Avro form of a type, composed at compile time.
///
/// The `Schema` derive implements it for every type it derives, from the forms of the type's
/// fields, and the crate implements it for the standard types `apache_avro`'s `AvroSchema`
/// derive maps. The derive compares the composed form with the newest migration of every
/// subject that declares the type's record, in a constant, so a mismatch fails the build and
/// nothing of the comparison reaches the binary.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no Avro form the `Schema` derive knows",
    label = "the field's type has no compile-time Avro form",
    note = "derive `Schema` on the type, or give the field `#[avro(with = ..)]` (its form is \
            then a run-time function, and the field is checked for presence only)"
)]
pub trait AvroForm {
    /// The form, as `AvroSchema::get_schema()` writes it.
    const FORM: &'static Form;

    /// The default a record field of this type takes when the field names none: `null` for an
    /// `Option`, the container's `#[avro(default = ..)]` for a derived type, nothing otherwise.
    /// Minified JSON with sorted keys.
    const FIELD_DEFAULT: Option<&'static str> = None;
}

/// The names of a record's fields on the wire, which a `#[serde(flatten)]` of the record
/// contributes to its parent.
///
/// The `Schema` derive implements it for every struct it derives, whether or not a migration
/// declares the struct's record: a flattened type is no record of its own.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is flattened into a checked record and lists no fields",
    label = "the flattened type does not derive `Schema`",
    note = "derive `Schema` on the flattened type, so the check knows the fields it contributes"
)]
pub trait AvroFields {
    /// The fields, in declaration order.
    const MEMBERS: &'static [Member];
}

/// One entry of [`AvroFields::MEMBERS`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Member {
    /// A field with this name.
    Field(&'static str),
    /// The fields of a flattened type.
    Flatten(&'static [Self]),
}

/// A named type's name as the type declares it.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Name {
    /// The namespace: `None` when the type names none and takes the enclosing one.
    pub namespace: Option<&'static str>,
    /// The name without its namespace.
    pub name: &'static str,
}

/// An Avro form.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Form {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array(&'static Self),
    Map(&'static Self),
    /// `["null", T]`, what an `Option<T>` writes.
    Nullable(&'static Self),
    /// Any other union.
    Union(&'static [&'static Self]),
    Record(&'static RecordForm),
    Enum(&'static EnumForm),
    Fixed(&'static FixedForm),
}

/// A record and its fields, in declaration order.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordForm {
    pub name: Name,
    pub fields: &'static [FieldForm],
}

/// One field of a record.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldForm {
    /// The field's name on the wire.
    pub name: &'static str,
    pub shape: Shape,
    /// The default, minified JSON with sorted keys.
    pub default: Option<&'static str>,
}

/// What a field contributes to its record.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// A field of this form.
    Typed(&'static Form),
    /// `#[avro(with = ..)]`: its form is a run-time function, so only its presence is checked.
    With,
    /// `#[serde(flatten)]` of a type with these fields: they join the parent's, checked for
    /// presence only.
    Flatten(&'static [Member]),
}

/// An Avro enum.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnumForm {
    pub name: Name,
    pub symbols: &'static [&'static str],
    pub default: Option<&'static str>,
}

/// An Avro fixed.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedForm {
    pub name: Name,
    pub size: usize,
}

macro_rules! primitive_form {
    ($($ty:ty => $form:ident),* $(,)?) => {
        $(impl AvroForm for $ty {
            const FORM: &'static Form = &Form::$form;
        })*
    };
}

primitive_form! {
    bool => Boolean,
    i8 => Int,
    i16 => Int,
    i32 => Int,
    u8 => Int,
    u16 => Int,
    i64 => Long,
    u32 => Long,
    f32 => Float,
    f64 => Double,
    String => String,
    str => String,
    char => String,
    () => Null,
}

const fn rust_name(name: &'static str) -> Name {
    Name {
        namespace: Some("org.apache.avro.rust"),
        name,
    }
}

macro_rules! fixed_form {
    ($($ty:ty => $name:literal, $size:literal),* $(,)?) => {
        $(impl AvroForm for $ty {
            const FORM: &'static Form = &Form::Fixed(&FixedForm {
                name: rust_name($name),
                size: $size,
            });
        })*
    };
}

fixed_form! {
    u64 => "u64", 8,
    u128 => "u128", 16,
    i128 => "i128", 16,
}

impl AvroForm for Duration {
    const FORM: &'static Form = &Form::Record(&RecordForm {
        name: rust_name("Duration"),
        fields: &[
            FieldForm {
                name: "secs",
                shape: Shape::Typed(<u64 as AvroForm>::FORM),
                default: None,
            },
            FieldForm {
                name: "nanos",
                shape: Shape::Typed(&Form::Long),
                default: None,
            },
        ],
    });
}

macro_rules! passthrough_form {
    ($($ty:ty),* $(,)?) => {
        $(impl<T: AvroForm + ?Sized> AvroForm for $ty {
            const FORM: &'static Form = T::FORM;
            const FIELD_DEFAULT: Option<&'static str> = T::FIELD_DEFAULT;
        })*
    };
}

passthrough_form!(&T, &mut T, Box<T>, Mutex<T>);

impl<T: AvroForm + ToOwned + ?Sized> AvroForm for Cow<'_, T> {
    const FORM: &'static Form = T::FORM;
    const FIELD_DEFAULT: Option<&'static str> = T::FIELD_DEFAULT;
}

impl<T: AvroForm> AvroForm for Vec<T> {
    const FORM: &'static Form = &Form::Array(T::FORM);
}

impl<T: AvroForm> AvroForm for [T] {
    const FORM: &'static Form = &Form::Array(T::FORM);
}

impl<T: AvroForm, Hasher> AvroForm for HashMap<String, T, Hasher> {
    const FORM: &'static Form = &Form::Map(T::FORM);
}

impl<T: AvroForm> AvroForm for Option<T> {
    const FORM: &'static Form = &Form::Nullable(T::FORM);
    const FIELD_DEFAULT: Option<&'static str> = Some("null");
}
