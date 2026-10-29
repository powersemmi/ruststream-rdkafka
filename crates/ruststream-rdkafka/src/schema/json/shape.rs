//! The serde shape of a Rust type: what `serde_json` writes for its values and what it reads into
//! them, built at compile time from the type's serde attributes and its fields' own shapes.

/// What `serde_json` makes of a type's values.
#[derive(Debug, Clone, Copy)]
pub enum Shape {
    /// `bool`.
    Bool,
    /// An integer type, by the range of values it holds.
    Integer {
        /// The type's name, for messages (`u32`).
        name: &'static str,
        /// The least value.
        min: i128,
        /// The greatest value.
        max: i128,
    },
    /// A float: `"float"` for `f32`, `"double"` for `f64`, as `schemars` writes the format.
    Number {
        /// The format of the type.
        format: &'static str,
    },
    /// A string, with the `format` `schemars` writes for it (`uuid`, `date-time`), if any.
    String {
        /// The format of the type.
        format: Option<&'static str>,
    },
    /// Any JSON value: `serde_json::Value`.
    Any,
    /// `Option<T>`: `null` or the inner value.
    Option(&'static Self),
    /// A sequence of the inner shape.
    Seq(&'static Self),
    /// A map from strings to the inner shape.
    Map(&'static Self),
    /// A struct with named fields.
    Struct(&'static Struct),
    /// An enum.
    Enum(&'static Enum),
    /// A type whose serde shape the check cannot see (`#[serde(with = ..)]`,
    /// `#[serde(into = ..)]`, ..): the reason, for the message.
    Opaque(&'static str),
    /// A field marked `#[schema(unchecked)]`: only its presence and optionality are compared.
    Unchecked {
        /// Whether the field's type is an `Option`.
        option: bool,
    },
}

/// A struct with named fields.
#[derive(Debug, Clone, Copy)]
pub struct Struct {
    /// The type's name, for messages.
    pub name: &'static str,
    /// The fields in declaration order; a fully skipped field is left out.
    pub fields: &'static [Field],
    /// `#[serde(deny_unknown_fields)]`.
    pub deny_unknown_fields: bool,
}

/// One field of a struct or of a struct variant.
#[derive(Debug, Clone, Copy)]
pub struct Field {
    /// The property name serde gives it.
    pub name: &'static str,
    /// The field's shape.
    pub shape: &'static Shape,
    /// When serializing writes it.
    pub write: Write,
    /// How deserializing reads it.
    pub read: Read,
    /// `#[serde(flatten)]`: its own properties join the parent's.
    pub flatten: bool,
}

/// When serializing writes a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    /// On every value.
    Always,
    /// Unless `skip_serializing_if` says otherwise.
    Sometimes,
    /// Never: `skip_serializing`.
    Never,
}

/// How deserializing reads a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Read {
    /// An absent property takes the default: `#[serde(default)]` on the field or the struct.
    Default,
    /// An absent property is `None` when the field is an `Option`, and an error otherwise.
    Plain,
    /// An absent property is an error: `deserialize_with` reads the field.
    Strict,
    /// Never: `skip_deserializing`.
    Never,
}

/// An enum.
#[derive(Debug, Clone, Copy)]
pub struct Enum {
    /// The type's name, for messages.
    pub name: &'static str,
    /// How serde tags its variants.
    pub tagging: Tagging,
    /// The variants in declaration order.
    pub variants: &'static [Variant],
    /// `#[serde(deny_unknown_fields)]`, which applies to its struct variants.
    pub deny_unknown_fields: bool,
}

/// How serde writes the variant of an enum value.
#[derive(Debug, Clone, Copy)]
pub enum Tagging {
    /// `{"Variant": payload}`, a unit variant as the string `"Variant"`.
    External,
    /// `#[serde(tag = "..")]`: the tag property inside the payload's object.
    Internal {
        /// The tag property.
        tag: &'static str,
    },
    /// `#[serde(tag = "..", content = "..")]`.
    Adjacent {
        /// The tag property.
        tag: &'static str,
        /// The content property.
        content: &'static str,
    },
    /// `#[serde(untagged)]`.
    Untagged,
}

/// A variant of an enum.
#[derive(Debug, Clone, Copy)]
pub struct Variant {
    /// The name serde gives it.
    pub name: &'static str,
    /// What it carries.
    pub payload: Payload,
    /// Serializing writes it (no `skip_serializing`).
    pub write: bool,
    /// Deserializing reads it (no `skip_deserializing`).
    pub read: bool,
}

/// What a variant carries.
#[derive(Debug, Clone, Copy)]
pub enum Payload {
    /// Nothing.
    Unit,
    /// One unnamed value.
    Newtype(&'static Shape),
    /// Named fields.
    Struct(&'static [Field]),
}

impl Shape {
    /// The shape an `Option` of `inner` has. `serde_json` writes `Some(None)` and `None` alike,
    /// so an `Option` of an `Option` is one `Option`.
    #[must_use]
    pub const fn option(inner: &'static Self) -> Self {
        match inner {
            Self::Option(_) => *inner,
            _ => Self::Option(inner),
        }
    }

    /// The range of an integer type.
    #[must_use]
    pub const fn integer(name: &'static str, min: i128, max: i128) -> Self {
        Self::Integer { name, min, max }
    }

    /// Whether an absent property reads as this shape's `None`.
    pub(crate) const fn is_option(&self) -> bool {
        matches!(self, Self::Option(_) | Self::Unchecked { option: true })
    }
}
