//! A JSON Schema as the check reads it: the derive turns the newest migration of a subject into a
//! constant of these types, with every `$ref` resolved and every construct the check does not
//! prove kept as a [`Node::Rejected`] at its place.

/// One schema.
#[derive(Debug, Clone, Copy)]
pub enum Node {
    /// `{}` or `true`: any value.
    Any,
    /// `false`: no value.
    Never,
    /// `{"type": "null"}`.
    Null,
    /// `{"type": "boolean"}`.
    Bool,
    /// `{"type": "integer"}`, by the range its `format`, `minimum` and `maximum` leave.
    Integer {
        /// The least value.
        min: i128,
        /// The greatest value.
        max: i128,
    },
    /// `{"type": "number"}`, with its `format` (`float`, `double`) if any.
    Number {
        /// The format.
        format: Option<&'static str>,
    },
    /// `{"type": "string"}`, with its `format` if any.
    String {
        /// The format.
        format: Option<&'static str>,
    },
    /// `{"enum": [..]}` of strings.
    Strings(&'static [&'static str]),
    /// `{"const": ".."}`: legal as an enum tag only.
    Const {
        /// The string.
        value: &'static str,
        /// Where the keyword stands, for messages.
        pointer: &'static str,
    },
    /// `{"type": [T, "null"]}` or `{"anyOf": [T, {"type": "null"}]}`.
    Nullable(&'static Self),
    /// `{"type": "array", "items": ..}`.
    Array(&'static Self),
    /// `{"type": "object", ..}`.
    Object(&'static Object),
    /// `{"oneOf": [..]}`.
    OneOf {
        /// The branches.
        branches: &'static [Self],
        /// Where the keyword stands, for messages.
        pointer: &'static str,
    },
    /// A construct a Rust type cannot promise: the keyword and the JSON pointer to it.
    Rejected {
        /// The keyword.
        keyword: &'static str,
        /// Where it stands.
        pointer: &'static str,
    },
}

/// An object schema.
#[derive(Debug, Clone, Copy)]
pub struct Object {
    /// `properties`, with `required` folded in.
    pub properties: &'static [Property],
    /// `additionalProperties`.
    pub additional: Additional,
    /// Where the object stands, for messages.
    pub pointer: &'static str,
}

/// One declared property.
#[derive(Debug, Clone, Copy)]
pub struct Property {
    /// Its name.
    pub name: &'static str,
    /// Its schema.
    pub node: &'static Node,
    /// Listed in `required`.
    pub required: bool,
}

/// What an object says about properties it does not declare.
#[derive(Debug, Clone, Copy)]
pub enum Additional {
    /// `additionalProperties` absent or `true`: any.
    Open,
    /// `additionalProperties: false`: none.
    Closed,
    /// `additionalProperties: {schema}`: each under that schema.
    Schema(&'static Node),
}

impl Object {
    /// The declared property called `name`.
    pub(crate) const fn property(&self, name: &str) -> Option<&'static Property> {
        let mut index = 0;
        while index < self.properties.len() {
            if str_eq(self.properties[index].name, name) {
                return Some(&self.properties[index]);
            }
            index += 1;
        }
        None
    }
}

impl Node {
    /// The string a tag schema fixes: a `const`, or an `enum` of one string.
    pub(crate) const fn tag_value(&self) -> Option<&'static str> {
        match self {
            Self::Const { value, .. } | Self::Strings([value]) => Some(value),
            _ => None,
        }
    }
}

/// String equality a `const fn` can call.
pub(crate) const fn str_eq(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_compare_byte_for_byte() {
        assert!(str_eq("orderId", "orderId"));
        assert!(!str_eq("orderId", "order_id"));
        assert!(!str_eq("a", "ab"));
    }

    #[test]
    fn a_tag_is_a_const_or_an_enum_of_one() {
        let pointer = "/oneOf/0/properties/kind/const";
        assert_eq!(
            Node::Const {
                value: "Web",
                pointer
            }
            .tag_value(),
            Some("Web")
        );
        assert_eq!(Node::Strings(&["Web"]).tag_value(), Some("Web"));
        assert_eq!(Node::Strings(&["Web", "Store"]).tag_value(), None);
        assert_eq!(Node::String { format: None }.tag_value(), None);
    }
}
