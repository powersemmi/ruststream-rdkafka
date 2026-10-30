//! The `#[schema(..)]` attributes the JSON check reads: `namespace` on the type, `unchecked` on a
//! field.

use syn::{Attribute, LitStr};

/// `#[schema(namespace = "..")]` on the type: the namespace of its record, if it carries one.
///
/// # Errors
///
/// A `#[schema(..)]` key the type does not take, or a namespace that is not a string.
pub(crate) fn namespace(attrs: &[Attribute]) -> syn::Result<Option<LitStr>> {
    let mut namespace = None;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("schema")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("namespace") {
                namespace = Some(meta.value()?.parse::<LitStr>()?);
                Ok(())
            } else {
                Err(meta.error(
                    "a type takes `#[schema(namespace = \"..\")]`; `unchecked` goes on a field",
                ))
            }
        })?;
    }
    Ok(namespace)
}

/// Whether a field carries `#[schema(unchecked)]`.
///
/// # Errors
///
/// A `#[schema(..)]` key a field does not take.
pub(crate) fn unchecked(attrs: &[Attribute]) -> syn::Result<bool> {
    let mut unchecked = false;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("schema")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("unchecked") {
                unchecked = true;
                Ok(())
            } else {
                Err(meta
                    .error("a field takes `#[schema(unchecked)]`; `namespace` goes on the type"))
            }
        })?;
    }
    Ok(unchecked)
}

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::*;

    #[test]
    fn the_namespace_is_read_from_the_type() {
        let input: syn::DeriveInput = parse_quote! {
            #[schema(namespace = "com.acme.orders")]
            struct Order { id: i64 }
        };
        let namespace = namespace(&input.attrs).expect("a namespace parses");
        assert_eq!(
            namespace.map(|lit| lit.value()),
            Some("com.acme.orders".to_owned())
        );
    }

    #[test]
    fn a_field_key_on_the_type_is_refused() {
        let input: syn::DeriveInput = parse_quote! {
            #[schema(unchecked)]
            struct Order { id: i64 }
        };
        let err = namespace(&input.attrs).expect_err("unchecked belongs on a field");
        assert!(err.to_string().contains("goes on a field"), "{err}");
    }

    #[test]
    fn unchecked_is_read_from_a_field() {
        let field: syn::Field = parse_quote! { #[schema(unchecked)] next: Option<Box<Node>> };
        assert!(unchecked(&field.attrs).expect("unchecked parses"));
        let field: syn::Field = parse_quote! { #[schema(namespace = "x")] next: u8 };
        let err = unchecked(&field.attrs).expect_err("a namespace belongs on the type");
        assert!(err.to_string().contains("goes on the type"), "{err}");
    }
}
