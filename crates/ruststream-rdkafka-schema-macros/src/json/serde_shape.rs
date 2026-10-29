//! The type's serde shape as a constant: the `JsonShape` impl the derive emits, read from the
//! serde attributes with `serde_derive_internals`, the parser serde's own derive uses.

use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, quote, quote_spanned};
use serde_derive_internals::ast::{Container, Data, Field, Style, Variant};
use serde_derive_internals::attr::TagType;
use serde_derive_internals::{Ctxt, Derive};
use syn::parse::Parser;
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::{Attribute, DeriveInput, Ident, Type};

use super::{attrs, private};

/// The `JsonShape` impl of the type.
///
/// # Errors
///
/// Serde attributes serde itself refuses, and the constructs the check rejects: tuple structs and
/// tuple variants, a property renamed differently per direction, a field whose type is the type
/// itself.
pub(crate) fn shape_impl(input: &DeriveInput) -> syn::Result<TokenStream> {
    // `serde_derive_internals` reads the syntax tree of the next `syn`.
    let parsed: syn3::DeriveInput = syn3::parse2(input.to_token_stream()).map_err(from_syn3)?;
    let ctxt = Ctxt::new();
    let container = Container::from_ast(
        &ctxt,
        &parsed,
        Derive::Deserialize,
        &Ident::new("__ruststream_private", Span::call_site()),
    );
    ctxt.check().map_err(from_syn3)?;
    let container = container.ok_or_else(|| {
        syn::Error::new_spanned(input, "serde could not read this type's attributes")
    })?;
    let shape = container_shape(&container)?;
    Ok(impl_with(input, &shape))
}

/// A `JsonShape` that checks nothing, for a type whose own errors are already reported: the
/// types that hold it then compile on, instead of repeating that it has no shape.
pub(crate) fn fallback_impl(input: &DeriveInput) -> TokenStream {
    let private = private();
    impl_with(input, &quote!(#private::Shape::Unchecked { option: false }))
}

fn impl_with(input: &DeriveInput, shape: &TokenStream) -> TokenStream {
    let private = private();
    let ident = &input.ident;
    let mut generics = input.generics.clone();
    {
        let clause = generics.make_where_clause();
        for param in input.generics.type_params() {
            let param = &param.ident;
            clause
                .predicates
                .push(syn::parse_quote!(#param: #private::JsonShape));
        }
    }
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl #impl_generics #private::JsonShape for #ident #type_generics #where_clause {
            const SHAPE: #private::Shape = #shape;
        }
    }
}

/// The same errors, at the same spans.
fn from_syn3(errors: syn3::Error) -> syn::Error {
    errors
        .into_iter()
        .map(|error| syn::Error::new(error.span(), error))
        .reduce(|mut all, error| {
            all.combine(error);
            all
        })
        .unwrap_or_else(|| syn::Error::new(Span::call_site(), "serde could not read the type"))
}

/// A field's attributes in this crate's `syn`.
fn attributes(field: &Field<'_>) -> syn::Result<Vec<Attribute>> {
    let mut attrs = Vec::new();
    for attr in &field.original.attrs {
        attrs.extend(Attribute::parse_outer.parse2(attr.to_token_stream())?);
    }
    Ok(attrs)
}

/// A field's type in this crate's `syn`.
fn field_type(field: &Field<'_>) -> syn::Result<Type> {
    syn::parse2(field.ty.to_token_stream())
}

fn container_shape(container: &Container<'_>) -> syn::Result<TokenStream> {
    let private = private();
    let attrs = &container.attrs;
    for (present, attribute) in [
        (attrs.type_from().is_some(), "#[serde(from = ..)]"),
        (attrs.type_try_from().is_some(), "#[serde(try_from = ..)]"),
        (attrs.type_into().is_some(), "#[serde(into = ..)]"),
    ] {
        if present {
            return Ok(quote!(#private::Shape::Opaque(#attribute)));
        }
    }
    let name = container.ident.to_string();
    match &container.data {
        Data::Struct(Style::Struct, fields) => {
            if attrs.transparent() {
                return transparent(container, fields);
            }
            let fields = field_list(container, fields, true)?;
            let deny = attrs.deny_unknown_fields();
            Ok(quote! {
                #private::Shape::Struct(&#private::Struct {
                    name: #name,
                    fields: &[#(#fields),*],
                    deny_unknown_fields: #deny,
                })
            })
        }
        Data::Struct(Style::Newtype, fields) => transparent(container, fields),
        Data::Struct(Style::Tuple, _) => Err(syn::Error::new_spanned(
            &container.ident,
            format!(
                "`{name}` is a tuple struct, which serde writes as an array the schema check \
                 does not compare; give its fields names"
            ),
        )),
        Data::Struct(Style::Unit, _) => Ok(quote!(#private::Shape::Opaque("a unit struct"))),
        Data::Enum(variants) => {
            let tagging = match attrs.tag() {
                TagType::External => quote!(#private::Tagging::External),
                TagType::Internal { tag } => quote!(#private::Tagging::Internal { tag: #tag }),
                TagType::Adjacent { tag, content } => {
                    quote!(#private::Tagging::Adjacent { tag: #tag, content: #content })
                }
                TagType::None => quote!(#private::Tagging::Untagged),
            };
            let variants = variants
                .iter()
                .map(|variant| variant_shape(container, variant))
                .collect::<syn::Result<Vec<_>>>()?;
            let deny = attrs.deny_unknown_fields();
            Ok(quote! {
                #private::Shape::Enum(&#private::Enum {
                    name: #name,
                    tagging: #tagging,
                    variants: &[#(#variants),*],
                    deny_unknown_fields: #deny,
                })
            })
        }
    }
}

/// A `#[serde(transparent)]` struct or a newtype struct: serde writes the inner value alone.
fn transparent(container: &Container<'_>, fields: &[Field<'_>]) -> syn::Result<TokenStream> {
    let field = fields
        .iter()
        .find(|field| field.attrs.transparent() || fields.len() == 1)
        .ok_or_else(|| syn::Error::new_spanned(&container.ident, "no field carries the value"))?;
    field_type_shape(container, field)
}

fn variant_shape(container: &Container<'_>, variant: &Variant<'_>) -> syn::Result<TokenStream> {
    let private = private();
    let names = variant.attrs.name();
    let name = names.serialize_name().value.clone();
    if names.deserialize_name().value != name {
        return Err(syn::Error::new_spanned(
            &variant.ident,
            format!(
                "variant `{}` is renamed differently for serializing and deserializing, and one \
                 oneOf branch cannot carry two names",
                variant.ident
            ),
        ));
    }
    if variant.attrs.untagged() {
        return Err(syn::Error::new_spanned(
            &variant.ident,
            "an untagged variant is matched by trying it, while oneOf demands exactly one match; \
             the schema check rejects it",
        ));
    }
    let payload = match variant.style {
        Style::Unit => quote!(#private::Payload::Unit),
        Style::Newtype => {
            let shape = if variant.attrs.serialize_with().is_some()
                || variant.attrs.deserialize_with().is_some()
            {
                quote!(&#private::Shape::Opaque("#[serde(with = ..)]"))
            } else {
                let shape = field_type_shape(container, &variant.fields[0])?;
                quote!(&#shape)
            };
            quote!(#private::Payload::Newtype(#shape))
        }
        Style::Struct => {
            let fields = field_list(container, &variant.fields, false)?;
            quote!(#private::Payload::Struct(&[#(#fields),*]))
        }
        Style::Tuple => {
            return Err(syn::Error::new_spanned(
                &variant.ident,
                format!(
                    "variant `{}` is a tuple variant, which serde writes as an array the schema \
                     check does not compare; give its fields names",
                    variant.ident
                ),
            ));
        }
    };
    let write = !variant.attrs.skip_serializing();
    let read = !variant.attrs.skip_deserializing();
    Ok(quote! {
        #private::Variant { name: #name, payload: #payload, write: #write, read: #read }
    })
}

/// The `Field` constants of the named fields serde reads or writes.
fn field_list(
    container: &Container<'_>,
    fields: &[Field<'_>],
    struct_default: bool,
) -> syn::Result<Vec<TokenStream>> {
    let private = private();
    let container_default = struct_default && !container.attrs.default().is_none();
    let mut list = Vec::new();
    for field in fields {
        let attrs = &field.attrs;
        if attrs.skip_serializing() && attrs.skip_deserializing() {
            continue;
        }
        let names = attrs.name();
        let name = names.serialize_name().value.clone();
        if names.deserialize_name().value != name {
            return Err(syn::Error::new_spanned(
                field.original,
                "this field is renamed differently for serializing and deserializing, and one \
                 property cannot carry two names",
            ));
        }
        let write = if attrs.skip_serializing() {
            quote!(#private::Write::Never)
        } else if attrs.skip_serializing_if().is_some() {
            quote!(#private::Write::Sometimes)
        } else {
            quote!(#private::Write::Always)
        };
        let read = if attrs.skip_deserializing() {
            quote!(#private::Read::Never)
        } else if container_default || !attrs.default().is_none() {
            quote!(#private::Read::Default)
        } else if attrs.deserialize_with().is_some() {
            quote!(#private::Read::Strict)
        } else {
            quote!(#private::Read::Plain)
        };
        let shape = field_shape(container, field)?;
        let flatten = attrs.flatten();
        list.push(quote! {
            #private::Field {
                name: #name,
                shape: &#shape,
                write: #write,
                read: #read,
                flatten: #flatten,
            }
        });
    }
    Ok(list)
}

/// The shape of one field: its type's constant, or what its attributes make of it.
fn field_shape(container: &Container<'_>, field: &Field<'_>) -> syn::Result<TokenStream> {
    let private = private();
    if attrs::unchecked(&attributes(field)?)? {
        let option = is_option(&field_type(field)?);
        return Ok(quote!(#private::Shape::Unchecked { option: #option }));
    }
    if field.attrs.serialize_with().is_some() || field.attrs.deserialize_with().is_some() {
        return Ok(quote!(#private::Shape::Opaque("#[serde(with = ..)]")));
    }
    field_type_shape(container, field)
}

/// The constant of the field's type, spanned at the type so a missing impl points at it.
fn field_type_shape(container: &Container<'_>, field: &Field<'_>) -> syn::Result<TokenStream> {
    let private = private();
    let ty = field_type(field)?;
    if attrs::unchecked(&attributes(field)?)? {
        let option = is_option(&ty);
        return Ok(quote!(#private::Shape::Unchecked { option: #option }));
    }
    if mentions(&ty, &container.original.ident.to_string()) {
        return Err(syn::Error::new_spanned(
            &ty,
            format!(
                "this field holds `{}` again, and the schema check rejects a recursive type; \
                 mark the field `#[schema(unchecked)]`",
                container.ident
            ),
        ));
    }
    Ok(quote_spanned!(ty.span()=> <#ty as #private::JsonShape>::SHAPE))
}

/// Whether the type is written `Option<..>`.
fn is_option(ty: &Type) -> bool {
    matches!(
        ty,
        Type::Path(path) if path.path.segments.last().is_some_and(|last| last.ident == "Option")
    )
}

/// Whether the type names `ident` or `Self` anywhere.
fn mentions(ty: &Type, ident: &str) -> bool {
    struct Finder<'a> {
        ident: &'a str,
        found: bool,
    }
    impl<'ast> Visit<'ast> for Finder<'_> {
        fn visit_ident(&mut self, ident: &'ast Ident) {
            if ident == self.ident || ident == "Self" {
                self.found = true;
            }
        }
    }
    let mut finder = Finder {
        ident,
        found: false,
    };
    finder.visit_type(ty);
    finder.found
}

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::*;

    fn expand(input: &DeriveInput) -> String {
        shape_impl(input)
            .expect("the type has a shape")
            .to_string()
            .replace(' ', "")
    }

    #[test]
    fn serde_names_and_defaults_reach_the_fields() {
        let input: DeriveInput = parse_quote! {
            #[serde(rename_all = "camelCase")]
            struct Order {
                order_id: i64,
                #[serde(default)]
                priority: i32,
                #[serde(skip_serializing_if = "Option::is_none")]
                note: Option<String>,
                #[serde(skip)]
                cache: Cache,
            }
        };
        let code = expand(&input);
        assert!(code.contains(r#"name:"orderId""#), "{code}");
        assert!(code.contains("Read::Default"), "{code}");
        assert!(code.contains("Write::Sometimes"), "{code}");
        assert!(
            !code.contains("Cache"),
            "a skipped field is left out: {code}"
        );
    }

    #[test]
    fn a_tuple_struct_is_refused() {
        let input: DeriveInput = parse_quote! { struct Pair(i64, i64); };
        let err = shape_impl(&input).expect_err("a tuple struct writes an array");
        assert!(err.to_string().contains("tuple struct"), "{err}");
    }

    #[test]
    fn a_newtype_struct_is_its_content() {
        let input: DeriveInput = parse_quote! { struct Sku(String); };
        let code = expand(&input);
        assert!(code.contains("constSHAPE:"), "{code}");
        assert!(code.contains("<Stringas"), "{code}");
    }

    #[test]
    fn a_property_with_two_names_is_refused() {
        let input: DeriveInput = parse_quote! {
            struct Order { #[serde(rename(serialize = "a", deserialize = "b"))] id: i64 }
        };
        let err = shape_impl(&input).expect_err("one property, two names");
        assert!(err.to_string().contains("two names"), "{err}");
    }

    #[test]
    fn a_recursive_field_is_refused_unless_unchecked() {
        let input: DeriveInput = parse_quote! { struct Node { next: Option<Box<Node>> } };
        let err = shape_impl(&input).expect_err("a recursive type has no finite shape");
        assert!(err.to_string().contains("recursive"), "{err}");
        let input: DeriveInput =
            parse_quote! { struct Node { #[schema(unchecked)] next: Option<Box<Node>> } };
        let code = expand(&input);
        assert!(
            code.contains("Unchecked") && code.contains("option:true"),
            "{code}"
        );
    }

    #[test]
    fn a_generic_type_bounds_its_parameters() {
        let input: DeriveInput = parse_quote! { struct Page<Item> { items: Vec<Item> } };
        let code = expand(&input);
        assert!(code.contains("Item:::ruststream_rdkafka"), "{code}");
    }

    #[test]
    fn tuple_and_untagged_variants_are_refused() {
        let input: DeriveInput = parse_quote! { enum Shape { Point(i64, i64) } };
        let err = shape_impl(&input).expect_err("a tuple variant writes an array");
        assert!(err.to_string().contains("tuple variant"), "{err}");
        let input: DeriveInput = parse_quote! { enum Shape { #[serde(untagged)] Any(String) } };
        let err = shape_impl(&input).expect_err("an untagged variant is tried");
        assert!(err.to_string().contains("untagged"), "{err}");
    }
}
