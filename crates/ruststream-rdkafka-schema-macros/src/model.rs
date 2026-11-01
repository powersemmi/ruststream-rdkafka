//! The derived type as `apache_avro`'s `AvroSchema` derive (0.22) sees it: the record's name,
//! the fields with their names on the wire, and the Avro attributes that change a field's form.
//!
//! Serde's attributes are read by `serde_derive_internals`, the parser serde's own derive uses,
//! so `rename`, `rename_all`, `skip` and `flatten` mean what serde makes of them; the `AvroSchema`
//! derive reads the same attributes with the same meaning.

use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use serde_derive_internals::{Ctxt, Derive, ast};
use serde_json::Value;
use syn::spanned::Spanned;
use syn::{Attribute, DeriveInput, Expr, ExprLit, Ident, Lit, LitStr, Type};

use crate::json_text;

/// The derived type.
pub(crate) struct Model {
    pub(crate) ident: Ident,
    pub(crate) generics: syn::Generics,
    pub(crate) generic: bool,
    /// The name `AvroSchema` gives the type: its namespace (`#[avro(namespace = ..)]`, or the
    /// leading part of a dotted name) and the name (serde's `rename`, or the identifier).
    pub(crate) namespace: Option<String>,
    pub(crate) name: String,
    /// The container's `#[avro(default = ..)]`, normalized: the default a field of this type
    /// takes when it names none.
    pub(crate) default: Option<String>,
    pub(crate) shape: ModelShape,
}

/// What the type is to Avro.
pub(crate) enum ModelShape {
    /// A struct with named fields: a record.
    Record(Vec<FieldModel>),
    /// `#[serde(transparent)]`: the form of its one field.
    Transparent(Type),
    /// An enum of unit variants: an Avro enum.
    Enum {
        symbols: Vec<(String, Span)>,
        default: Option<String>,
    },
    /// An enum whose variants each wrap one model: a set of input models.
    Set(Vec<(Ident, Type)>),
}

/// One field of a record.
pub(crate) struct FieldModel {
    /// The field's name on the wire.
    pub(crate) name: String,
    pub(crate) ty: Type,
    pub(crate) span: Span,
    pub(crate) form: FieldForm,
    pub(crate) default: FieldDefault,
}

/// How a field contributes to its record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldForm {
    /// The form of its type.
    Typed,
    /// `#[avro(with = ..)]`: a run-time function.
    With,
    /// `#[serde(flatten)]`: its type's fields join the record.
    Flatten,
}

/// Where a field's default comes from, as `AvroSchema` decides it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FieldDefault {
    /// The type's own default (`null` for an `Option`).
    OfType,
    /// None: `#[avro(default = false)]`, or a `with` field that names none.
    Disabled,
    /// `#[avro(default = "..")]`, normalized.
    Json(String),
}

impl Model {
    /// The record's full name as `AvroSchema::get_schema()` writes it for the type on its own.
    pub(crate) fn full_name(&self) -> String {
        match &self.namespace {
            Some(namespace) if !namespace.is_empty() => format!("{namespace}.{}", self.name),
            _ => self.name.clone(),
        }
    }

    /// Reads the type.
    ///
    /// # Errors
    ///
    /// A shape the derive does not check (a union, a tuple struct, a data-carrying enum that is
    /// not a set of models), or an attribute that does not parse.
    pub(crate) fn read(input: &DeriveInput) -> syn::Result<Self> {
        let ctxt = Ctxt::new();
        let container = ast::Container::from_ast(&ctxt, input, Derive::Serialize);
        ctxt.check()?;
        let Some(container) = container else {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "the Schema derive could not read the type's serde attributes",
            ));
        };
        let avro = AvroAttrs::read(&input.attrs)?;
        let serde_name = container.attrs.name().serialize_name().to_owned();
        let (namespace, name) = match (avro.namespace, serde_name.rsplit_once('.')) {
            (_, Some((space, name))) => (Some(space.to_owned()), name.to_owned()),
            (namespace, None) => (namespace, serde_name),
        };
        let default = avro.default.as_ref().map(normalized_json).transpose()?;
        let shape = match &container.data {
            ast::Data::Struct(_, fields) if container.attrs.transparent() => {
                let field = fields
                    .iter()
                    .find(|field| !skipped(&field.attrs))
                    .ok_or_else(|| {
                        syn::Error::new_spanned(&input.ident, "a transparent struct has a field")
                    })?;
                ModelShape::Transparent(field.ty.clone())
            }
            ast::Data::Struct(ast::Style::Struct | ast::Style::Unit, fields) => {
                ModelShape::Record(read_fields(fields)?)
            }
            ast::Data::Struct(ast::Style::Tuple | ast::Style::Newtype, _) => {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "the Schema derive checks a record with named fields, a unit-variant enum, or \
                     a set of input models; a tuple struct is none of them",
                ));
            }
            ast::Data::Enum(variants) => read_enum(&input.ident, variants, default.as_deref())?,
        };
        Ok(Self {
            ident: input.ident.clone(),
            generics: input.generics.clone(),
            generic: !input.generics.params.is_empty(),
            namespace,
            name,
            default,
            shape,
        })
    }
}

fn skipped(attrs: &serde_derive_internals::attr::Field) -> bool {
    attrs.skip_serializing() && attrs.skip_deserializing()
}

fn read_fields(fields: &[ast::Field<'_>]) -> syn::Result<Vec<FieldModel>> {
    let mut out = Vec::new();
    for field in fields {
        if skipped(&field.attrs) {
            continue;
        }
        let avro = AvroAttrs::read(&field.original.attrs)?;
        let form = if field.attrs.flatten() {
            FieldForm::Flatten
        } else if avro.with {
            FieldForm::With
        } else {
            FieldForm::Typed
        };
        let default = match (&avro.default, avro.default_disabled, form) {
            (Some(lit), _, _) => FieldDefault::Json(normalized_json(lit)?),
            (None, true, _) | (None, false, FieldForm::With) => FieldDefault::Disabled,
            (None, false, _) => FieldDefault::OfType,
        };
        out.push(FieldModel {
            name: field.attrs.name().serialize_name().to_owned(),
            ty: field.ty.clone(),
            span: field.ty.span(),
            form,
            default,
        });
    }
    Ok(out)
}

fn read_enum(
    ident: &Ident,
    variants: &[ast::Variant<'_>],
    default: Option<&str>,
) -> syn::Result<ModelShape> {
    let kept: Vec<_> = variants
        .iter()
        .filter(|variant| !(variant.attrs.skip_serializing() && variant.attrs.skip_deserializing()))
        .collect();
    if kept
        .iter()
        .all(|variant| matches!(variant.style, ast::Style::Unit))
    {
        let mut symbols = Vec::new();
        let mut symbol_default = match default.map(serde_json::from_str::<Value>) {
            Some(Ok(Value::String(symbol))) => Some(symbol),
            _ => None,
        };
        for variant in &kept {
            let symbol = variant.attrs.name().serialize_name().to_owned();
            if symbol_default.is_none()
                && variant
                    .original
                    .attrs
                    .iter()
                    .any(|attr| attr.path().is_ident("default"))
            {
                symbol_default = Some(symbol.clone());
            }
            symbols.push((symbol, variant.original.ident.span()));
        }
        return Ok(ModelShape::Enum {
            symbols,
            default: symbol_default,
        });
    }
    if kept
        .iter()
        .all(|variant| matches!(variant.style, ast::Style::Newtype))
    {
        return Ok(ModelShape::Set(
            kept.iter()
                .map(|variant| (variant.ident.clone(), variant.fields[0].ty.clone()))
                .collect(),
        ));
    }
    Err(syn::Error::new_spanned(
        ident,
        "the Schema derive checks an enum of unit variants (an Avro enum) or a set of input \
         models (every variant wraps one model); this enum is neither",
    ))
}

/// The Avro attributes the derive reads; the others are `AvroSchema`'s alone.
#[derive(Default)]
struct AvroAttrs {
    namespace: Option<String>,
    default: Option<LitStr>,
    default_disabled: bool,
    with: bool,
}

impl AvroAttrs {
    fn read(attrs: &[Attribute]) -> syn::Result<Self> {
        let mut out = Self::default();
        for attr in attrs.iter().filter(|attr| attr.path().is_ident("avro")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("namespace") {
                    out.namespace = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("default") {
                    match meta.value()?.parse::<Expr>()? {
                        Expr::Lit(ExprLit {
                            lit: Lit::Str(lit), ..
                        }) => out.default = Some(lit),
                        Expr::Lit(ExprLit {
                            lit: Lit::Bool(lit),
                            ..
                        }) if !lit.value => out.default_disabled = true,
                        other => {
                            return Err(syn::Error::new_spanned(
                                other.into_token_stream(),
                                "`#[avro(default = ..)]` takes a JSON string or `false`",
                            ));
                        }
                    }
                } else if meta.path.is_ident("with") {
                    out.with = true;
                    if meta.input.peek(syn::Token![=]) {
                        meta.value()?.parse::<Expr>()?;
                    }
                } else if meta.input.peek(syn::Token![=]) {
                    meta.value()?.parse::<Expr>()?;
                } else if meta.input.peek(syn::token::Paren) {
                    meta.input.parse::<TokenStream>()?;
                }
                Ok(())
            })?;
        }
        Ok(out)
    }
}

/// A default written in an attribute, normalized the way a migration's default is.
fn normalized_json(lit: &LitStr) -> syn::Result<String> {
    let value: Value = serde_json::from_str(&lit.value()).map_err(|err| {
        syn::Error::new_spanned(lit, format!("the Avro default is not JSON: {err}"))
    })?;
    Ok(json_text(&value))
}

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::*;

    #[test]
    fn the_full_name_is_the_namespace_and_serdes_name() {
        let input: DeriveInput = parse_quote! {
            #[avro(namespace = "com.acme.orders")]
            #[serde(rename = "PlacedOrder", rename_all = "camelCase")]
            struct Order {
                order_id: i64,
                #[serde(skip)]
                cache: u8,
                #[avro(default = "{\"b\": 1, \"a\": null}")]
                extra: Extra,
                #[avro(with = path::to)]
                when: Instant,
                #[serde(flatten)]
                audit: Audit,
                note: Option<String>,
            }
        };
        let model = Model::read(&input).expect("the struct reads");
        assert_eq!(model.full_name(), "com.acme.orders.PlacedOrder");
        let ModelShape::Record(fields) = model.shape else {
            panic!("a struct with named fields is a record");
        };
        let seen: Vec<_> = fields
            .iter()
            .map(|field| (field.name.as_str(), field.form, field.default.clone()))
            .collect();
        assert_eq!(
            seen,
            [
                ("orderId", FieldForm::Typed, FieldDefault::OfType),
                (
                    "extra",
                    FieldForm::Typed,
                    FieldDefault::Json(r#"{"a":null,"b":1}"#.to_owned())
                ),
                ("when", FieldForm::With, FieldDefault::Disabled),
                ("audit", FieldForm::Flatten, FieldDefault::OfType),
                ("note", FieldForm::Typed, FieldDefault::OfType),
            ]
        );
    }

    #[test]
    fn a_name_without_namespace_is_bare_and_a_dotted_rename_carries_one() {
        let bare: DeriveInput = parse_quote!(
            struct Item {
                sku: String,
            }
        );
        assert_eq!(Model::read(&bare).expect("reads").full_name(), "Item");
        let dotted: DeriveInput = parse_quote!(
            #[serde(rename = "com.acme.Item")]
            struct Item {
                sku: String,
            }
        );
        let model = Model::read(&dotted).expect("reads");
        assert_eq!(model.namespace.as_deref(), Some("com.acme"));
        assert_eq!(model.full_name(), "com.acme.Item");
    }

    #[test]
    fn a_unit_enum_lists_its_symbols_after_renames() {
        let input: DeriveInput = parse_quote! {
            #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
            enum Status {
                Placed,
                #[default]
                OnHold,
                #[serde(skip)]
                Internal,
            }
        };
        let ModelShape::Enum { symbols, default } = Model::read(&input).expect("reads").shape
        else {
            panic!("unit variants make an Avro enum");
        };
        let names: Vec<_> = symbols.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, ["PLACED", "ON_HOLD"]);
        assert_eq!(default.as_deref(), Some("ON_HOLD"));
    }

    #[test]
    fn newtype_variants_make_a_set_and_mixed_variants_are_refused() {
        let set: DeriveInput = parse_quote! {
            enum OrderEvent {
                Created(OrderCreated),
                Cancelled(OrderCancelled),
            }
        };
        assert!(matches!(
            Model::read(&set).expect("reads").shape,
            ModelShape::Set(variants) if variants.len() == 2
        ));
        let mixed: DeriveInput = parse_quote! {
            enum Mixed {
                Unit,
                Created(OrderCreated),
            }
        };
        let err = Model::read(&mixed).err().expect("a mixed enum is refused");
        assert!(err.to_string().contains("this enum is neither"), "{err}");
    }

    #[test]
    fn a_tuple_struct_is_refused() {
        let input: DeriveInput = parse_quote!(
            struct Pair(i64, i64);
        );
        let err = Model::read(&input)
            .err()
            .expect("a tuple struct is refused");
        assert!(
            err.to_string().contains("a tuple struct is none of them"),
            "{err}"
        );
    }
}
