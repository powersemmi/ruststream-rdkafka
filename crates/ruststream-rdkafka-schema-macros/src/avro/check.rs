//! The checks of a type against one migration: per-field bounds that name the site, field-set
//! errors at the field's span, and the whole-schema comparison in a constant.

use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};

use super::emit::{self, private};
use super::parse::{Avro, Enum, Record};
use super::site_ident;
use crate::model::{FieldForm, FieldModel, Model, ModelShape};

/// The type, the subject and the migration a check runs against.
pub(crate) struct Site<'a> {
    pub(crate) model: &'a Model,
    pub(crate) subject: &'a str,
    pub(crate) migration: &'a str,
    /// An external subject: the type reads every version under Avro's resolution rules.
    pub(crate) external: bool,
}

impl Site<'_> {
    fn span(&self) -> Span {
        self.model.ident.span()
    }

    fn at(&self) -> String {
        format!(
            "of subject `{}` (migration {})",
            self.subject, self.migration
        )
    }
}

/// Every check of the type against the named type `def` the migration declares under the
/// type's full name.
pub(crate) fn checks(site: &Site<'_>, def: &Avro) -> syn::Result<TokenStream> {
    match (&site.model.shape, def) {
        (ModelShape::Record(fields), Avro::Record(record)) => record_checks(site, fields, record, def),
        (ModelShape::Enum { symbols, default }, Avro::Enum(def)) => {
            enum_checks(site, symbols, default.as_deref(), def)
        }
        _ => Err(syn::Error::new(
            site.span(),
            format!(
                "`{}` is a {}, and subject `{}` (migration {}) declares `{}` as {}",
                site.model.ident,
                match site.model.shape {
                    ModelShape::Enum { .. } => "unit-variant enum (an Avro enum)",
                    _ => "struct (an Avro record)",
                },
                site.subject,
                site.migration,
                site.model.full_name(),
                def.describe()
            ),
        )),
    }
}

fn record_checks(
    site: &Site<'_>,
    fields: &[FieldModel],
    record: &Record,
    def: &Avro,
) -> syn::Result<TokenStream> {
    let p = private();
    let model = site.model;
    let full = model.full_name();
    let mut errors: Vec<syn::Error> = Vec::new();
    let mut bounds = TokenStream::new();
    let mut defaults = TokenStream::new();
    let flattens = fields.iter().any(|field| field.form == FieldForm::Flatten);
    for field in fields.iter().filter(|field| field.form != FieldForm::Flatten) {
        let declared = record.fields.iter().find(|declared| declared.name == field.name);
        match (declared, site.external) {
            (None, false) => errors.push(syn::Error::new(
                field.span,
                format!(
                    "field `{}` of `{}` is not in record `{full}` {}",
                    field.name,
                    model.ident,
                    site.at()
                ),
            )),
            (None, true) => match field.default.known() {
                Some(Some(_)) => {}
                Some(None) => errors.push(syn::Error::new(
                    field.span,
                    format!(
                        "field `{}` of `{}` is not in record `{full}` {}, and it has no \
                         default; a reader needs one for a field the writer lacks",
                        field.name,
                        model.ident,
                        site.at()
                    ),
                )),
                None => {
                    let ty = &field.ty;
                    let message = format!(
                        "field `{}` of `{}` is not in record `{full}` {}, and its type gives it \
                         no default; a reader needs one for a field the writer lacks (add \
                         `#[avro(default = ..)]` or make it an `Option`)",
                        field.name,
                        model.ident,
                        site.at()
                    );
                    defaults.extend(quote_spanned! {field.span=>
                        const _: () = ::core::assert!(
                            <#ty as #p::AvroForm>::FIELD_DEFAULT.is_some(),
                            #message,
                        );
                    });
                }
            },
            (Some(declared), external) if field.form == FieldForm::Typed => {
                let ty = &field.ty;
                let kind = emit::kind(&declared.ty);
                let site_ty = site_ident(model, &field.name, site.subject, site.migration);
                let bound = if external {
                    quote!(#p::Reads)
                } else {
                    quote!(#p::Carries)
                };
                bounds.extend(quote! {
                    #[allow(non_camel_case_types)]
                    struct #site_ty;
                    const _: fn() = || {
                        fn field<__Field: ?::core::marker::Sized + #bound<__Kind, __Site>, __Kind, __Site>() {}
                        field::<#ty, #kind, #site_ty>();
                    };
                });
            }
            (Some(_), _) => {}
        }
    }
    if !site.external && !flattens {
        for declared in &record.fields {
            if fields.iter().any(|field| field.name == declared.name) {
                continue;
            }
            let how = match &declared.default {
                None => "without a default".to_owned(),
                Some(default) => format!(
                    "with the default `{default}` (migration {}); the type of an owned \
                     subject has every field of its newest migration",
                    site.migration
                ),
            };
            errors.push(syn::Error::new(
                site.span(),
                format!(
                    "`{}` has no field `{}`, which record `{full}` of subject `{}` declares {how}",
                    model.ident, declared.name, site.subject
                ),
            ));
        }
    }
    if let Some(error) = errors.into_iter().reduce(|mut all, next| {
        all.combine(next);
        all
    }) {
        return Err(error);
    }
    let compare = compare(site, def);
    Ok(quote! {
        #bounds
        #defaults
        #compare
    })
}

/// The whole-schema comparison, in a constant whose failure names the nested record that
/// differs. A recursive record is left to the migrations tool.
fn compare(site: &Site<'_>, def: &Avro) -> TokenStream {
    if def.is_recursive() {
        return TokenStream::new();
    }
    let p = private();
    let model = site.model;
    let ident = &model.ident;
    let form = emit::form(def);
    let (type_name, subject, migration) = (ident.to_string(), site.subject, site.migration);
    let assert = if site.external {
        quote!(#p::assert_reads)
    } else {
        quote!(#p::assert_equals)
    };
    quote_spanned! {ident.span()=>
        const _: () = #assert(
            <#ident as #p::AvroForm>::FORM,
            #form,
            #p::Site { type_name: #type_name, subject: #subject, migration: #migration },
        );
    }
}

fn enum_checks(
    site: &Site<'_>,
    symbols: &[(String, Span)],
    default: Option<&str>,
    def: &Enum,
) -> syn::Result<TokenStream> {
    let model = site.model;
    let full = model.full_name();
    let mut errors: Vec<syn::Error> = Vec::new();
    if !site.external {
        for (symbol, span) in symbols {
            if !def.symbols.contains(symbol) {
                errors.push(syn::Error::new(
                    *span,
                    format!(
                        "variant `{symbol}` of `{}` is not a symbol of enum `{full}` {}",
                        model.ident,
                        site.at()
                    ),
                ));
            }
        }
    }
    for symbol in &def.symbols {
        if !symbols.iter().any(|(own, _)| own == symbol) {
            errors.push(syn::Error::new(
                site.span(),
                format!(
                    "`{}` has no variant for the symbol `{symbol}`, which enum `{full}` {} declares",
                    model.ident,
                    site.at()
                ),
            ));
        }
    }
    if errors.is_empty() && !site.external {
        let own: Vec<&String> = symbols.iter().map(|(symbol, _)| symbol).collect();
        let declared: Vec<&String> = def.symbols.iter().collect();
        if own != declared {
            errors.push(syn::Error::new(
                site.span(),
                format!(
                    "the variants of `{}` are not in the order of the symbols of enum `{full}` \
                     {}; Avro writes a symbol as its position",
                    model.ident,
                    site.at()
                ),
            ));
        }
        if default != def.default.as_deref() {
            errors.push(syn::Error::new(
                site.span(),
                format!(
                    "the default symbol of `{}` is {}, and enum `{full}` {} declares {}",
                    model.ident,
                    default.map_or("none".to_owned(), |symbol| format!("`{symbol}`")),
                    site.at(),
                    def.default
                        .as_deref()
                        .map_or("none".to_owned(), |symbol| format!("`{symbol}`")),
                ),
            ));
        }
    }
    match errors.into_iter().reduce(|mut all, next| {
        all.combine(next);
        all
    }) {
        Some(error) => Err(error),
        None => Ok(TokenStream::new()),
    }
}
