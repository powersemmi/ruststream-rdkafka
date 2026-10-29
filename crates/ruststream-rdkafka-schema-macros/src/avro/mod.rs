//! The `Schema` derive for an Avro type: the type is bound to its migrations by its record's
//! full name, and checked against the newest migration of every owned subject that declares the
//! record, or as a reader of every pulled version of an external one.

mod check;
mod emit;
mod parse;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::DeriveInput;

use self::emit::private;
use self::parse::Avro;
use crate::migrations::{self, Format, Migration, Owner, Subject};
use crate::model::{FieldForm, Model, ModelShape};
use crate::{export, online};

/// Expands the derive for an Avro type.
pub(crate) fn derive(input: &DeriveInput) -> syn::Result<TokenStream> {
    let model = Model::read(input)?;
    let span = model.ident.span();
    let mut out = runtime_impls(&model);
    if let ModelShape::Set(variants) = &model.shape {
        out.extend(set_impls(&model, variants));
        return Ok(out);
    }
    if let ModelShape::Transparent(inner) = &model.shape {
        out.extend(transparent_form(&model, inner));
        return Ok(out);
    }
    if !model.generic {
        out.extend(export_test(&model));
    }
    if export::active() {
        return Ok(out);
    }
    let catalog = migrations::read_catalog(&migrations::schema_dir())
        .map_err(|err| syn::Error::new(span, err))?;
    let avro: Vec<&Subject> = catalog
        .iter()
        .filter(|subject| subject.settings.format == Format::Avro)
        .collect();
    let (named, declaring) = names_in(&avro, &model).map_err(|err| syn::Error::new(span, err))?;
    if !declaring.is_empty() && model.generic {
        return Err(syn::Error::new(
            span,
            format!(
                "`{}` is generic, and a migration declares its record `{}`: a generic type has \
                 no one form to check; check a concrete type instead",
                model.ident,
                model.full_name()
            ),
        ));
    }
    let mut checks = TokenStream::new();
    let mut errors: Option<syn::Error> = None;
    let mut recursive = false;
    for subject in &declaring {
        for migration in subject.checked_migrations() {
            match checks_against(&model, &catalog, subject, migration) {
                Ok((tokens, cycle)) => {
                    checks.extend(tokens);
                    recursive |= cycle;
                }
                Err(err) => match &mut errors {
                    Some(errors) => errors.combine(err),
                    None => errors = Some(err),
                },
            }
        }
    }
    if let Some(errors) = errors {
        return Err(errors);
    }
    // A recursive record's form would be an infinite constant, which the compiler rejects as a
    // cycle the moment it exists; such a record is compared by the migrations tool alone.
    if named && !recursive {
        out.extend(own_form(&model));
    }
    let guard = online::guard(&declaring, span)?;
    out.extend(quote! {
        const _: () = {
            #guard
            #checks
        };
    });
    Ok(out)
}

/// Whether any Avro migration names a type with the model's name (so another type's check may
/// need the model's form), and the subjects whose checked migrations declare the model's
/// record under its full name.
fn names_in<'a>(
    subjects: &[&'a Subject],
    model: &Model,
) -> Result<(bool, Vec<&'a Subject>), String> {
    let full = model.full_name();
    let mut named = false;
    let mut declaring = Vec::new();
    for subject in subjects {
        let mut declares = false;
        let mut records = Vec::new();
        match subject.settings.owner {
            Owner::Service => records.extend(subject.declared_records()?),
            Owner::External => {
                for migration in subject.checked_migrations() {
                    records.extend(Subject::named_types_of(migration)?);
                }
            }
        }
        for name in records {
            let simple = name
                .rsplit_once('.')
                .map_or(name.as_str(), |(_, simple)| simple);
            named |= simple == model.name;
            declares |= name == full;
        }
        if declares {
            declaring.push(*subject);
        }
    }
    Ok((named, declaring))
}

/// The impls the runtime and the bounds read: the record name, and the kinds the type carries.
fn runtime_impls(model: &Model) -> TokenStream {
    let p = private();
    let ident = &model.ident;
    let kind = match &model.shape {
        ModelShape::Record(_) => quote!(#p::kind::record),
        ModelShape::Enum { .. } => quote!(#p::kind::r#enum),
        ModelShape::Transparent(_) | ModelShape::Set(_) => return TokenStream::new(),
    };
    let record = model.full_name();
    let (impl_generics, ty_generics, where_clause) = split(model);
    let fields = match &model.shape {
        ModelShape::Record(fields) => {
            let members = emit::members(fields);
            let bounds = fields
                .iter()
                .filter(|field| field.form == FieldForm::Flatten)
                .map(|field| {
                    let ty = &field.ty;
                    quote!(#ty: #p::AvroFields)
                });
            let where_clause = generic_where(model, bounds);
            quote! {
                impl<#impl_generics> #p::AvroFields for #ident #ty_generics #where_clause {
                    const MEMBERS: &'static [#p::Member] = #members;
                }
            }
        }
        _ => TokenStream::new(),
    };
    quote! {
        #fields
        impl<#impl_generics> #p::Record for #ident #ty_generics #where_clause {
            const RECORD: &'static str = #record;
        }
        impl<#impl_generics __Site> #p::Carries<#kind, __Site> for #ident #ty_generics
            #where_clause {}
        impl<#impl_generics __Site> #p::Reads<#kind, __Site> for #ident #ty_generics
            #where_clause {}
    }
}

/// The generics of the derived type, without the angle brackets of the impl's own list.
fn split(model: &Model) -> (TokenStream, TokenStream, TokenStream) {
    let generics = &model.generics;
    let params = &generics.params;
    let params = if params.is_empty() {
        TokenStream::new()
    } else {
        quote!(#params,)
    };
    let (_, ty_generics, where_clause) = generics.split_for_impl();
    (params, quote!(#ty_generics), quote!(#where_clause))
}

fn own_form(model: &Model) -> TokenStream {
    let p = private();
    let ident = &model.ident;
    let (form, bounds) = match &model.shape {
        ModelShape::Record(fields) => {
            let bounds: Vec<_> = fields
                .iter()
                .filter(|field| field.form == FieldForm::Typed)
                .map(|field| {
                    let ty = &field.ty;
                    quote!(#ty: #p::AvroForm)
                })
                .collect();
            (emit::record_form(model, fields), bounds)
        }
        ModelShape::Enum { symbols, default } => {
            let symbols: Vec<String> = symbols.iter().map(|(symbol, _)| symbol.clone()).collect();
            (
                emit::enum_form(model, &symbols, default.as_deref()),
                Vec::new(),
            )
        }
        ModelShape::Transparent(_) | ModelShape::Set(_) => return TokenStream::new(),
    };
    let default = emit::option(model.default.as_deref());
    let (impl_generics, ty_generics, _) = model.generics.split_for_impl();
    let where_clause = generic_where(model, bounds);
    quote! {
        impl #impl_generics #p::AvroForm for #ident #ty_generics #where_clause {
            const FORM: &'static #p::Form = #form;
            const FIELD_DEFAULT: ::core::option::Option<&'static str> = #default;
        }
    }
}

/// The type's where clause, with `bounds` on the field types when the type is generic: a
/// concrete type needs none, they would only repeat what the constant states.
fn generic_where(model: &Model, bounds: impl IntoIterator<Item = TokenStream>) -> TokenStream {
    let predicates: Vec<_> = model
        .generics
        .where_clause
        .iter()
        .flat_map(|clause| clause.predicates.iter())
        .collect();
    let bounds: Vec<_> = if model.generic {
        bounds.into_iter().collect()
    } else {
        Vec::new()
    };
    if predicates.is_empty() && bounds.is_empty() {
        TokenStream::new()
    } else {
        quote!(where #(#predicates,)* #(#bounds,)*)
    }
}

fn transparent_form(model: &Model, inner: &syn::Type) -> TokenStream {
    let p = private();
    let ident = &model.ident;
    let (impl_generics, ty_generics, where_clause) = model.generics.split_for_impl();
    let (params, ..) = split(model);
    quote! {
        impl #impl_generics #p::AvroForm for #ident #ty_generics #where_clause {
            const FORM: &'static #p::Form = <#inner as #p::AvroForm>::FORM;
            const FIELD_DEFAULT: ::core::option::Option<&'static str> =
                <#inner as #p::AvroForm>::FIELD_DEFAULT;
        }
        impl<#params __Kind, __Site> #p::Carries<__Kind, __Site> for #ident #ty_generics
            where #inner: #p::Carries<__Kind, __Site> {}
        impl<#params __Kind, __Site> #p::Reads<__Kind, __Site> for #ident #ty_generics
            where #inner: #p::Reads<__Kind, __Site> {}
    }
}

/// A set of input models: the record each variant carries, read from the variant's model, and
/// the kind that names each variant.
fn set_impls(model: &Model, variants: &[(syn::Ident, syn::Type)]) -> TokenStream {
    let p = private();
    let ident = &model.ident;
    let kind = format_ident!("{ident}Kind");
    let records = variants.iter().map(|(_, ty)| {
        quote::quote_spanned!(syn::spanned::Spanned::span(ty)=> <#ty as #p::Record>::RECORD)
    });
    let kinds = variants
        .iter()
        .map(|(variant, ty)| quote!((<#ty as #p::Record>::RECORD, #kind::#variant)));
    quote! {
        impl #p::RecordSet for #ident {
            const RECORDS: &'static [&'static str] = &[#(#records),*];
        }
        impl #p::RecordKind for #kind {
            const KINDS: &'static [(&'static str, Self)] = &[#(#kinds),*];
        }
    }
}

fn export_test(model: &Model) -> TokenStream {
    let p = private();
    let ident = &model.ident;
    let schema = quote! {
        #p::serde_json::to_string(
            &#p::serde_json::to_value(<super::#ident as #p::AvroSchema>::get_schema())
                .expect("an Avro schema is JSON"),
        )
        .expect("JSON writes")
    };
    export::test(ident, &model.full_name(), "avsc", &schema)
}

/// The checks of the model against one migration of one subject, and whether the record the
/// migration declares for it is recursive.
fn checks_against(
    model: &Model,
    catalog: &[Subject],
    subject: &Subject,
    migration: &Migration,
) -> syn::Result<(TokenStream, bool)> {
    let span = model.ident.span();
    let external = |name: &str| reference(catalog, subject, name);
    let schema = parse::parse(&migration.content, &external).map_err(|err| {
        syn::Error::new(
            span,
            format!("migration `{}`: {err}", migration.path.display()),
        )
    })?;
    let Some(def) = schema.find(&model.full_name()) else {
        return Ok((TokenStream::new(), false));
    };
    let site = check::Site {
        model,
        subject: &subject.name,
        migration: &migration.stem,
        external: subject.settings.owner == Owner::External,
    };
    let path = migration.path.display().to_string();
    let checks = check::checks(&site, def)?;
    let tokens = quote! {
        const _: &[u8] = include_bytes!(#path);
        #checks
    };
    Ok((tokens, def.is_recursive()))
}

/// A named type another subject holds, found through the references of `subject.toml`.
fn reference(catalog: &[Subject], subject: &Subject, name: &str) -> Option<Avro> {
    let referenced = subject
        .settings
        .references
        .iter()
        .find(|reference| reference.name == name)?;
    let holder = catalog
        .iter()
        .find(|candidate| candidate.name == referenced.subject)?;
    let newest = holder.newest()?;
    let none = |_: &str| None;
    parse::parse(&newest.content, &none)
        .ok()?
        .find(name)
        .cloned()
}

/// Names that are already taken in the module, never repeated: the per-field sites are unique
/// per subject and migration.
pub(crate) fn site_ident(model: &Model, field: &str, subject: &str, migration: &str) -> syn::Ident {
    let clean = |text: &str| -> String {
        text.chars()
            .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
            .collect()
    };
    format_ident!(
        "{}__{}__in__{}__{}",
        model.ident,
        clean(field),
        clean(subject),
        clean(migration)
    )
}
