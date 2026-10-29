//! The JSON Schema side of the derive: a type with `#[schema(namespace = ..)]` gets its serde
//! shape as a constant, and a comparison with the newest migration of every JSON subject that
//! declares its record.

mod attrs;
mod schema;
mod serde_shape;

use std::env;

use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::DeriveInput;
use syn::spanned::Spanned;

use crate::migrations::{self, Format, Owner, Subject};
use crate::online;

/// The path generated code reaches the runtime side through.
fn private() -> TokenStream {
    quote!(::ruststream_rdkafka::schema::__json)
}

/// Whether the type carries `#[schema(namespace = ..)]`, which puts it on the JSON path. A
/// malformed `#[schema(..)]` on the type puts it there too, so [`derive`] reports it.
pub(crate) fn has_namespace(input: &DeriveInput) -> bool {
    attrs::namespace(&input.attrs).map_or(true, |namespace| namespace.is_some())
}

/// The derive for a type with `#[schema(namespace = ..)]`: its `JsonShape`, the comparison with
/// every JSON subject that declares its record, and the export test `cargo kafka-schema` runs.
///
/// # Errors
///
/// What the type's attributes or the subjects' migrations make impossible to check, at the type.
pub(crate) fn derive(input: &DeriveInput) -> syn::Result<TokenStream> {
    let namespace = attrs::namespace(&input.attrs)?.ok_or_else(|| {
        syn::Error::new_spanned(
            &input.ident,
            "the JSON check needs `#[schema(namespace = \"..\")]`",
        )
    })?;
    let ident = &input.ident;
    let record = format!("{}.{ident}", namespace.value());
    let shape = match serde_shape::shape_impl(input) {
        Ok(shape) => shape,
        Err(err) => {
            let fallback = serde_shape::fallback_impl(input);
            let err = err.into_compile_error();
            return Ok(quote!(#fallback #err));
        }
    };
    let export = export_test(input, &record);
    let triggers = quote! {
        const _: () = {
            let _ = ::core::option_env!("RUSTSTREAM_SCHEMA_EXPORT");
            let _ = ::core::option_env!("RUSTSTREAM_SCHEMA_DIR");
        };
    };
    // Export mode emits no checks, so a changed type exports before its migration exists.
    if env::var_os("RUSTSTREAM_SCHEMA_EXPORT").is_some() {
        return Ok(quote!(#shape #export #triggers));
    }
    let catalog = migrations::read_catalog(&migrations::schema_dir())
        .map_err(|err| syn::Error::new_spanned(&input.ident, err))?;
    let subjects =
        declaring(&catalog, &record).map_err(|err| syn::Error::new_spanned(&input.ident, err))?;
    if !subjects.is_empty() && !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            format!(
                "subject `{}` declares `{record}`, and a generic type has no single shape to \
                 compare; declare the record on a type without parameters",
                subjects[0].name
            ),
        ));
    }
    let mut checks = Vec::new();
    for subject in &subjects {
        checks.push(check(input, subject, &catalog)?);
    }
    let guard = online::guard(&subjects, input.ident.span())?;
    Ok(quote!(#shape #(#checks)* #guard #export #triggers))
}

/// The JSON subjects whose `record` is `record`.
fn declaring<'a>(catalog: &'a [Subject], record: &str) -> Result<Vec<&'a Subject>, String> {
    let mut subjects = Vec::new();
    for subject in catalog {
        if subject.settings.format == Format::Json
            && subject
                .declared_records()?
                .iter()
                .any(|name| name == record)
        {
            subjects.push(subject);
        }
    }
    Ok(subjects)
}

/// The comparison of the type with the newest migration of `subject`, evaluated by the compiler.
fn check(input: &DeriveInput, subject: &Subject, catalog: &[Subject]) -> syn::Result<TokenStream> {
    let private = private();
    let ident = &input.ident;
    let mut reader = schema::Reader::new(catalog);
    let node = reader
        .read(subject)
        .map_err(|err| syn::Error::new_spanned(ident, err))?
        .tokens();
    let migration = subject
        .newest()
        .map(|newest| newest.stem.clone())
        .unwrap_or_default();
    let direction = match subject.settings.owner {
        Owner::Service => quote!(#private::Direction::OWNED),
        Owner::External => quote!(#private::Direction::EXTERNAL),
    };
    let name = ident.to_string();
    let subject_name = &subject.name;
    let files = reader.files.iter().map(|file| file.display().to_string());
    Ok(quote_spanned! {ident.span()=>
        const _: () = {
            #(let _ = ::core::include_bytes!(#files);)*
            const SCHEMA: #private::Node = #node;
            const CHECK: #private::Check = #private::Check {
                type_name: #name,
                subject: #subject_name,
                migration: #migration,
                direction: #direction,
            };
            if let ::core::result::Result::Err(message) =
                #private::compare(&<#ident as #private::JsonShape>::SHAPE, &SCHEMA, &CHECK)
            {
                ::core::panic!("{}", message.as_str());
            }
        };
    })
}

/// The hidden test that writes the type's `schemars` schema under `RUSTSTREAM_SCHEMA_EXPORT`.
fn export_test(input: &DeriveInput, record: &str) -> TokenStream {
    if !input.generics.params.is_empty() {
        return TokenStream::new();
    }
    let private = private();
    let ident = &input.ident;
    let test = format_ident!(
        "__ruststream_schema_export_{}",
        snake_case(&ident.to_string())
    );
    quote_spanned! {input.span()=>
        #[cfg(test)]
        #[test]
        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #test() {
            #private::export::<#ident>(#record);
        }
    }
}

fn snake_case(name: &str) -> String {
    let mut snake = String::new();
    for (index, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if index > 0 {
                snake.push('_');
            }
            snake.extend(ch.to_lowercase());
        } else {
            snake.push(ch);
        }
    }
    snake
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_export_test_is_named_after_the_type() {
        assert_eq!(snake_case("OrderKey"), "order_key");
        assert_eq!(snake_case("Order"), "order");
    }
}
