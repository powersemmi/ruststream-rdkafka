//! Tokens for the two sides of the Avro check: a migration's tree as a form constant and as the
//! kind a per-field bound names, and the type's own form composed from its fields.

use proc_macro2::TokenStream;
use quote::quote;

use super::parse::{Avro, FullName};
use crate::model::{FieldDefault, FieldForm, FieldModel, Model};

/// The path generated code reaches the crate's machinery through.
pub(crate) fn private() -> TokenStream {
    quote!(::ruststream_rdkafka::schema::__private)
}

pub(crate) fn option(value: Option<&str>) -> TokenStream {
    value.map_or_else(
        || quote!(::core::option::Option::None),
        |value| quote!(::core::option::Option::Some(#value)),
    )
}

fn migration_name(name: &FullName) -> TokenStream {
    let p = private();
    let (namespace, name) = (&name.namespace, &name.name);
    quote!(#p::Name { namespace: ::core::option::Option::Some(#namespace), name: #name })
}

/// The migration's tree as a `&'static Form` expression.
///
/// # Panics
///
/// On a recursive reference: the caller compares no recursive tree.
pub(crate) fn form(avro: &Avro) -> TokenStream {
    let p = private();
    match avro {
        Avro::Null => quote!(&#p::Form::Null),
        Avro::Boolean => quote!(&#p::Form::Boolean),
        Avro::Int => quote!(&#p::Form::Int),
        Avro::Long => quote!(&#p::Form::Long),
        Avro::Float => quote!(&#p::Form::Float),
        Avro::Double => quote!(&#p::Form::Double),
        Avro::Bytes => quote!(&#p::Form::Bytes),
        Avro::String => quote!(&#p::Form::String),
        Avro::Array(items) => {
            let items = form(items);
            quote!(&#p::Form::Array(#items))
        }
        Avro::Map(values) => {
            let values = form(values);
            quote!(&#p::Form::Map(#values))
        }
        Avro::Nullable(value) => {
            let value = form(value);
            quote!(&#p::Form::Nullable(#value))
        }
        Avro::Union(branches) => {
            let branches = branches.iter().map(form);
            quote!(&#p::Form::Union(&[#(#branches),*]))
        }
        Avro::Record(record) => {
            let name = migration_name(&record.name);
            let fields = record.fields.iter().map(|field| {
                let (field_name, ty) = (&field.name, form(&field.ty));
                let default = option(field.default.as_deref());
                quote!(#p::FieldForm {
                    name: #field_name,
                    shape: #p::Shape::Typed(#ty),
                    default: #default,
                })
            });
            quote!(&#p::Form::Record(&#p::RecordForm { name: #name, fields: &[#(#fields),*] }))
        }
        Avro::Enum(def) => {
            let name = migration_name(&def.name);
            let symbols = &def.symbols;
            let default = option(def.default.as_deref());
            quote!(&#p::Form::Enum(&#p::EnumForm {
                name: #name,
                symbols: &[#(#symbols),*],
                default: #default,
            }))
        }
        Avro::Fixed(def) => {
            let name = migration_name(&def.name);
            let size = usize::try_from(def.size).unwrap_or(usize::MAX);
            quote!(&#p::Form::Fixed(&#p::FixedForm { name: #name, size: #size }))
        }
        Avro::Recursive(name) => unreachable!("recursive record `{name}` reached the comparison"),
    }
}

/// The kind a per-field bound names for a field of this Avro type.
pub(crate) fn kind(avro: &Avro) -> TokenStream {
    let p = private();
    let k = quote!(#p::kind);
    match avro {
        Avro::Null => quote!(#k::null),
        Avro::Boolean => quote!(#k::boolean),
        Avro::Int => quote!(#k::int),
        Avro::Long => quote!(#k::long),
        Avro::Float => quote!(#k::float),
        Avro::Double => quote!(#k::double),
        Avro::Bytes => quote!(#k::bytes),
        Avro::String => quote!(#k::string),
        Avro::Array(items) => {
            let items = kind(items);
            quote!(#k::array<#items>)
        }
        Avro::Map(values) => {
            let values = kind(values);
            quote!(#k::map<#values>)
        }
        Avro::Nullable(value) => {
            let value = kind(value);
            quote!(#k::nullable<#value>)
        }
        Avro::Union(_) => quote!(#k::union),
        Avro::Record(_) | Avro::Recursive(_) => quote!(#k::record),
        Avro::Enum(_) => quote!(#k::r#enum),
        Avro::Fixed(_) => quote!(#k::fixed),
    }
}

/// The type's own name as a `Name` expression: no namespace means the enclosing one.
fn model_name(model: &Model) -> TokenStream {
    let p = private();
    let namespace = option(model.namespace.as_deref());
    let name = &model.name;
    quote!(#p::Name { namespace: #namespace, name: #name })
}

/// The type's form, composed from its fields' forms.
pub(crate) fn record_form(model: &Model, fields: &[FieldModel]) -> TokenStream {
    let p = private();
    let name = model_name(model);
    let fields = fields.iter().map(|field| {
        let (field_name, ty) = (&field.name, &field.ty);
        let shape = match field.form {
            FieldForm::Typed => quote!(#p::Shape::Typed(<#ty as #p::AvroForm>::FORM)),
            FieldForm::With => quote!(#p::Shape::With),
            FieldForm::Flatten => quote!(#p::Shape::Flatten(<#ty as #p::AvroFields>::MEMBERS)),
        };
        let default = match (&field.default, field.form) {
            (FieldDefault::Json(json), _) => option(Some(json)),
            (FieldDefault::OfType, FieldForm::Typed) => {
                quote!(<#ty as #p::AvroForm>::FIELD_DEFAULT)
            }
            _ => option(None),
        };
        quote!(#p::FieldForm { name: #field_name, shape: #shape, default: #default })
    });
    quote!(&#p::Form::Record(&#p::RecordForm { name: #name, fields: &[#(#fields),*] }))
}

/// The type's form as an Avro enum.
pub(crate) fn enum_form(model: &Model, symbols: &[String], default: Option<&str>) -> TokenStream {
    let p = private();
    let name = model_name(model);
    let default = option(default);
    quote!(&#p::Form::Enum(&#p::EnumForm {
        name: #name,
        symbols: &[#(#symbols),*],
        default: #default,
    }))
}

/// The names of the type's fields on the wire, a flattened field's type contributing its own.
pub(crate) fn members(fields: &[FieldModel]) -> TokenStream {
    let p = private();
    let members = fields.iter().map(|field| {
        let (name, ty) = (&field.name, &field.ty);
        match field.form {
            FieldForm::Flatten => quote!(#p::Member::Flatten(<#ty as #p::AvroFields>::MEMBERS)),
            FieldForm::Typed | FieldForm::With => quote!(#p::Member::Field(#name)),
        }
    });
    quote!(&[#(#members),*])
}
