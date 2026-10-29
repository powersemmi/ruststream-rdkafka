//! The Avro types a migration declares, as marker types the per-field bounds name.
//!
//! A bound reads `i64: Carries<kind::long, Site>`, where the site names the type, the field,
//! the subject and the migration, so a mismatch is reported as
//! `` `i32` cannot carry the Avro type `long` declared at `Order__id__in__orders_value__0002_add_item` ``.

use std::borrow::Cow;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Mutex;
use std::time::Duration;

/// Declares that a field of this type writes the Avro type `Kind`, as the migration at `Site`
/// declares it.
///
/// The `Schema` derive bounds every field of a type an owned subject declares on it. The crate
/// implements it for the types `apache_avro`'s `AvroSchema` derive maps, and the derive for the
/// type it derives (`kind::record` for a struct, `kind::enum` for a unit-variant enum).
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot carry the Avro type `{Kind}` declared at `{Site}`",
    label = "this field's type does not match the registered schema",
    note = "the schema comes from the newest migration of each subject that declares the \
            record; change the field or add a migration \
            (`cargo kafka-schema migrate add <subject> <description>`)"
)]
pub trait Carries<Kind, Site> {}

/// Declares that a field of this type reads values the Avro type `Kind` wrote, under Avro's
/// resolution rules, as a version of an external subject declares it at `Site`.
///
/// Beyond what [`Carries`] admits, `int` promotes to `long`, `float` and `double`, `long` to
/// `float` and `double`, `float` to `double`, and `bytes` reads as a string.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot read the Avro type `{Kind}` written at `{Site}`",
    label = "this field's type cannot read what the external subject writes",
    note = "an external subject is read under Avro's resolution rules: `int` promotes to \
            `long`, `float` or `double`, `string` and `bytes` convert, and a field the writer \
            lacks needs a default; change the field, or pull the subject again \
            (`cargo kafka-schema migrate import <subject>`)"
)]
pub trait Reads<Kind, Site> {}

macro_rules! marker {
    ($($(#[$doc:meta])* $name:ident),* $(,)?) => {
        $($(#[$doc])* #[derive(Debug)] #[allow(non_camel_case_types)] pub enum $name {})*
    };
}

marker! {
    /// `null`.
    null,
    /// `boolean`.
    boolean,
    /// `int`.
    int,
    /// `long`.
    long,
    /// `float`.
    float,
    /// `double`.
    double,
    /// `bytes`.
    bytes,
    /// `string`.
    string,
    /// A `record`; its name is compared by the whole-schema constant.
    record,
    /// An `enum`; its symbols are compared where the derive reads the enum.
    r#enum,
    /// A `fixed`; its name and size are compared by the whole-schema constant.
    fixed,
    /// A union other than `["null", T]`.
    union,
}

/// An `array` of `Items`.
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct array<Items>(PhantomData<Items>);

/// A `map` with values of `Values`.
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct map<Values>(PhantomData<Values>);

/// `["null", T]`: what an `Option<T>` writes.
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct nullable<Value>(PhantomData<Value>);

macro_rules! carries {
    ($($kind:ident: $($ty:ty),+;)*) => {
        $($(impl<Site> Carries<$kind, Site> for $ty {})+)*
    };
}

carries! {
    null: ();
    boolean: bool;
    int: i8, i16, i32, u8, u16;
    long: i64, u32;
    float: f32;
    double: f64;
    string: ::std::string::String, str, char;
    fixed: u64, u128, i128;
    record: Duration;
}

macro_rules! reads {
    ($($kind:ident: $($ty:ty),+;)*) => {
        $($(impl<Site> Reads<$kind, Site> for $ty {})+)*
    };
}

reads! {
    null: ();
    boolean: bool;
    int: i8, i16, i32, u8, u16, i64, u32, f32, f64;
    long: i64, u32, f32, f64;
    float: f32, f64;
    double: f64;
    string: ::std::string::String, str, char;
    bytes: ::std::string::String, str;
    fixed: u64, u128, i128;
    record: Duration;
}

macro_rules! passthrough {
    ($($ty:ty),* $(,)?) => {
        $(
            impl<Kind, Site, T: Carries<Kind, Site> + ?Sized> Carries<Kind, Site> for $ty {}
            impl<Kind, Site, T: Reads<Kind, Site> + ?Sized> Reads<Kind, Site> for $ty {}
        )*
    };
}

passthrough!(&T, &mut T, Box<T>, Mutex<T>);

impl<Kind, Site, T: Carries<Kind, Site> + ToOwned + ?Sized> Carries<Kind, Site> for Cow<'_, T> {}
impl<Kind, Site, T: Reads<Kind, Site> + ToOwned + ?Sized> Reads<Kind, Site> for Cow<'_, T> {}

impl<Items, Site, T: Carries<Items, Site>> Carries<array<Items>, Site> for Vec<T> {}
impl<Items, Site, T: Carries<Items, Site>> Carries<array<Items>, Site> for [T] {}
impl<Items, Site, T: Reads<Items, Site>> Reads<array<Items>, Site> for Vec<T> {}
impl<Items, Site, T: Reads<Items, Site>> Reads<array<Items>, Site> for [T] {}

impl<Values, Site, T: Carries<Values, Site>> Carries<map<Values>, Site>
    for HashMap<::std::string::String, T>
{
}
impl<Values, Site, T: Reads<Values, Site>> Reads<map<Values>, Site>
    for HashMap<::std::string::String, T>
{
}

impl<Value, Site, T: Carries<Value, Site>> Carries<nullable<Value>, Site> for Option<T> {}
impl<Value, Site, T: Reads<Value, Site>> Reads<nullable<Value>, Site> for Option<T> {}

// A reader's `Option` picks its branch by what the writer wrote, so it reads a writer that
// never writes `null` as well. Listed per kind: a blanket impl would overlap the one above.
macro_rules! option_reads {
    ($($kind:ident),* $(,)?) => {
        $(impl<Site, T: Reads<$kind, Site>> Reads<$kind, Site> for Option<T> {})*
    };
}

option_reads!(
    null, boolean, int, long, float, double, bytes, string, record, r#enum, fixed, union
);

impl<Items, Site, T: Reads<array<Items>, Site>> Reads<array<Items>, Site> for Option<T> {}
impl<Values, Site, T: Reads<map<Values>, Site>> Reads<map<Values>, Site> for Option<T> {}
