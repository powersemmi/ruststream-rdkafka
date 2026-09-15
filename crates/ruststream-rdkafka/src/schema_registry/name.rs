//! The key a registering codec looks a message type up by: the name serde gives it.

use std::fmt;

use serde::{Serialize, Serializer};

/// The probe's only outcome. Returning the name as an error is what stops serialization at the
/// header, before a single field is visited.
#[derive(Debug)]
pub(crate) struct Named(Option<&'static str>);

impl fmt::Display for Named {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(name) => write!(f, "{name}"),
            None => f.write_str("an unnamed value"),
        }
    }
}

impl std::error::Error for Named {}

impl serde::ser::Error for Named {
    fn custom<T: fmt::Display>(_msg: T) -> Self {
        Self(None)
    }
}

/// A serializer that reads the name off a value's header and refuses to go further.
///
/// `TypeId` would be the obvious key and is unavailable: it needs `T: 'static`, and
/// [`Codec::encode`](ruststream::codec::Codec::encode) bounds `T` by `Serialize` alone. What
/// `Serialize` does hand over is the name every derived implementation passes to
/// `serialize_struct` before touching a field, so that is the key - and it is the right one on
/// merit, being the same name `AvroSchema` writes into the Avro record, which is what
/// registration reads when it has a type but no value.
///
/// `std::any::type_name` is deliberately not used: the standard library promises neither
/// uniqueness nor stability for it, and which schema a message is written with is not a thing to
/// rest on a diagnostic string.
struct NameProbe;

/// The scalar arms: a bare value carries no name, so every one of them ends the probe.
macro_rules! unnamed {
    ($($method:ident: $ty:ty),* $(,)?) => {$(
        fn $method(self, _value: $ty) -> Result<Self::Ok, Self::Error> {
            Err(Named(None))
        }
    )*};
}

impl Serializer for NameProbe {
    type Ok = ();
    type Error = Named;
    type SerializeSeq = serde::ser::Impossible<(), Named>;
    type SerializeTuple = serde::ser::Impossible<(), Named>;
    type SerializeTupleStruct = serde::ser::Impossible<(), Named>;
    type SerializeTupleVariant = serde::ser::Impossible<(), Named>;
    type SerializeMap = serde::ser::Impossible<(), Named>;
    type SerializeStruct = serde::ser::Impossible<(), Named>;
    type SerializeStructVariant = serde::ser::Impossible<(), Named>;

    unnamed! {
        serialize_bool: bool,
        serialize_i8: i8,
        serialize_i16: i16,
        serialize_i32: i32,
        serialize_i64: i64,
        serialize_u8: u8,
        serialize_u16: u16,
        serialize_u32: u32,
        serialize_u64: u64,
        serialize_f32: f32,
        serialize_f64: f64,
        serialize_char: char,
        serialize_str: &str,
        serialize_bytes: &[u8],
    }

    fn serialize_none(self) -> Result<(), Named> {
        Err(Named(None))
    }

    fn serialize_some<T: ?Sized + Serialize>(self, _value: &T) -> Result<(), Named> {
        Err(Named(None))
    }

    fn serialize_unit(self) -> Result<(), Named> {
        Err(Named(None))
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<(), Named> {
        Err(Named(Some(name)))
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        _index: u32,
        _variant: &'static str,
    ) -> Result<(), Named> {
        Err(Named(Some(name)))
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        _value: &T,
    ) -> Result<(), Named> {
        Err(Named(Some(name)))
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<(), Named> {
        Err(Named(Some(name)))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Named> {
        Err(Named(None))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Named> {
        Err(Named(None))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Named> {
        Err(Named(Some(name)))
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Named> {
        Err(Named(Some(name)))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Named> {
        Err(Named(None))
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Named> {
        Err(Named(Some(name)))
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Named> {
        Err(Named(Some(name)))
    }
}

/// The name `value`'s type gives serde, or `None` for a value that has none (a bare integer, a
/// sequence, a map).
///
/// The cost is one `serialize_struct` call that returns immediately: no field is visited, whatever
/// the value holds, and nothing is allocated.
pub(crate) fn serde_name<T: Serialize + ?Sized>(value: &T) -> Option<&'static str> {
    match value.serialize(NameProbe) {
        Ok(()) => None,
        Err(Named(name)) => name,
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::serde_name;

    #[derive(Serialize)]
    struct Order {
        id: i64,
    }

    #[derive(Serialize)]
    #[serde(rename = "RenamedOrder")]
    struct Renamed {
        id: i64,
    }

    #[derive(Serialize)]
    struct Wrapper(i64);

    #[test]
    fn a_named_value_yields_the_name_serde_knows_it_by() {
        assert_eq!(serde_name(&Order { id: 1 }), Some("Order"));
        assert_eq!(serde_name(&Renamed { id: 1 }), Some("RenamedOrder"));
        assert_eq!(serde_name(&Wrapper(1)), Some("Wrapper"));
    }

    #[test]
    fn an_unnamed_value_yields_nothing() {
        assert_eq!(serde_name(&7i64), None);
        assert_eq!(serde_name(&vec![1, 2, 3]), None);
        assert_eq!(serde_name("text"), None);
    }

    /// The claim the whole key rests on: the probe returns at the struct header, so a field is
    /// never asked for its value.
    #[test]
    fn the_probe_never_reaches_a_field() {
        struct Exploding;

        impl Serialize for Exploding {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeStruct;

                let mut state = serializer.serialize_struct("Exploding", 1)?;
                state.serialize_field("boom", &unreachable_field())?;
                state.end()
            }
        }

        fn unreachable_field() -> i64 {
            panic!("the probe must stop at the struct header");
        }

        assert_eq!(serde_name(&Exploding), Some("Exploding"));
    }
}
