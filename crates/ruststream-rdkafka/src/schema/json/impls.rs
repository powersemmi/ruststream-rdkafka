//! The serde shapes of the standard types, and of `uuid` and `chrono` types under their features.

use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasher;
use std::num::{NonZeroU8, NonZeroU16, NonZeroU32, NonZeroU64};
use std::rc::Rc;
use std::sync::Arc;

use super::JsonShape;
use super::shape::Shape;

macro_rules! integers {
    ($($ty:ty => $min:expr, $max:expr;)*) => {$(
        impl JsonShape for $ty {
            // `From` is not callable in a constant, and every bound fits an `i128`.
            #[allow(clippy::cast_lossless, clippy::cast_possible_wrap)]
            const SHAPE: Shape = Shape::integer(stringify!($ty), $min as i128, $max as i128);
        }
    )*};
}

integers! {
    i8 => i8::MIN, i8::MAX;
    i16 => i16::MIN, i16::MAX;
    i32 => i32::MIN, i32::MAX;
    i64 => i64::MIN, i64::MAX;
    isize => isize::MIN, isize::MAX;
    u8 => 0, u8::MAX;
    u16 => 0, u16::MAX;
    u32 => 0, u32::MAX;
    u64 => 0, u64::MAX;
    usize => 0, usize::MAX;
    NonZeroU8 => 1, u8::MAX;
    NonZeroU16 => 1, u16::MAX;
    NonZeroU32 => 1, u32::MAX;
    NonZeroU64 => 1, u64::MAX;
}

impl JsonShape for bool {
    const SHAPE: Shape = Shape::Bool;
}

impl JsonShape for f32 {
    const SHAPE: Shape = Shape::Number { format: "float" };
}

impl JsonShape for f64 {
    const SHAPE: Shape = Shape::Number { format: "double" };
}

impl JsonShape for String {
    const SHAPE: Shape = Shape::String { format: None };
}

impl JsonShape for str {
    const SHAPE: Shape = Shape::String { format: None };
}

impl JsonShape for serde_json::Value {
    const SHAPE: Shape = Shape::Any;
}

impl<T: JsonShape> JsonShape for Option<T> {
    const SHAPE: Shape = Shape::option(&T::SHAPE);
}

impl<T: JsonShape> JsonShape for Vec<T> {
    const SHAPE: Shape = Shape::Seq(&T::SHAPE);
}

impl<T: JsonShape> JsonShape for [T] {
    const SHAPE: Shape = Shape::Seq(&T::SHAPE);
}

impl<T: JsonShape, Hasher: BuildHasher> JsonShape for HashMap<String, T, Hasher> {
    const SHAPE: Shape = Shape::Map(&T::SHAPE);
}

impl<T: JsonShape> JsonShape for BTreeMap<String, T> {
    const SHAPE: Shape = Shape::Map(&T::SHAPE);
}

impl JsonShape for serde_json::Map<String, serde_json::Value> {
    const SHAPE: Shape = Shape::Map(&Shape::Any);
}

impl<T: JsonShape + ?Sized> JsonShape for Box<T> {
    const SHAPE: Shape = T::SHAPE;
}

impl<T: JsonShape + ?Sized> JsonShape for Rc<T> {
    const SHAPE: Shape = T::SHAPE;
}

impl<T: JsonShape + ?Sized> JsonShape for Arc<T> {
    const SHAPE: Shape = T::SHAPE;
}

impl<T: JsonShape + ?Sized> JsonShape for &T {
    const SHAPE: Shape = T::SHAPE;
}

#[cfg(feature = "uuid")]
impl JsonShape for uuid::Uuid {
    const SHAPE: Shape = Shape::String {
        format: Some("uuid"),
    };
}

#[cfg(feature = "chrono")]
mod chrono_shapes {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};

    use super::super::JsonShape;
    use super::super::shape::Shape;

    impl<Zone: TimeZone> JsonShape for DateTime<Zone> {
        const SHAPE: Shape = Shape::String {
            format: Some("date-time"),
        };
    }

    impl JsonShape for NaiveDate {
        const SHAPE: Shape = Shape::String {
            format: Some("date"),
        };
    }

    impl JsonShape for NaiveDateTime {
        const SHAPE: Shape = Shape::String {
            format: Some("partial-date-time"),
        };
    }

    impl JsonShape for NaiveTime {
        const SHAPE: Shape = Shape::String {
            format: Some("partial-time"),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_option_of_an_option_is_one_option() {
        let Shape::Option(inner) = <Option<Option<u8>> as JsonShape>::SHAPE else {
            panic!("an Option is nullable");
        };
        assert!(matches!(
            inner,
            Shape::Integer {
                min: 0,
                max: 255,
                ..
            }
        ));
    }

    #[test]
    fn a_non_zero_unsigned_integer_starts_at_one() {
        assert!(matches!(
            <NonZeroU32 as JsonShape>::SHAPE,
            Shape::Integer { min: 1, max, .. } if max == i128::from(u32::MAX)
        ));
    }

    #[test]
    fn a_box_is_its_content() {
        assert!(matches!(
            <Box<Vec<String>> as JsonShape>::SHAPE,
            Shape::Seq(Shape::String { format: None })
        ));
    }
}
