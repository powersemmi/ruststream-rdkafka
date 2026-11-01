//! The local Protobuf rule the registry lacks: a field number that disappears from a message must
//! be reserved (`buf`'s `FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED`). The registry accepts moving
//! `item = 2` to `item = 3` under `BACKWARD`, and old records then lose the field silently.

use std::collections::BTreeMap;

use anyhow::{Result, anyhow, bail};
use prost_types::DescriptorProto;

use crate::catalog::Migration;

/// One message of a `.proto`: its fields by number, and its reserved number ranges.
struct Message {
    fields: BTreeMap<i32, String>,
    /// Half-open `[start, end)`, as descriptors store them.
    reserved: Vec<(i32, i32)>,
}

impl Message {
    fn keeps(&self, number: i32) -> bool {
        self.fields.contains_key(&number)
            || self
                .reserved
                .iter()
                .any(|&(start, end)| (start..end).contains(&number))
    }
}

/// Checks `candidate` (the text of migration `file` of `subject`) against every earlier
/// migration: each field number an earlier version of a message used is still a field of that
/// message or reserved in it.
pub(crate) fn check(
    subject: &str,
    file: &str,
    candidate: &str,
    earlier: &[Migration],
) -> Result<()> {
    let new = messages(subject, file, candidate)?;
    for old_migration in earlier {
        let old = messages(subject, &old_migration.file, &old_migration.content)?;
        for (name, old_message) in &old {
            let Some(new_message) = new.get(name) else {
                continue;
            };
            for (number, field) in &old_message.fields {
                if !new_message.keeps(*number) {
                    bail!(
                        "migration `{file}` of subject `{subject}` drops field `{field} = \
                         {number}` of message `{name}` (declared in `{}`) without reserving its \
                         number: add `reserved {number};` to `{name}`, or a reader of old \
                         records reads whatever field takes the number next",
                        old_migration.file
                    );
                }
            }
        }
    }
    Ok(())
}

/// Parses a migration: it has to be a `.proto` file, whatever the rule finds.
pub(crate) fn parse_check(subject: &str, file: &str, text: &str) -> Result<()> {
    messages(subject, file, text).map(drop)
}

/// The full name of the first message of a `.proto`: the record a Protobuf subject declares
/// when nothing names another, as the registry's serializers pick it.
pub(crate) fn first_message(subject: &str, file: &str, text: &str) -> Result<String> {
    let descriptor = protox_parse::parse(file, text)
        .map_err(|err| anyhow!("`{file}` of subject `{subject}` is not a .proto file: {err}"))?;
    let first = descriptor
        .message_type
        .first()
        .and_then(|message| message.name.clone())
        .ok_or_else(|| anyhow!("`{file}` of subject `{subject}` declares no message"))?;
    Ok(match descriptor.package.as_deref() {
        Some(package) if !package.is_empty() => format!("{package}.{first}"),
        _ => first,
    })
}

fn messages(subject: &str, file: &str, text: &str) -> Result<BTreeMap<String, Message>> {
    let descriptor = protox_parse::parse(file, text).map_err(|err| {
        anyhow!("migration `{file}` of subject `{subject}` is not a .proto file: {err}")
    })?;
    let package = descriptor.package.unwrap_or_default();
    let mut out = BTreeMap::new();
    for message in &descriptor.message_type {
        collect(&package, message, &mut out);
    }
    Ok(out)
}

fn collect(scope: &str, message: &DescriptorProto, out: &mut BTreeMap<String, Message>) {
    let name = message.name.as_deref().unwrap_or_default();
    let full = if scope.is_empty() {
        name.to_owned()
    } else {
        format!("{scope}.{name}")
    };
    for nested in &message.nested_type {
        collect(&full, nested, out);
    }
    let fields = message
        .field
        .iter()
        .filter_map(|field| Some((field.number?, field.name.clone().unwrap_or_default())))
        .collect();
    let reserved = message
        .reserved_range
        .iter()
        .filter_map(|range| Some((range.start?, range.end?)))
        .collect();
    out.insert(full, Message { fields, reserved });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn migration(file: &str, content: &str) -> Migration {
        Migration {
            number: 1,
            file: file.to_owned(),
            content: content.to_owned(),
        }
    }

    const V1: &str = "syntax = \"proto3\";\npackage acme;\nmessage Order {\n  int64 id = 1;\n  string item = 2;\n  message Line { int32 qty = 1; string sku = 2; }\n}\n";

    #[test]
    fn a_renumbered_field_without_a_reservation_is_refused() {
        let moved = "syntax = \"proto3\";\npackage acme;\nmessage Order {\n  int64 id = 1;\n  string item = 3;\n  message Line { int32 qty = 1; string sku = 2; }\n}\n";
        let err = check(
            "orders-value",
            "0002_move.proto",
            moved,
            &[migration("0001_init.proto", V1)],
        )
        .expect_err("2 is gone")
        .to_string();
        assert_eq!(
            err,
            "migration `0002_move.proto` of subject `orders-value` drops field `item = 2` of \
             message `acme.Order` (declared in `0001_init.proto`) without reserving its number: \
             add `reserved 2;` to `acme.Order`, or a reader of old records reads whatever field \
             takes the number next"
        );
    }

    #[test]
    fn a_reserved_number_may_disappear() {
        let reserved = "syntax = \"proto3\";\npackage acme;\nmessage Order {\n  reserved 2;\n  int64 id = 1;\n  string item = 3;\n  message Line { int32 qty = 1; reserved 2 to 4; }\n}\n";
        check(
            "orders-value",
            "0002.proto",
            reserved,
            &[migration("0001.proto", V1)],
        )
        .expect("reserved");
    }

    #[test]
    fn nested_messages_are_checked_by_their_full_name() {
        let dropped = "syntax = \"proto3\";\npackage acme;\nmessage Order {\n  int64 id = 1;\n  string item = 2;\n  message Line { int32 qty = 1; }\n}\n";
        let err = check("o", "0002.proto", dropped, &[migration("0001.proto", V1)])
            .expect_err("sku")
            .to_string();
        assert!(
            err.contains("drops field `sku = 2` of message `acme.Order.Line`"),
            "{err}"
        );
    }

    #[test]
    fn every_earlier_migration_counts() {
        // 0002 reserved 2; 0003 lifts the reservation while 0001 still used the number.
        let v2 =
            "syntax = \"proto3\";\npackage acme;\nmessage Order { reserved 2; int64 id = 1; }\n";
        let v3 = "syntax = \"proto3\";\npackage acme;\nmessage Order { int64 id = 1; }\n";
        let earlier = [migration("0001.proto", V1), migration("0002.proto", v2)];
        let err = check("o", "0003.proto", v3, &earlier)
            .expect_err("lifted")
            .to_string();
        assert!(err.contains("(declared in `0001.proto`)"), "{err}");
    }

    #[test]
    fn a_removed_message_and_new_fields_pass() {
        let other = "syntax = \"proto3\";\npackage acme;\nmessage Refund { int64 id = 1; }\n";
        check("o", "0002.proto", other, &[migration("0001.proto", V1)]).expect("message gone");
    }

    #[test]
    fn the_first_message_is_the_record() {
        assert_eq!(
            first_message("o", "a.proto", V1).expect("first"),
            "acme.Order"
        );
        let bare = "syntax = \"proto3\";\nmessage Refund { int64 id = 1; }\n";
        assert_eq!(first_message("o", "a.proto", bare).expect("bare"), "Refund");
        let none = "syntax = \"proto3\";\n";
        assert!(first_message("o", "a.proto", none).is_err());
    }

    #[test]
    fn text_that_is_not_proto_is_refused() {
        let err = parse_check("o", "0001.proto", "message {")
            .expect_err("parse")
            .to_string();
        assert!(
            err.starts_with("migration `0001.proto` of subject `o` is not a .proto file"),
            "{err}"
        );
    }
}
