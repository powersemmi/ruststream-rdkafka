//! The normalized form a migration is checksummed and compared in: LF line endings, and for the
//! JSON formats the document minified with its keys sorted.

use std::fmt::Write;

use anyhow::{Context, Result};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::catalog::Format;

/// The form every comparison and checksum reads.
pub(crate) fn normalize(format: Format, text: &str) -> Result<String> {
    let text = text.replace("\r\n", "\n");
    match format {
        Format::Protobuf => Ok(text),
        Format::Avro | Format::Json => {
            let value: Value = serde_json::from_str(&text).context("the schema is not JSON")?;
            Ok(sorted(value).to_string())
        }
    }
}

/// The hex SHA-256 of the normalized form, as the lock records it.
pub(crate) fn checksum(format: Format, text: &str) -> Result<String> {
    let digest = Sha256::digest(normalize(format, text)?.as_bytes());
    Ok(digest
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        }))
}

/// The form a migration file is written in: the JSON formats pretty-printed with sorted keys,
/// so a review reads one field per line; a `.proto` as it is, with LF line endings.
pub(crate) fn pretty(format: Format, text: &str) -> Result<String> {
    let text = text.replace("\r\n", "\n");
    match format {
        Format::Protobuf => Ok(text),
        Format::Avro | Format::Json => {
            let value: Value = serde_json::from_str(&text).context("the schema is not JSON")?;
            let mut pretty = serde_json::to_string_pretty(&sorted(value))?;
            pretty.push('\n');
            Ok(pretty)
        }
    }
}

/// The value with every object's keys in order, whatever map `serde_json` was built with.
fn sorted(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut out = Map::new();
            for (key, value) in entries {
                out.insert(key, sorted(value));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sorted).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_is_minified_with_sorted_keys_and_crlf_does_not_count() {
        let written = "{\r\n  \"type\": \"record\",\r\n  \"name\": \"Order\",\r\n  \"fields\": [{\"type\": \"long\", \"name\": \"id\"}]\r\n}\r\n";
        assert_eq!(
            normalize(Format::Avro, written).expect("json"),
            r#"{"fields":[{"name":"id","type":"long"}],"name":"Order","type":"record"}"#
        );
        assert_eq!(
            checksum(Format::Avro, written).expect("sum"),
            checksum(Format::Json, &written.replace("\r\n", "")).expect("sum")
        );
    }

    #[test]
    fn a_proto_only_loses_its_carriage_returns() {
        let proto = "syntax = \"proto3\";\r\nmessage A {  int64 id = 1; }\r\n";
        assert_eq!(
            normalize(Format::Protobuf, proto).expect("proto"),
            "syntax = \"proto3\";\nmessage A {  int64 id = 1; }\n"
        );
    }

    #[test]
    fn the_checksum_is_the_hex_sha256_of_the_normalized_form() {
        // sha256("{}")
        assert_eq!(
            checksum(Format::Json, "{ }").expect("sum"),
            "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        );
    }

    #[test]
    fn pretty_json_is_sorted_and_ends_with_a_newline() {
        assert_eq!(
            pretty(Format::Avro, r#"{"type":"long","name":"x"}"#).expect("pretty"),
            "{\n  \"name\": \"x\",\n  \"type\": \"long\"\n}\n"
        );
    }

    #[test]
    fn text_that_is_not_json_is_refused() {
        assert!(normalize(Format::Avro, "{").is_err());
    }
}
