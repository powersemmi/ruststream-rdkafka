//! The local checks of a JSON Schema migration: it is valid under its draft's metaschema, and
//! its content model suits the subject's level.
//!
//! JSON Schema has no local compatibility check; the registry answers that question.

use anyhow::{Context, Result, bail};
use ruststream_rdkafka::schema_registry::admin::CompatibilityLevel;
use serde_json::Value;

/// The drafts the registry accepts, by the `$schema` they declare.
const DRAFTS: [&str; 5] = [
    "http://json-schema.org/draft-04/schema",
    "http://json-schema.org/draft-06/schema",
    "http://json-schema.org/draft-07/schema",
    "https://json-schema.org/draft/2019-09/schema",
    "https://json-schema.org/draft/2020-12/schema",
];

/// Checks that `text` is a JSON Schema valid under the metaschema of the draft it declares
/// (2020-12 when it declares none).
pub(crate) fn check_metaschema(subject: &str, file: &str, text: &str) -> Result<()> {
    let schema: Value = serde_json::from_str(text)
        .with_context(|| format!("migration `{file}` of subject `{subject}` is not JSON"))?;
    if let Some(declared) = schema.get("$schema") {
        let known = declared
            .as_str()
            .map(|uri| uri.trim_end_matches('#'))
            .is_some_and(|uri| DRAFTS.contains(&uri));
        if !known {
            bail!(
                "migration `{file}` of subject `{subject}` declares `$schema` {declared}, which \
                 is none of the drafts the registry accepts (04, 06, 07, 2019-09, 2020-12)"
            );
        }
    }
    if let Err(err) = jsonschema::meta::validate(&schema) {
        bail!(
            "migration `{file}` of subject `{subject}` is not a valid JSON Schema: at `{}`: {err}",
            err.instance_path()
        );
    }
    Ok(())
}

/// Refuses a content model the level makes a trap: an open object at a `BACKWARD` level, where
/// adding a property later is `PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL`, and a closed one at a
/// `FORWARD` level, where adding a property is refused.
pub(crate) fn check_content_model(
    subject: &str,
    file: &str,
    level: CompatibilityLevel,
    text: &str,
) -> Result<()> {
    let schema: Value = serde_json::from_str(text)
        .with_context(|| format!("migration `{file}` of subject `{subject}` is not JSON"))?;
    let refuse_open = level.checks_backward() && !level.checks_forward();
    let refuse_closed = level.checks_forward() && !level.checks_backward();
    if !refuse_open && !refuse_closed {
        return Ok(());
    }
    let mut objects = Vec::new();
    walk(&schema, "", &mut objects);
    for (pointer, closed) in objects {
        let at = if pointer.is_empty() {
            "#".to_owned()
        } else {
            format!("#{pointer}")
        };
        if refuse_open && !closed {
            bail!(
                "migration `{file}` of subject `{subject}` leaves the object at `{at}` open, and \
                 under {level} adding a property to an open object is \
                 PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL: close it with \
                 `#[serde(deny_unknown_fields)]` on its type"
            );
        }
        if refuse_closed && closed {
            bail!(
                "migration `{file}` of subject `{subject}` closes the object at `{at}`, and under \
                 {level} adding a property to a closed object is refused: open it by removing \
                 `#[serde(deny_unknown_fields)]` from its type"
            );
        }
    }
    Ok(())
}

/// Every object schema (one that declares `properties`) with its JSON pointer and whether it is
/// closed.
fn walk(value: &Value, pointer: &str, out: &mut Vec<(String, bool)>) {
    match value {
        Value::Object(map) => {
            if map.get("properties").is_some_and(Value::is_object) {
                let closed = [
                    map.get("additionalProperties"),
                    map.get("unevaluatedProperties"),
                ]
                .into_iter()
                .flatten()
                .any(|value| *value == Value::Bool(false));
                out.push((pointer.to_owned(), closed));
            }
            for (key, child) in map {
                walk(child, &format!("{pointer}/{}", escape(key)), out);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                walk(child, &format!("{pointer}/{index}"), out);
            }
        }
        _ => {}
    }
}

fn escape(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLOSED: &str = r##"{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"Order","type":"object","properties":{"id":{"type":"integer"},"item":{"$ref":"#/$defs/Item"}},"required":["id"],"additionalProperties":false,"$defs":{"Item":{"type":"object","properties":{"sku":{"type":"string"}},"additionalProperties":false}}}"##;

    fn open_item() -> String {
        CLOSED.replace(
            r#""properties":{"sku":{"type":"string"}},"additionalProperties":false"#,
            r#""properties":{"sku":{"type":"string"}}"#,
        )
    }

    #[test]
    fn every_registry_draft_validates_and_a_broken_schema_names_its_path() {
        check_metaschema("s", "0001.json", CLOSED).expect("2020-12");
        for draft in [
            "http://json-schema.org/draft-04/schema#",
            "http://json-schema.org/draft-07/schema#",
        ] {
            let schema = format!(r#"{{"$schema":"{draft}","type":"object"}}"#);
            check_metaschema("s", "0001.json", &schema).expect(draft);
        }
        let broken = r#"{"type":"object","properties":{"id":{"type":"integr"}}}"#;
        let err = check_metaschema("orders-value", "0002_x.json", broken)
            .expect_err("bad type")
            .to_string();
        assert!(
            err.starts_with(
                "migration `0002_x.json` of subject `orders-value` is not a valid JSON Schema: \
                 at `/properties/id/type`"
            ),
            "{err}"
        );
    }

    #[test]
    fn an_unknown_draft_is_refused() {
        let err = check_metaschema("s", "0001.json", r#"{"$schema":"https://example.com/s"}"#)
            .expect_err("unknown")
            .to_string();
        assert!(
            err.contains("none of the drafts the registry accepts"),
            "{err}"
        );
    }

    #[test]
    fn backward_refuses_an_open_object_and_names_where() {
        check_content_model("s", "0001.json", CompatibilityLevel::Backward, CLOSED)
            .expect("closed");
        let err = check_content_model(
            "orders-value",
            "0001_x.json",
            CompatibilityLevel::BackwardTransitive,
            &open_item(),
        )
        .expect_err("Item open")
        .to_string();
        assert_eq!(
            err,
            "migration `0001_x.json` of subject `orders-value` leaves the object at \
             `#/$defs/Item` open, and under BACKWARD_TRANSITIVE adding a property to an open \
             object is PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL: close it with \
             `#[serde(deny_unknown_fields)]` on its type"
        );
    }

    #[test]
    fn forward_refuses_a_closed_object() {
        let err = check_content_model("s", "0001.json", CompatibilityLevel::Forward, CLOSED)
            .expect_err("closed")
            .to_string();
        assert!(
            err.contains("closes the object at `#`, and under FORWARD"),
            "{err}"
        );
        let open = open_item().replace(r#","additionalProperties":false,"$defs""#, r#","$defs""#);
        check_content_model(
            "s",
            "0001.json",
            CompatibilityLevel::ForwardTransitive,
            &open,
        )
        .expect("every object open");
    }

    #[test]
    fn full_and_none_leave_the_model_alone() {
        for level in [CompatibilityLevel::Full, CompatibilityLevel::None] {
            check_content_model("s", "0001.json", level, &open_item()).expect("no rule");
            check_content_model("s", "0001.json", level, CLOSED).expect("no rule");
        }
    }
}
