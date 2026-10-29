//! The migrations directory as the derive reads it: one directory per subject, its
//! `subject.toml`, and its numbered migrations, each the full schema of one subject version.
//!
//! ```text
//! schemas/
//!   kafka-schema.lock
//!   orders-value/
//!     subject.toml
//!     0001_create_order.avsc
//!     0002_add_item.avsc
//! ```

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::Value;

/// The file the tool writes beside the subject directories.
pub(crate) const LOCK_FILE: &str = "kafka-schema.lock";

/// The settings file of a subject directory.
pub(crate) const SUBJECT_FILE: &str = "subject.toml";

/// Where the migrations live: `RUSTSTREAM_SCHEMA_DIR` (absolute, or relative to the crate's
/// manifest directory) when it is set, `schemas/` beside the crate's `Cargo.toml` otherwise.
pub(crate) fn schema_dir() -> PathBuf {
    let manifest = env::var_os("CARGO_MANIFEST_DIR").map_or_else(PathBuf::new, PathBuf::from);
    match env::var_os("RUSTSTREAM_SCHEMA_DIR") {
        Some(dir) => manifest.join(dir),
        None => manifest.join("schemas"),
    }
}

/// The format a subject's schemas are written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Format {
    Avro,
    Json,
    Protobuf,
}

impl Format {
    /// The extension of this format's migration files.
    pub(crate) const fn extension(self) -> &'static str {
        match self {
            Self::Avro => "avsc",
            Self::Json => "json",
            Self::Protobuf => "proto",
        }
    }
}

/// Who writes a subject: this service registers it, or another service does and this one
/// only pulls its versions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Owner {
    #[default]
    Service,
    External,
}

/// A schema another subject's schema refers to (a Protobuf import, a JSON `$ref`, a member of
/// an Avro union).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reference {
    /// The name the referring schema uses: the import path, the `$ref`, the record's full name.
    pub(crate) name: String,
    /// The subject that holds the referenced schema.
    pub(crate) subject: String,
}

/// The contents of `subject.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Settings {
    pub(crate) format: Format,
    /// The compatibility level the tool sets on the registry (`BACKWARD_TRANSITIVE`, ..).
    pub(crate) compatibility: Option<String>,
    #[serde(default)]
    pub(crate) owner: Owner,
    /// The record the subject declares: the Avro record's full name, the Protobuf package and
    /// message, or for JSON Schema the namespace plus the type name.
    pub(crate) record: Option<String>,
    /// The `.proto` file `prost-build` compiles, for a Protobuf subject.
    pub(crate) source: Option<String>,
    #[serde(default)]
    pub(crate) references: Vec<Reference>,
}

/// One migration: the full schema of one subject version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Migration {
    pub(crate) number: u32,
    /// The file name without its extension (`0002_add_item`).
    pub(crate) stem: String,
    pub(crate) path: PathBuf,
    pub(crate) content: String,
}

/// One subject directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Subject {
    pub(crate) name: String,
    pub(crate) settings: Settings,
    /// In number order, `0001` first.
    pub(crate) migrations: Vec<Migration>,
}

impl Subject {
    /// The migration the type is checked against.
    pub(crate) fn newest(&self) -> Option<&Migration> {
        self.migrations.last()
    }

    /// The full names of the records the newest migration declares: for Avro every named type
    /// it defines (nested ones included) and every name a top-level union refers to; for JSON
    /// Schema and Protobuf the `record` of `subject.toml`.
    ///
    /// # Errors
    ///
    /// An Avro migration that is not JSON.
    pub(crate) fn declared_records(&self) -> Result<Vec<String>, String> {
        match self.settings.format {
            Format::Avro => {
                let Some(newest) = self.newest() else {
                    return Ok(Vec::new());
                };
                let schema: Value = serde_json::from_str(&newest.content).map_err(|err| {
                    format!("migration `{}` is not JSON: {err}", newest.path.display())
                })?;
                let mut names = Vec::new();
                if let Value::Array(members) = &schema {
                    for member in members {
                        if let Value::String(name) = member
                            && !is_primitive(name)
                        {
                            names.push(name.clone());
                        }
                    }
                }
                avro_named_types(&schema, None, &mut names);
                Ok(names)
            }
            Format::Json | Format::Protobuf => Ok(self.settings.record.iter().cloned().collect()),
        }
    }
}

impl Subject {
    /// The migrations the derive checks a type against: the newest one for a subject this
    /// service owns, every pulled version for an external one.
    pub(crate) fn checked_migrations(&self) -> &[Migration] {
        match self.settings.owner {
            Owner::Service => self.migrations.len().checked_sub(1).map_or(&[], |newest| {
                &self.migrations[newest..]
            }),
            Owner::External => &self.migrations,
        }
    }

    /// The full names of the named types an Avro migration defines, nested ones included.
    ///
    /// # Errors
    ///
    /// A migration that is not JSON.
    pub(crate) fn named_types_of(migration: &Migration) -> Result<Vec<String>, String> {
        let schema: Value = serde_json::from_str(&migration.content).map_err(|err| {
            format!("migration `{}` is not JSON: {err}", migration.path.display())
        })?;
        let mut names = Vec::new();
        avro_named_types(&schema, None, &mut names);
        Ok(names)
    }
}

/// Every subject directory under `dir`, sorted by subject name. A missing directory is an
/// empty catalog: a crate with no migrations derives `Schema` with no checks.
///
/// # Errors
///
/// A `subject.toml` that is missing or does not parse, a file that is not a migration of the
/// subject's format, two migrations with one number, or a gap in the numbering.
pub(crate) fn read_catalog(dir: &Path) -> Result<Vec<Subject>, String> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(Vec::new());
    };
    let mut subjects = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| format!("reading `{}`: {err}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            subjects.push(read_subject(&path)?);
        }
    }
    subjects.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(subjects)
}

fn read_subject(dir: &Path) -> Result<Subject, String> {
    let name = dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("subject directory `{}` is not UTF-8", dir.display()))?
        .to_owned();
    let settings_path = dir.join(SUBJECT_FILE);
    let text = fs::read_to_string(&settings_path).map_err(|err| {
        format!(
            "subject `{name}` has no readable `{SUBJECT_FILE}` (`{}`): {err}",
            settings_path.display()
        )
    })?;
    let settings: Settings = toml::from_str(&text)
        .map_err(|err| format!("`{}` does not parse: {err}", settings_path.display()))?;
    let mut migrations = Vec::new();
    let entries = fs::read_dir(dir).map_err(|err| format!("reading `{}`: {err}", dir.display()))?;
    for entry in entries {
        let path = entry
            .map_err(|err| format!("reading `{}`: {err}", dir.display()))?
            .path();
        if path.file_name().is_some_and(|file| file == SUBJECT_FILE) {
            continue;
        }
        migrations.push(read_migration(&name, settings.format, path)?);
    }
    migrations.sort_by_key(|migration| migration.number);
    for (expected, migration) in (1..).zip(&migrations) {
        if migration.number != expected {
            return Err(format!(
                "subject `{name}`: migration `{}` is numbered {:04}, where {expected:04} is \
                 expected; migrations are numbered from 0001 with no gaps or repeats",
                migration.stem, migration.number,
            ));
        }
    }
    Ok(Subject {
        name,
        settings,
        migrations,
    })
}

fn read_migration(subject: &str, format: Format, path: PathBuf) -> Result<Migration, String> {
    let file = path
        .file_name()
        .and_then(|file| file.to_str())
        .unwrap_or_default();
    let shape = || {
        format!(
            "subject `{subject}`: `{file}` is not a migration; a migration is named \
             `NNNN_<description>.{}`",
            format.extension()
        )
    };
    let stem = file
        .strip_suffix(format.extension())
        .and_then(|stem| stem.strip_suffix('.'))
        .ok_or_else(shape)?;
    let digits = stem.get(..4).ok_or_else(shape)?;
    let rest = &stem[4..];
    if !digits.bytes().all(|byte| byte.is_ascii_digit())
        || !(rest.is_empty() || rest.starts_with('_'))
    {
        return Err(shape());
    }
    let number = digits.parse().map_err(|_| shape())?;
    let content = fs::read_to_string(&path)
        .map_err(|err| format!("reading migration `{}`: {err}", path.display()))?;
    Ok(Migration {
        number,
        stem: stem.to_owned(),
        path,
        content,
    })
}

fn is_primitive(name: &str) -> bool {
    matches!(
        name,
        "null" | "boolean" | "int" | "long" | "float" | "double" | "bytes" | "string"
    )
}

/// Collects the full names of the named types (`record`, `enum`, `fixed`) a schema defines.
fn avro_named_types(schema: &Value, namespace: Option<&str>, names: &mut Vec<String>) {
    match schema {
        Value::Array(members) => {
            for member in members {
                avro_named_types(member, namespace, names);
            }
        }
        Value::Object(object) => {
            let kind = object.get("type");
            let is_named = matches!(
                kind.and_then(Value::as_str),
                Some("record" | "error" | "enum" | "fixed")
            );
            let mut inner = namespace.map(str::to_owned);
            if is_named && let Some(name) = object.get("name").and_then(Value::as_str) {
                let full = if name.contains('.') {
                    name.to_owned()
                } else {
                    match object
                        .get("namespace")
                        .and_then(Value::as_str)
                        .or(namespace)
                    {
                        Some(space) if !space.is_empty() => format!("{space}.{name}"),
                        _ => name.to_owned(),
                    }
                };
                inner = full.rsplit_once('.').map(|(space, _)| space.to_owned());
                names.push(full);
            }
            if let Some(Value::Array(fields)) = object.get("fields") {
                for field in fields {
                    if let Some(ty) = field.get("type") {
                        avro_named_types(ty, inner.as_deref(), names);
                    }
                }
            }
            for key in ["items", "values"] {
                if let Some(ty) = object.get(key) {
                    avro_named_types(ty, inner.as_deref(), names);
                }
            }
            if !is_named && let Some(ty) = kind.filter(|ty| !ty.is_string()) {
                avro_named_types(ty, inner.as_deref(), names);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::process;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// A fresh directory per test under the system temporary directory.
    fn scratch(files: &[(&str, &str)]) -> PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = env::temp_dir().join(format!(
            "ruststream-schema-macros-{}-{}",
            process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        for (path, content) in files {
            let path = dir.join(path);
            fs::create_dir_all(path.parent().expect("a file sits in a directory"))
                .expect("the scratch directory is writable");
            fs::write(&path, content).expect("the scratch file is writable");
        }
        dir
    }

    const ORDER_V2: &str = r#"{"type":"record","name":"Order","namespace":"com.acme.orders",
        "fields":[{"name":"id","type":"long"},
        {"name":"item","type":{"type":"record","name":"Item","fields":[{"name":"sku","type":"string"}]}}]}"#;

    #[test]
    fn a_subject_reads_its_migrations_in_number_order_and_the_newest_is_the_last() {
        let dir = scratch(&[
            (
                "orders-value/subject.toml",
                "format = \"avro\"\ncompatibility = \"BACKWARD\"\n",
            ),
            ("orders-value/0002_add_item.avsc", ORDER_V2),
            (
                "orders-value/0001_create_order.avsc",
                r#"{"type":"record","name":"Order","fields":[]}"#,
            ),
        ]);
        let catalog = read_catalog(&dir).expect("a well-formed catalog reads");
        let [orders] = catalog.as_slice() else {
            panic!("one subject directory is one subject: {catalog:?}");
        };
        assert_eq!(orders.name, "orders-value");
        assert_eq!(orders.settings.owner, Owner::Service);
        assert_eq!(
            orders.newest().map(|m| m.stem.as_str()),
            Some("0002_add_item")
        );
        assert_eq!(
            orders.declared_records(),
            Ok(vec![
                "com.acme.orders.Order".to_owned(),
                "com.acme.orders.Item".to_owned()
            ])
        );
    }

    #[test]
    fn a_union_of_references_declares_every_record_it_names() {
        let dir = scratch(&[
            ("orders-value/subject.toml", "format = \"avro\"\n"),
            (
                "orders-value/0001_events.avsc",
                r#"["com.acme.orders.OrderCreated", "com.acme.orders.OrderCancelled"]"#,
            ),
        ]);
        let catalog = read_catalog(&dir).expect("a union migration reads");
        assert_eq!(
            catalog[0].declared_records(),
            Ok(vec![
                "com.acme.orders.OrderCreated".to_owned(),
                "com.acme.orders.OrderCancelled".to_owned()
            ])
        );
    }

    #[test]
    fn a_json_subject_declares_the_record_its_settings_name() {
        let dir = scratch(&[
            (
                "orders-value/subject.toml",
                "format = \"json\"\nowner = \"external\"\nrecord = \"com.acme.Order\"\n",
            ),
            ("orders-value/0001.json", "{}"),
        ]);
        let catalog = read_catalog(&dir).expect("an imported migration without description reads");
        assert_eq!(catalog[0].settings.owner, Owner::External);
        assert_eq!(
            catalog[0].declared_records(),
            Ok(vec!["com.acme.Order".to_owned()])
        );
    }

    #[test]
    fn a_gap_in_the_numbering_names_the_migration() {
        let dir = scratch(&[
            ("orders-value/subject.toml", "format = \"avro\"\n"),
            ("orders-value/0001_a.avsc", "{}"),
            ("orders-value/0003_c.avsc", "{}"),
        ]);
        let err = read_catalog(&dir).expect_err("a gap means a migration went missing");
        assert!(
            err.contains("`0003_c` is numbered 0003, where 0002"),
            "{err}"
        );
    }

    #[test]
    fn a_file_of_another_format_is_not_a_migration() {
        let dir = scratch(&[
            ("orders-value/subject.toml", "format = \"avro\"\n"),
            ("orders-value/0001_a.json", "{}"),
        ]);
        let err = read_catalog(&dir).expect_err("an Avro subject holds .avsc files");
        assert!(err.contains("`0001_a.json` is not a migration"), "{err}");
    }

    #[test]
    fn a_missing_directory_is_an_empty_catalog() {
        let dir = env::temp_dir().join("ruststream-schema-macros-absent");
        assert_eq!(read_catalog(&dir), Ok(Vec::new()));
    }

    #[test]
    fn an_unknown_setting_is_refused() {
        let dir = scratch(&[(
            "orders-value/subject.toml",
            "format = \"avro\"\nowner2 = 1\n",
        )]);
        let err = read_catalog(&dir).expect_err("a typo in a setting is refused");
        assert!(err.contains("does not parse"), "{err}");
    }
}
