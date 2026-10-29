//! The lock `cargo kafka-schema` writes beside the subject directories, `kafka-schema.lock`: per
//! subject, the registry's version list as of the last sync and, per migration, its checksum and
//! the registry version and id it was registered as.
//!
//! ```toml
//! version = 1
//!
//! [[subject]]
//! name = "orders-value"
//! registry-versions = [1, 2]
//!
//! [[subject.migration]]
//! file = "0001_create_order.avsc"
//! sha256 = "<hex of the normalized content>"
//! version = 1
//! id = 100001
//! ```

use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use serde::Deserialize;

/// The lock format this crate reads.
pub(crate) const FORMAT_VERSION: u32 = 1;

/// The whole lock.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Lock {
    pub(crate) version: u32,
    /// Sorted by name.
    #[serde(default, rename = "subject")]
    pub(crate) subjects: Vec<LockedSubject>,
}

/// One subject of the lock.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct LockedSubject {
    pub(crate) name: String,
    /// The subject's full version list as of the last sync.
    #[serde(default)]
    pub(crate) registry_versions: Vec<u32>,
    /// Sorted by migration number.
    #[serde(default, rename = "migration")]
    pub(crate) migrations: Vec<LockedMigration>,
}

/// One migration of a locked subject.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LockedMigration {
    /// The migration's file name (`0001_create_order.avsc`).
    pub(crate) file: String,
    /// The hex SHA-256 of the normalized content.
    pub(crate) sha256: String,
    /// The registry version; absent while the migration is pending.
    pub(crate) version: Option<u32>,
    /// The registry id; absent while the migration is pending.
    pub(crate) id: Option<u32>,
}

impl Lock {
    /// The entry of the subject named `name`.
    pub(crate) fn subject(&self, name: &str) -> Option<&LockedSubject> {
        self.subjects.iter().find(|subject| subject.name == name)
    }
}

impl LockedSubject {
    /// The entry of the migration in `file`.
    pub(crate) fn migration(&self, file: &str) -> Option<&LockedMigration> {
        self.migrations
            .iter()
            .find(|migration| migration.file == file)
    }
}

/// Parses the text of a lock.
///
/// # Errors
///
/// Text that is not a lock of [`FORMAT_VERSION`].
pub(crate) fn parse(text: &str) -> Result<Lock, String> {
    let lock: Lock = toml::from_str(text).map_err(|err| err.to_string())?;
    if lock.version != FORMAT_VERSION {
        return Err(format!(
            "the lock is of format version {}, and this derive reads version {FORMAT_VERSION}; \
             upgrade `ruststream-rdkafka` to the release `cargo kafka-schema` matches",
            lock.version
        ));
    }
    Ok(lock)
}

/// Reads the lock at `path`; a missing file is no lock.
///
/// # Errors
///
/// A file that cannot be read or is not a lock.
pub(crate) fn read(path: &Path) -> Result<Option<Lock>, String> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(format!("reading `{}`: {err}", path.display())),
    };
    parse(&text)
        .map(Some)
        .map_err(|err| format!("`{}` is not a lock: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCK: &str = r#"
version = 1

[[subject]]
name = "orders-value"
registry-versions = [1, 2]

[[subject.migration]]
file = "0001_create_order.avsc"
sha256 = "aa"
version = 1
id = 100001

[[subject.migration]]
file = "0002_add_item.avsc"
sha256 = "bb"
version = 2
id = 100002

[[subject.migration]]
file = "0003_add_note.avsc"
sha256 = "cc"

[[subject]]
name = "payments-value"
"#;

    #[test]
    fn parses_subjects_and_migrations_pending_ones_included() -> Result<(), String> {
        let lock = parse(LOCK)?;
        assert_eq!(lock.version, 1);
        let orders = lock.subject("orders-value").ok_or("no orders-value")?;
        assert_eq!(orders.registry_versions, [1, 2]);
        assert_eq!(
            orders.migration("0002_add_item.avsc"),
            Some(&LockedMigration {
                file: "0002_add_item.avsc".into(),
                sha256: "bb".into(),
                version: Some(2),
                id: Some(100_002),
            })
        );
        let pending = orders.migration("0003_add_note.avsc").ok_or("no 0003")?;
        assert_eq!((pending.version, pending.id), (None, None));
        let payments = lock.subject("payments-value").ok_or("no payments-value")?;
        assert!(payments.registry_versions.is_empty() && payments.migrations.is_empty());
        assert_eq!(lock.subject("absent"), None);
        Ok(())
    }

    #[test]
    fn rejects_another_format_version() {
        let err = parse("version = 2\n").unwrap_err();
        assert!(err.contains("format version 2"), "{err}");
        assert!(err.contains("version 1"), "{err}");
    }

    #[test]
    fn rejects_an_unknown_key() {
        let err = parse("version = 1\n[[subject]]\nname = \"a\"\nregistry = [1]\n").unwrap_err();
        assert!(err.contains("unknown field `registry`"), "{err}");
    }

    #[test]
    fn a_missing_file_is_no_lock() -> Result<(), String> {
        let dir = std::env::temp_dir().join(format!("rs-lock-missing-{}", std::process::id()));
        assert_eq!(read(&dir.join("kafka-schema.lock"))?, None);
        Ok(())
    }

    #[test]
    fn reads_a_lock_file_and_names_it_when_it_does_not_parse() -> Result<(), String> {
        let dir = std::env::temp_dir().join(format!("rs-lock-read-{}", std::process::id()));
        fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
        let path = dir.join("kafka-schema.lock");
        fs::write(&path, LOCK).map_err(|err| err.to_string())?;
        assert_eq!(read(&path)?, Some(parse(LOCK)?));
        fs::write(&path, "version = \"one\"").map_err(|err| err.to_string())?;
        let err = read(&path).unwrap_err();
        fs::remove_dir_all(&dir).map_err(|err| err.to_string())?;
        assert!(err.contains(&path.display().to_string()), "{err}");
        Ok(())
    }
}
