//! `kafka-schema.lock`: per migration its checksum, and once registered its registry version and
//! schema id; per subject its full version list as of the last sync.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

const LOCK_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Lock {
    version: u32,
    #[serde(default, rename = "subject", skip_serializing_if = "Vec::is_empty")]
    pub(crate) subjects: Vec<LockSubject>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct LockSubject {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) registry_versions: Vec<u32>,
    #[serde(default, rename = "migration", skip_serializing_if = "Vec::is_empty")]
    pub(crate) migrations: Vec<LockMigration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LockMigration {
    pub(crate) file: String,
    pub(crate) sha256: String,
    /// Absent while the migration is pending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<u32>,
}

impl LockMigration {
    pub(crate) fn is_applied(&self) -> bool {
        self.version.is_some()
    }
}

impl Lock {
    pub(crate) fn new() -> Self {
        Self {
            version: LOCK_VERSION,
            subjects: Vec::new(),
        }
    }

    /// Reads the lock at `path`; a missing file is an empty lock.
    pub(crate) fn read(path: &Path) -> Result<Self> {
        let text = match fs::read_to_string(path) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Self::new()),
            Err(err) => return Err(err).with_context(|| format!("reading `{}`", path.display())),
        };
        let lock: Self = toml::from_str(&text)
            .with_context(|| format!("`{}` does not parse", path.display()))?;
        if lock.version != LOCK_VERSION {
            bail!(
                "`{}` is lock version {}, and this tool reads version {LOCK_VERSION}",
                path.display(),
                lock.version
            );
        }
        Ok(lock)
    }

    /// The text of the lock: subjects by name, migrations by file name (which orders them by
    /// number), no timestamps, so equal state writes equal bytes.
    pub(crate) fn render(&self) -> Result<String> {
        let mut sorted = self.clone();
        sorted.subjects.sort_by(|a, b| a.name.cmp(&b.name));
        for subject in &mut sorted.subjects {
            subject.migrations.sort_by(|a, b| a.file.cmp(&b.file));
            subject.registry_versions.sort_unstable();
        }
        let body = toml::to_string(&sorted).context("rendering the lock")?;
        Ok(format!(
            "# Written by `cargo kafka-schema`; commit it. The derive reads it offline.\n{body}"
        ))
    }

    pub(crate) fn write(&self, path: &Path) -> Result<()> {
        fs::write(path, self.render()?).with_context(|| format!("writing `{}`", path.display()))
    }

    pub(crate) fn subject(&self, name: &str) -> Option<&LockSubject> {
        self.subjects.iter().find(|subject| subject.name == name)
    }

    pub(crate) fn subject_mut(&mut self, name: &str) -> &mut LockSubject {
        if let Some(index) = self
            .subjects
            .iter()
            .position(|subject| subject.name == name)
        {
            return &mut self.subjects[index];
        }
        self.subjects.push(LockSubject {
            name: name.to_owned(),
            ..LockSubject::default()
        });
        self.subjects.last_mut().expect("just pushed")
    }
}

impl LockSubject {
    pub(crate) fn migration(&self, file: &str) -> Option<&LockMigration> {
        self.migrations
            .iter()
            .find(|migration| migration.file == file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testdir::TestDir;

    fn sample() -> Lock {
        let mut lock = Lock::new();
        let orders = lock.subject_mut("orders-value");
        orders.registry_versions = vec![2, 1];
        orders.migrations = vec![
            LockMigration {
                file: "0002_add_item.avsc".to_owned(),
                sha256: "bb".to_owned(),
                version: None,
                id: None,
            },
            LockMigration {
                file: "0001_create_order.avsc".to_owned(),
                sha256: "aa".to_owned(),
                version: Some(1),
                id: Some(100_001),
            },
        ];
        lock.subject_mut("inventory-value").registry_versions = vec![1];
        lock
    }

    #[test]
    fn renders_sorted_and_deterministically() {
        let text = sample().render().expect("render");
        assert_eq!(
            text,
            "# Written by `cargo kafka-schema`; commit it. The derive reads it offline.\n\
             version = 1\n\n\
             [[subject]]\nname = \"inventory-value\"\nregistry-versions = [1]\n\n\
             [[subject]]\nname = \"orders-value\"\nregistry-versions = [1, 2]\n\n\
             [[subject.migration]]\nfile = \"0001_create_order.avsc\"\nsha256 = \"aa\"\n\
             version = 1\nid = 100001\n\n\
             [[subject.migration]]\nfile = \"0002_add_item.avsc\"\nsha256 = \"bb\"\n"
        );
    }

    #[test]
    fn reads_back_what_it_writes_and_a_missing_file_is_empty() {
        let dir = TestDir::new();
        let path = dir.path("kafka-schema.lock");
        assert_eq!(Lock::read(&path).expect("missing"), Lock::new());
        sample().write(&path).expect("write");
        let read = Lock::read(&path).expect("read");
        assert_eq!(
            read.render().expect("render"),
            sample().render().expect("render")
        );
        let orders = read.subject("orders-value").expect("orders");
        assert!(
            orders
                .migration("0001_create_order.avsc")
                .expect("0001")
                .is_applied()
        );
        assert!(
            !orders
                .migration("0002_add_item.avsc")
                .expect("0002")
                .is_applied()
        );
    }

    #[test]
    fn another_lock_version_is_refused() {
        let dir = TestDir::new();
        dir.write("kafka-schema.lock", "version = 2\n");
        let err = Lock::read(&dir.path("kafka-schema.lock"))
            .expect_err("v2")
            .to_string();
        assert!(
            err.contains("is lock version 2, and this tool reads version 1"),
            "{err}"
        );
    }
}
