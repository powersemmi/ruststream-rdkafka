//! The migrations directory: one directory per subject, its `subject.toml`, and its numbered
//! migrations, each the full schema of one subject version.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use ruststream_rdkafka::schema_registry::SchemaType;
use ruststream_rdkafka::schema_registry::admin::CompatibilityLevel;
use serde::{Deserialize, Serialize};

use crate::lock::Lock;

/// The settings file of a subject directory.
pub(crate) const SUBJECT_FILE: &str = "subject.toml";

/// The lock file beside the subject directories.
pub(crate) const LOCK_FILE: &str = "kafka-schema.lock";

/// The format a subject's schemas are written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
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

    pub(crate) const fn schema_type(self) -> SchemaType {
        match self {
            Self::Avro => SchemaType::Avro,
            Self::Json => SchemaType::Json,
            Self::Protobuf => SchemaType::Protobuf,
        }
    }

    pub(crate) fn of(schema_type: SchemaType) -> Self {
        match schema_type {
            SchemaType::Protobuf => Self::Protobuf,
            SchemaType::Json => Self::Json,
            _ => Self::Avro,
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Avro => "avro",
            Self::Json => "json",
            Self::Protobuf => "protobuf",
        }
    }
}

/// Who writes a subject: this service registers it, or another service does and this one only
/// pulls its versions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Owner {
    #[default]
    Service,
    External,
}

impl Owner {
    // serde's `skip_serializing_if` hands the field over by reference.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn is_service(&self) -> bool {
        *self == Self::Service
    }
}

/// A schema another subject's schema refers to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reference {
    pub(crate) name: String,
    pub(crate) subject: String,
}

/// The contents of `subject.toml`, as written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Settings {
    pub(crate) format: Format,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) compatibility: Option<String>,
    #[serde(default, skip_serializing_if = "Owner::is_service")]
    pub(crate) owner: Owner,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) record: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) source: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) references: Vec<Reference>,
}

/// One migration: the full schema of one subject version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Migration {
    pub(crate) number: u32,
    /// The file name (`0002_add_item.avsc`).
    pub(crate) file: String,
    pub(crate) content: String,
}

/// One subject directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Subject {
    pub(crate) name: String,
    pub(crate) dir: PathBuf,
    pub(crate) settings: Settings,
    /// The compatibility level of `subject.toml`, parsed.
    pub(crate) level: Option<CompatibilityLevel>,
    /// In number order, `0001` first.
    pub(crate) migrations: Vec<Migration>,
}

impl Subject {
    pub(crate) fn newest(&self) -> Option<&Migration> {
        self.migrations.last()
    }

    pub(crate) fn is_owned(&self) -> bool {
        self.settings.owner == Owner::Service
    }

    pub(crate) fn format(&self) -> Format {
        self.settings.format
    }

    /// The level the local checks hold the subject to: the declared one, or the registry's own
    /// default, `BACKWARD`, which is what an undeclared subject gets on a fresh registry.
    pub(crate) fn effective_level(&self) -> CompatibilityLevel {
        self.level.unwrap_or(CompatibilityLevel::Backward)
    }

    /// The file name of the next migration with `description`.
    pub(crate) fn next_file(&self, description: &str) -> String {
        let number = self.migrations.last().map_or(1, |last| last.number + 1);
        format!("{number:04}_{description}.{}", self.format().extension())
    }
}

/// The migrations directory of a crate.
#[derive(Debug, Clone)]
pub(crate) struct Catalog {
    pub(crate) root: PathBuf,
    /// Sorted by name.
    pub(crate) subjects: Vec<Subject>,
}

impl Catalog {
    /// Reads every subject under `root`. A missing directory is an empty catalog.
    pub(crate) fn read(root: &Path) -> Result<Self> {
        let mut subjects = Vec::new();
        if root.is_dir() {
            for entry in
                fs::read_dir(root).with_context(|| format!("reading `{}`", root.display()))?
            {
                let path = entry
                    .with_context(|| format!("reading `{}`", root.display()))?
                    .path();
                if path.is_dir() {
                    subjects.push(read_subject(&path)?);
                }
            }
        }
        subjects.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Self {
            root: root.to_owned(),
            subjects,
        })
    }

    pub(crate) fn subject(&self, name: &str) -> Option<&Subject> {
        self.subjects.iter().find(|subject| subject.name == name)
    }

    pub(crate) fn lock_path(&self) -> PathBuf {
        self.root.join(LOCK_FILE)
    }

    /// The lock beside the subjects; an empty one when there is none yet.
    pub(crate) fn lock(&self) -> Result<Lock> {
        Lock::read(&self.lock_path())
    }
}

/// Writes `subject.toml` into `dir`, creating the directory.
pub(crate) fn write_settings(dir: &Path, settings: &Settings) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("creating `{}`", dir.display()))?;
    let text = toml::to_string(settings).context("writing subject.toml")?;
    let path = dir.join(SUBJECT_FILE);
    fs::write(&path, text).with_context(|| format!("writing `{}`", path.display()))
}

/// Whether `description` can name a migration: lower-case letters, digits and underscores.
pub(crate) fn check_description(description: &str) -> Result<()> {
    let valid = !description.is_empty()
        && description
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !valid {
        bail!(
            "`{description}` cannot name a migration: use lower-case letters, digits and \
             underscores (`add_item`)"
        );
    }
    Ok(())
}

fn read_subject(dir: &Path) -> Result<Subject> {
    let name = dir
        .file_name()
        .and_then(|name| name.to_str())
        .with_context(|| format!("subject directory `{}` is not UTF-8", dir.display()))?
        .to_owned();
    let settings_path = dir.join(SUBJECT_FILE);
    let text = fs::read_to_string(&settings_path).with_context(|| {
        format!(
            "subject `{name}` has no readable `{SUBJECT_FILE}` (`{}`)",
            settings_path.display()
        )
    })?;
    let settings: Settings = toml::from_str(&text)
        .with_context(|| format!("`{}` does not parse", settings_path.display()))?;
    let level = settings
        .compatibility
        .as_deref()
        .map(str::parse::<CompatibilityLevel>)
        .transpose()
        .with_context(|| format!("`{}`: `compatibility`", settings_path.display()))?;
    let mut migrations = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("reading `{}`", dir.display()))? {
        let path = entry
            .with_context(|| format!("reading `{}`", dir.display()))?
            .path();
        let Some(file) = path.file_name().and_then(|file| file.to_str()) else {
            bail!("`{}` is not UTF-8", path.display());
        };
        if file == SUBJECT_FILE || path.is_dir() {
            continue;
        }
        let number = migration_number(&name, settings.format, file)?;
        let content =
            fs::read_to_string(&path).with_context(|| format!("reading `{}`", path.display()))?;
        migrations.push(Migration {
            number,
            file: file.to_owned(),
            content,
        });
    }
    migrations.sort_by_key(|migration| migration.number);
    for (expected, migration) in (1..).zip(&migrations) {
        if migration.number != expected {
            let previous = migrations
                .iter()
                .find(|other| other.number == migration.number && other.file != migration.file);
            match previous {
                Some(other) => bail!(
                    "subject `{name}` has two migrations numbered {:04}: `{}` and `{}`",
                    migration.number,
                    other.file,
                    migration.file
                ),
                None => bail!(
                    "subject `{name}` has no migration {expected:04}: the numbering jumps to `{}`",
                    migration.file
                ),
            }
        }
    }
    Ok(Subject {
        name,
        dir: dir.to_owned(),
        settings,
        level,
        migrations,
    })
}

/// The number of a migration file `NNNN_<description>.<ext>` or `NNNN.<ext>`.
fn migration_number(subject: &str, format: Format, file: &str) -> Result<u32> {
    let ext = format.extension();
    let stem = file
        .strip_suffix(ext)
        .and_then(|stem| stem.strip_suffix('.'))
        .with_context(|| {
            format!(
                "`{file}` in subject `{subject}` is not a migration: a `{}` subject holds \
                 `NNNN_<description>.{ext}` files",
                format.name()
            )
        })?;
    let (digits, rest) = stem.split_at(stem.len().min(4));
    let number = digits.parse::<u32>().ok().filter(|_| {
        digits.len() == 4
            && digits.bytes().all(|b| b.is_ascii_digit())
            && (rest.is_empty()
                || rest
                    .strip_prefix('_')
                    .is_some_and(|d| check_description(d).is_ok()))
    });
    match number {
        Some(number) if number > 0 => Ok(number),
        _ => bail!(
            "`{file}` in subject `{subject}` is not named `NNNN_<description>.{ext}` or \
             `NNNN.{ext}`"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testdir::TestDir;

    #[test]
    fn reads_subjects_sorted_with_their_migrations_in_number_order() {
        let dir = TestDir::new();
        dir.write("schemas/orders-value/subject.toml", "format = \"avro\"\ncompatibility = \"BACKWARD_TRANSITIVE\"\nrecord = \"com.acme.Order\"\n");
        dir.write("schemas/orders-value/0002_add_item.avsc", "{}");
        dir.write("schemas/orders-value/0001_create_order.avsc", "{}");
        dir.write(
            "schemas/inventory-value/subject.toml",
            "format = \"avro\"\nowner = \"external\"\n",
        );
        dir.write("schemas/inventory-value/0001.avsc", "{}");
        let catalog = Catalog::read(&dir.path("schemas")).expect("catalog");
        let names: Vec<_> = catalog.subjects.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["inventory-value", "orders-value"]);
        let orders = catalog.subject("orders-value").expect("orders");
        let files: Vec<_> = orders.migrations.iter().map(|m| m.file.as_str()).collect();
        assert_eq!(files, ["0001_create_order.avsc", "0002_add_item.avsc"]);
        assert_eq!(orders.level, Some(CompatibilityLevel::BackwardTransitive));
        assert!(orders.is_owned());
        assert!(
            !catalog
                .subject("inventory-value")
                .expect("inventory")
                .is_owned()
        );
        assert_eq!(orders.next_file("add_note"), "0003_add_note.avsc");
    }

    #[test]
    fn a_missing_directory_is_an_empty_catalog() {
        let dir = TestDir::new();
        let catalog = Catalog::read(&dir.path("schemas")).expect("empty");
        assert_eq!(catalog.subjects, Vec::new());
    }

    #[test]
    fn gaps_repeats_and_strangers_are_refused() {
        let dir = TestDir::new();
        dir.write("schemas/s/subject.toml", "format = \"avro\"\n");
        dir.write("schemas/s/0001_a.avsc", "{}");
        dir.write("schemas/s/0003_c.avsc", "{}");
        let err = Catalog::read(&dir.path("schemas"))
            .expect_err("gap")
            .to_string();
        assert_eq!(
            err,
            "subject `s` has no migration 0002: the numbering jumps to `0003_c.avsc`"
        );

        dir.write("schemas/s/0003_c.avsc", "");
        fs::remove_file(dir.path("schemas/s/0003_c.avsc")).expect("rm");
        dir.write("schemas/s/0001_b.avsc", "{}");
        let err = Catalog::read(&dir.path("schemas"))
            .expect_err("repeat")
            .to_string();
        assert!(
            err.starts_with("subject `s` has two migrations numbered 0001"),
            "{err}"
        );

        fs::remove_file(dir.path("schemas/s/0001_b.avsc")).expect("rm");
        dir.write("schemas/s/0002_b.proto", "");
        let err = format!(
            "{:#}",
            Catalog::read(&dir.path("schemas")).expect_err("stranger")
        );
        assert!(
            err.contains("`0002_b.proto` in subject `s` is not a migration"),
            "{err}"
        );

        fs::remove_file(dir.path("schemas/s/0002_b.proto")).expect("rm");
        dir.write("schemas/s/2_b.avsc", "");
        let err = format!(
            "{:#}",
            Catalog::read(&dir.path("schemas")).expect_err("name")
        );
        assert!(
            err.contains("is not named `NNNN_<description>.avsc`"),
            "{err}"
        );
    }

    #[test]
    fn a_wrong_level_or_unknown_key_names_the_file() {
        let dir = TestDir::new();
        dir.write(
            "schemas/s/subject.toml",
            "format = \"avro\"\ncompatibility = \"backward\"\n",
        );
        let err = format!(
            "{:#}",
            Catalog::read(&dir.path("schemas")).expect_err("level")
        );
        assert!(err.contains("subject.toml`: `compatibility`"), "{err}");
        assert!(
            err.contains("`backward` is not a compatibility level"),
            "{err}"
        );
        dir.write(
            "schemas/s/subject.toml",
            "format = \"avro\"\nrecrd = \"x\"\n",
        );
        let err = format!(
            "{:#}",
            Catalog::read(&dir.path("schemas")).expect_err("key")
        );
        assert!(err.contains("does not parse"), "{err}");
    }

    #[test]
    fn settings_write_only_what_is_set() {
        let dir = TestDir::new();
        let settings = Settings {
            format: Format::Protobuf,
            compatibility: Some("BACKWARD".to_owned()),
            owner: Owner::Service,
            record: Some("acme.Payment".to_owned()),
            source: Some("proto/payments.proto".to_owned()),
            references: vec![Reference {
                name: "money.proto".to_owned(),
                subject: "money-value".to_owned(),
            }],
        };
        write_settings(&dir.path("schemas/p"), &settings).expect("write");
        let text = dir.read("schemas/p/subject.toml");
        assert_eq!(
            text,
            "format = \"protobuf\"\ncompatibility = \"BACKWARD\"\nrecord = \"acme.Payment\"\n\
             source = \"proto/payments.proto\"\n\n[[references]]\nname = \"money.proto\"\n\
             subject = \"money-value\"\n"
        );
        let read = Catalog::read(&dir.path("schemas")).expect("read back");
        assert_eq!(read.subjects[0].settings, settings);
    }

    #[test]
    fn descriptions_are_snake_case() {
        check_description("add_item_2").expect("valid");
        for bad in ["", "Add", "add-item", "add item"] {
            assert!(check_description(bad).is_err(), "{bad}");
        }
    }
}
