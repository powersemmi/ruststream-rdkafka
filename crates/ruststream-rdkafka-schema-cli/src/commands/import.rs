//! `migrate import`: every registry version of a subject as migrations, for a baseline or an
//! external subject.

use std::fs;

use anyhow::{Context, Result, bail};
use ruststream_rdkafka::schema_registry::admin::SubjectVersion;
use serde_json::Value;

use super::{Workspace, report};
use crate::catalog::{self, Format, Owner, Reference, Settings, Subject};
use crate::proto;
use crate::registry::Registry;
use crate::state::{drift, reconcile};

pub(crate) async fn run(
    workspace: &Workspace,
    name: &str,
    owner: Owner,
    record: Option<String>,
) -> Result<()> {
    let registry = workspace.registry("migrate import")?;
    let admin = registry.admin();
    let versions = admin
        .versions(name)
        .await
        .with_context(|| format!("listing the versions of subject `{name}`"))?;
    let Some(&latest) = versions.last() else {
        bail!(
            "the registry at {} holds no subject `{name}`",
            registry.shown()
        );
    };
    let subject = subject_of(workspace, registry, name, latest, owner, record).await?;
    let catalog = workspace.catalog()?;
    let remote = registry.remote(&subject).await?;
    if let Some(pending) = subject
        .migrations
        .iter()
        .zip(&remote.found)
        .find_map(|(migration, found)| found.is_none().then_some(&migration.file))
    {
        bail!(
            "migration `{pending}` of subject `{name}` is not in the registry; an import appends \
             the registry's versions after the migrations it holds, so register or remove it first"
        );
    }
    let described = remote
        .found
        .iter()
        .flatten()
        .map(|found| found.version)
        .max();
    let (later, earlier): (Vec<u32>, Vec<u32>) = remote
        .versions
        .iter()
        .filter(|version| {
            !remote
                .found
                .iter()
                .flatten()
                .any(|found| found.version == **version)
        })
        .partition(|version| described.is_none_or(|described| **version > described));
    if !earlier.is_empty() {
        let lock = catalog.lock()?;
        let mut lines = drift(&subject, lock.subject(name), &remote);
        lines.insert(
            0,
            format!(
                "versions {earlier:?} of subject `{name}` sit between versions the migrations \
                 describe, and a migration cannot be inserted before an existing one"
            ),
        );
        return report(&lines, "import");
    }
    let format = subject.format();
    let mut number = subject.migrations.last().map_or(0, |last| last.number);
    for version in &later {
        let fetched = admin.version(name, *version).await?;
        if Format::of(fetched.schema_type()) != format {
            bail!(
                "version {version} of subject `{name}` is {:?}, and the subject's directory holds \
                 {} migrations",
                fetched.schema_type(),
                format.name()
            );
        }
        number += 1;
        let path = subject
            .dir
            .join(format!("{number:04}.{}", format.extension()));
        // Written as the registry returns it: the registry finds a schema by its own canonical
        // form, and for JSON Schema that form keeps the key order.
        fs::write(&path, fetched.definition())
            .with_context(|| format!("writing `{}`", path.display()))?;
        println!("wrote `{}` (version {version})", path.display());
    }
    let catalog = workspace.catalog()?;
    let subject = catalog.subject(name).context("the subject just written")?;
    let remote = registry.remote(subject).await?;
    let mut lock = catalog.lock()?;
    let lines = drift(subject, lock.subject(name), &remote);
    report(&lines, "import")?;
    let entry = reconcile(subject, lock.subject(name), Some(&remote))?;
    *lock.subject_mut(name) = entry;
    lock.write(&catalog.lock_path())?;
    if later.is_empty() {
        println!("subject `{name}`: every registry version is described already");
    }
    Ok(())
}

/// The subject's directory, created from the registry's newest version when it is missing.
async fn subject_of(
    workspace: &Workspace,
    registry: &Registry,
    name: &str,
    latest: u32,
    owner: Owner,
    record: Option<String>,
) -> Result<Subject> {
    let catalog = workspace.catalog()?;
    if let Some(existing) = catalog.subject(name) {
        return Ok(existing.clone());
    }
    let admin = registry.admin();
    let newest = admin.version(name, latest).await?;
    let level = admin.compatibility(name).await?;
    let settings = settings_of(name, &newest, owner, record)?;
    catalog::write_settings(
        &catalog.root.join(name),
        &Settings {
            compatibility: level.map(|level| level.as_api().to_owned()),
            ..settings
        },
    )?;
    workspace
        .catalog()?
        .subject(name)
        .cloned()
        .context("the subject just written")
}

/// The `subject.toml` of a subject the registry holds and the directory does not.
fn settings_of(
    name: &str,
    newest: &SubjectVersion,
    owner: Owner,
    record: Option<String>,
) -> Result<Settings> {
    let format = Format::of(newest.schema_type());
    let record = match (record, format) {
        (Some(record), _) => Some(record),
        (None, Format::Avro) => avro_record(newest.definition()),
        (None, Format::Protobuf) => {
            Some(proto::first_message(name, "registry", newest.definition())?)
        }
        (None, Format::Json) => bail!(
            "subject `{name}` is a JSON Schema subject: name the record it declares with \
             `--record <namespace>.<type>`"
        ),
    };
    Ok(Settings {
        format,
        compatibility: None,
        owner,
        record,
        source: None,
        references: newest
            .references()
            .iter()
            .map(|reference| Reference {
                name: reference.name.clone(),
                subject: reference.subject.clone(),
            })
            .collect(),
    })
}

/// The full name of an Avro schema's top-level named type, when it has one.
fn avro_record(definition: &str) -> Option<String> {
    let value: Value = serde_json::from_str(definition).ok()?;
    let name = value.get("name")?.as_str()?;
    if name.contains('.') {
        return Some(name.to_owned());
    }
    match value.get("namespace").and_then(Value::as_str) {
        Some(namespace) if !namespace.is_empty() => Some(format!("{namespace}.{name}")),
        _ => Some(name.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_avro_record_is_the_full_name_of_the_top_level_type() {
        let namespaced = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[]}"#;
        assert_eq!(avro_record(namespaced).as_deref(), Some("com.acme.Order"));
        let dotted = r#"{"type":"record","name":"com.acme.Order","namespace":"x","fields":[]}"#;
        assert_eq!(avro_record(dotted).as_deref(), Some("com.acme.Order"));
        assert_eq!(avro_record(r#"["com.acme.A","com.acme.B"]"#), None);
        assert_eq!(avro_record(r#""string""#), None);
    }
}
