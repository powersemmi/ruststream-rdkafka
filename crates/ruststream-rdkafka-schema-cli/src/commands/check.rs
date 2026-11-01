//! `check`: the CI gate. Exported schemas equal their newest migrations, the lock's checksums
//! hold, local compatibility holds, and with a registry, there is no drift and every pending
//! migration is one the registry would accept.

use std::fs;

use anyhow::Result;

use super::add::check_line;
use super::{Workspace, report};
use crate::catalog::{Catalog, Format, Subject};
use crate::export::Exports;
use crate::lock::Lock;
use crate::normalize::normalize;
use crate::registry::{Registry, draft};
use crate::state::{drift, reconcile, stale};

pub(crate) async fn run(workspace: &Workspace) -> Result<()> {
    let catalog = workspace.catalog()?;
    let lock = catalog.lock()?;
    let mut problems = Vec::new();
    lock_holds(&catalog, &lock, &mut problems);
    lines_hold(&catalog, &mut problems);
    sources_match(workspace, &catalog, &mut problems);
    if catalog.subjects.iter().any(exported) {
        let exports = Exports::run(&workspace.krate)?;
        exports_match(&catalog, &exports, &mut problems)?;
    }
    if let Some(registry) = &workspace.registry {
        registry_agrees(registry, &catalog, &lock, &mut problems).await?;
    }
    report(&problems, "check")?;
    println!("check passed: {} subject(s)", catalog.subjects.len());
    Ok(())
}

/// Whether the subject's newest migration is compared with an exported schema.
fn exported(subject: &Subject) -> bool {
    subject.is_owned()
        && subject.newest().is_some()
        && matches!(subject.format(), Format::Avro | Format::Json)
        // A union of records held by subjects of their own declares no record; its members are
        // checked through their own subjects.
        && (subject.settings.record.is_some() || subject.settings.references.is_empty())
}

fn lock_holds(catalog: &Catalog, lock: &Lock, problems: &mut Vec<String>) {
    for subject in &catalog.subjects {
        let recorded = lock.subject(&subject.name);
        match reconcile(subject, recorded, None) {
            Ok(expected) => {
                let lines = stale(&subject.name, recorded, &expected);
                if !lines.is_empty() {
                    problems.extend(lines);
                    problems.push(format!(
                        "the lock is stale for subject `{}`: refresh it with `cargo kafka-schema \
                         prepare`",
                        subject.name
                    ));
                }
            }
            Err(err) => problems.push(format!("{err:#}")),
        }
    }
    for recorded in &lock.subjects {
        if catalog.subject(&recorded.name).is_none() {
            problems.push(format!(
                "the lock records subject `{}`, which has no directory in `{}`",
                recorded.name,
                catalog.root.display()
            ));
        }
    }
}

fn lines_hold(catalog: &Catalog, problems: &mut Vec<String>) {
    for subject in &catalog.subjects {
        for (index, migration) in subject.migrations.iter().enumerate() {
            let earlier = &subject.migrations[..index];
            if let Err(err) = check_line(
                catalog,
                subject,
                &migration.file,
                &migration.content,
                earlier,
            ) {
                problems.push(format!("{err:#}"));
            }
        }
    }
}

fn sources_match(workspace: &Workspace, catalog: &Catalog, problems: &mut Vec<String>) {
    for subject in &catalog.subjects {
        if subject.format() != Format::Protobuf || !subject.is_owned() {
            continue;
        }
        let (Some(source), Some(newest)) = (&subject.settings.source, subject.newest()) else {
            continue;
        };
        let path = workspace.manifest_dir.join(source);
        match fs::read_to_string(&path) {
            Ok(text) if text.replace("\r\n", "\n") == newest.content.replace("\r\n", "\n") => {}
            Ok(_) => problems.push(format!(
                "`{source}` differs from `{}`, the newest migration of subject `{}`: describe the \
                 change with `cargo kafka-schema migrate add {} <description>`",
                newest.file, subject.name, subject.name
            )),
            Err(err) => problems.push(format!(
                "subject `{}` compiles `{source}`, which cannot be read: {err}",
                subject.name
            )),
        }
    }
}

fn exports_match(catalog: &Catalog, exports: &Exports, problems: &mut Vec<String>) -> Result<()> {
    for subject in catalog.subjects.iter().filter(|subject| exported(subject)) {
        let Some(newest) = subject.newest() else {
            continue;
        };
        let Some(record) = subject.settings.record.as_deref() else {
            problems.push(format!(
                "subject `{}` names no `record` in its subject.toml, so no exported schema can be \
                 compared with it: add `record = \"<full name>\"`",
                subject.name
            ));
            continue;
        };
        let format = subject.format();
        match exports.schema(record, format)? {
            None => problems.push(format!(
                "no type in the crate exported record `{record}` of subject `{}`: derive \
                 `Schema` on it",
                subject.name
            )),
            Some(text) => {
                let equal = normalize(format, &text)
                    .ok()
                    .zip(normalize(format, &newest.content).ok())
                    .is_some_and(|(exported, recorded)| exported == recorded);
                if !equal {
                    problems.push(format!(
                        "the exported schema of record `{record}` differs from `{}`, the newest \
                         migration of subject `{}`: describe the change with `cargo kafka-schema \
                         migrate add {} <description>`",
                        newest.file, subject.name, subject.name
                    ));
                }
            }
        }
    }
    Ok(())
}

async fn registry_agrees(
    registry: &Registry,
    catalog: &Catalog,
    lock: &Lock,
    problems: &mut Vec<String>,
) -> Result<()> {
    for subject in &catalog.subjects {
        let remote = registry.remote(subject).await?;
        problems.extend(drift(subject, lock.subject(&subject.name), &remote));
        if !subject.is_owned() {
            continue;
        }
        let references = registry.reference_candidates(subject).await?;
        for (migration, found) in subject.migrations.iter().zip(&remote.found) {
            if found.is_some() {
                continue;
            }
            let draft = draft(
                subject,
                migration,
                references.first().cloned().unwrap_or_default(),
            );
            let verdict = registry.verdict(subject, &migration.file, &draft).await?;
            if !verdict.is_compatible() {
                problems.push(format!(
                    "the registry would refuse pending migration `{}` of subject `{}` under its \
                     level (its messages are above)",
                    migration.file, subject.name
                ));
            }
            // Later pending migrations stand on this one, which the registry does not hold yet.
            break;
        }
    }
    Ok(())
}
