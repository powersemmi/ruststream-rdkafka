//! `migrate run [--dry-run]`: every pending migration registered, in order, after the registry's
//! compatibility check.

use anyhow::{Context, Result, bail};
use ruststream_rdkafka::schema_registry::admin::SchemaReference;

use super::Workspace;
use super::prepare::refreshed;
use crate::catalog::{Catalog, Subject};
use crate::registry::{Registry, draft};
use crate::state::reconcile;

pub(crate) async fn run(workspace: &Workspace, dry_run: bool) -> Result<()> {
    let registry = workspace.registry("migrate run")?;
    let catalog = workspace.catalog()?;
    let previous = catalog.lock()?;
    let mut remotes = Vec::with_capacity(catalog.subjects.len());
    for subject in &catalog.subjects {
        remotes.push(Some(registry.remote(subject).await?));
    }
    // Drift anywhere, or a changed registered migration, stops the run before it registers.
    let mut lock = refreshed(&catalog, &previous, &remotes)?;
    let path = catalog.lock_path();
    if !dry_run {
        lock.write(&path)?;
    }
    let mut registered = 0_usize;
    for subject in order(&catalog)? {
        if !subject.is_owned() {
            continue;
        }
        set_level(registry, subject, dry_run).await?;
        let remote = registry.remote(subject).await?;
        for (migration, found) in subject.migrations.iter().zip(&remote.found) {
            if found.is_some() {
                continue;
            }
            let Some(references) = latest_references(registry, subject).await? else {
                if dry_run {
                    println!(
                        "{}: would register `{}` once the subjects it references are registered",
                        subject.name, migration.file
                    );
                    break;
                }
                bail!(
                    "subject `{}` references a subject the registry does not hold: register \
                     that subject first",
                    subject.name
                );
            };
            let draft = draft(subject, migration, references);
            println!(
                "{}: checking `{}` against the registry",
                subject.name, migration.file
            );
            let verdict = registry.verdict(subject, &migration.file, &draft).await?;
            if !verdict.is_compatible() {
                bail!(
                    "the registry refuses migration `{}` of subject `{}` under the subject's \
                     compatibility level (its messages are above); nothing after it was registered",
                    migration.file,
                    subject.name
                );
            }
            if dry_run {
                println!("{}: would register `{}`", subject.name, migration.file);
                // The next pending migration stands on this one, which the registry lacks.
                break;
            }
            let id = registry
                .admin()
                .register(&subject.name, &draft)
                .await
                .with_context(|| {
                    format!(
                        "registering migration `{}` of subject `{}`",
                        migration.file, subject.name
                    )
                })?;
            registered += 1;
            // Recorded at once: a run cut short keeps what it registered.
            let remote = registry.remote(subject).await?;
            *lock.subject_mut(&subject.name) =
                reconcile(subject, lock.subject(&subject.name), Some(&remote))?;
            lock.write(&path)?;
            let version = lock
                .subject(&subject.name)
                .and_then(|entry| entry.migration(&migration.file))
                .and_then(|entry| entry.version)
                .unwrap_or_default();
            println!(
                "{}: registered `{}` as version {version} (id {id})",
                subject.name, migration.file
            );
        }
    }
    if dry_run {
        println!("dry run: nothing was set or registered");
    } else {
        println!(
            "{registered} migration(s) registered; `{}` updated",
            path.display()
        );
    }
    Ok(())
}

/// Sets the subject's declared compatibility level where the registry holds another.
async fn set_level(registry: &Registry, subject: &Subject, dry_run: bool) -> Result<()> {
    let Some(level) = subject.level else {
        return Ok(());
    };
    let current = registry.admin().compatibility(&subject.name).await?;
    if current == Some(level) {
        return Ok(());
    }
    if dry_run {
        println!(
            "{}: would set the compatibility level to {level}",
            subject.name
        );
        return Ok(());
    }
    registry
        .admin()
        .set_compatibility(&subject.name, level)
        .await
        .with_context(|| {
            format!(
                "setting the compatibility level of subject `{}`",
                subject.name
            )
        })?;
    println!("{}: compatibility level set to {level}", subject.name);
    Ok(())
}

/// The subjects with every subject a subject references before it, so the registry can resolve
/// each import when it registers.
fn order(catalog: &Catalog) -> Result<Vec<&Subject>> {
    let mut ordered: Vec<&Subject> = Vec::with_capacity(catalog.subjects.len());
    let mut visiting = Vec::new();
    for subject in &catalog.subjects {
        visit(catalog, subject, &mut visiting, &mut ordered)?;
    }
    Ok(ordered)
}

fn visit<'catalog>(
    catalog: &'catalog Catalog,
    subject: &'catalog Subject,
    visiting: &mut Vec<String>,
    ordered: &mut Vec<&'catalog Subject>,
) -> Result<()> {
    if ordered.iter().any(|done| done.name == subject.name) {
        return Ok(());
    }
    if visiting.contains(&subject.name) {
        bail!(
            "subjects reference each other in a cycle: {} -> {}",
            visiting.join(" -> "),
            subject.name
        );
    }
    visiting.push(subject.name.clone());
    for reference in &subject.settings.references {
        if let Some(referenced) = catalog.subject(&reference.subject) {
            visit(catalog, referenced, visiting, ordered)?;
        }
    }
    visiting.pop();
    ordered.push(subject);
    Ok(())
}

/// The references of a migration about to be registered: the latest version of each referenced
/// subject, which the run registered first; `None` when one of them is not registered yet.
async fn latest_references(
    registry: &Registry,
    subject: &Subject,
) -> Result<Option<Vec<SchemaReference>>> {
    let mut references = Vec::with_capacity(subject.settings.references.len());
    for reference in &subject.settings.references {
        let versions = registry.admin().versions(&reference.subject).await?;
        let Some(&version) = versions.last() else {
            return Ok(None);
        };
        references.push(SchemaReference {
            name: reference.name.clone(),
            subject: reference.subject.clone(),
            version,
        });
    }
    Ok(Some(references))
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use super::*;
    use crate::testdir::TestDir;

    fn catalog(subjects: &[(&str, &[&str])]) -> (TestDir, Catalog) {
        let dir = TestDir::new();
        for (name, references) in subjects {
            let mut text = "format = \"protobuf\"\n".to_owned();
            for referenced in *references {
                let _ = write!(
                    text,
                    "\n[[references]]\nname = \"{referenced}.proto\"\nsubject = \"{referenced}\"\n"
                );
            }
            dir.write(&format!("schemas/{name}/subject.toml"), &text);
        }
        let catalog = Catalog::read(&dir.path("schemas")).expect("catalog");
        (dir, catalog)
    }

    #[test]
    fn referenced_subjects_come_first() {
        let (_dir, catalog) = catalog(&[
            ("a-payments", &["z-money", "m-common"]),
            ("m-common", &[]),
            ("z-money", &["m-common"]),
        ]);
        let names: Vec<_> = order(&catalog)
            .expect("order")
            .iter()
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(names, ["m-common", "z-money", "a-payments"]);
    }

    #[test]
    fn a_cycle_is_named() {
        let (_dir, catalog) = catalog(&[("a", &["b"]), ("b", &["a"])]);
        let err = order(&catalog).expect_err("cycle").to_string();
        assert_eq!(err, "subjects reference each other in a cycle: a -> b -> a");
    }
}
