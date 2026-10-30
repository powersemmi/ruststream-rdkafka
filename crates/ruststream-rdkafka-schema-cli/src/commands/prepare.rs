//! `prepare [--check]`: the lock, refreshed from the registry (or, offline, from the migrations).

use std::fs;

use anyhow::{Result, bail};

use super::{Workspace, report};
use crate::catalog::Catalog;
use crate::lock::{Lock, LockMigration};
use crate::registry::Remote;
use crate::state::{drift, reconcile, stale};

pub(crate) async fn run(workspace: &Workspace, check: bool) -> Result<()> {
    let catalog = workspace.catalog()?;
    let previous = catalog.lock()?;
    let mut remotes = Vec::with_capacity(catalog.subjects.len());
    if let Some(registry) = &workspace.registry {
        for subject in &catalog.subjects {
            remotes.push(Some(registry.remote(subject).await?));
        }
    } else {
        remotes.resize(catalog.subjects.len(), None);
    }
    let fresh = refreshed(&catalog, &previous, &remotes)?;
    let path = catalog.lock_path();
    let rendered = fresh.render()?;
    if check {
        let current = fs::read_to_string(&path).unwrap_or_default();
        if current == rendered {
            println!("`{}` is up to date", path.display());
            return Ok(());
        }
        let mut lines = Vec::new();
        for subject in &fresh.subjects {
            lines.extend(stale(
                &subject.name,
                previous.subject(&subject.name),
                subject,
            ));
        }
        for gone in &previous.subjects {
            if fresh.subject(&gone.name).is_none() {
                lines.push(format!(
                    "the lock records subject `{}`, which is gone",
                    gone.name
                ));
            }
        }
        if lines.is_empty() {
            lines.push("the lock is not in the form `prepare` writes".to_owned());
        }
        lines.push("refresh it with `cargo kafka-schema prepare` and commit it".to_owned());
        return report(&lines, &format!("`{}` is stale", path.display()));
    }
    fresh.write(&path)?;
    println!("wrote `{}`", path.display());
    Ok(())
}

/// The lock the directory and the registry make, or an error on drift or on a changed
/// registered migration.
pub(crate) fn refreshed(
    catalog: &Catalog,
    previous: &Lock,
    remotes: &[Option<Remote>],
) -> Result<Lock> {
    let mut lines = Vec::new();
    let mut fresh = Lock::new();
    for (subject, remote) in catalog.subjects.iter().zip(remotes) {
        let recorded = previous.subject(&subject.name);
        if let Some(remote) = remote {
            lines.extend(drift(subject, recorded, remote));
        }
        *fresh.subject_mut(&subject.name) = reconcile(subject, recorded, remote.as_ref())?;
    }
    for recorded in &previous.subjects {
        if catalog.subject(&recorded.name).is_none()
            && recorded.migrations.iter().any(LockMigration::is_applied)
        {
            bail!(
                "the lock records registered migrations of subject `{}`, whose directory is gone \
                 from `{}`: restore it",
                recorded.name,
                catalog.root.display()
            );
        }
    }
    report(&lines, "drift between the migrations and the registry")?;
    Ok(fresh)
}
