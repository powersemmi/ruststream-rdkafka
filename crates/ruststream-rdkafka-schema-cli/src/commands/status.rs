//! `migrate status`: per subject, its migrations, registry versions, pending migrations and
//! drift.

use anyhow::Result;

use super::{Workspace, report};
use crate::state::{drift, reconcile, stale};

pub(crate) async fn run(workspace: &Workspace) -> Result<()> {
    let catalog = workspace.catalog()?;
    let lock = catalog.lock()?;
    match &workspace.registry {
        Some(registry) => println!("registry: {}", registry.shown()),
        None => println!("registry: none (offline: the lock's record)"),
    }
    let mut problems = Vec::new();
    for subject in &catalog.subjects {
        let recorded = lock.subject(&subject.name);
        let remote = match &workspace.registry {
            Some(registry) => Some(registry.remote(subject).await?),
            None => None,
        };
        let level = subject.level.map_or_else(
            || "level not declared".to_owned(),
            |level| level.to_string(),
        );
        let owner = if subject.is_owned() {
            "owned"
        } else {
            "external"
        };
        println!();
        println!(
            "{} ({}, {level}, {owner})",
            subject.name,
            subject.format().name()
        );
        let expected = match reconcile(subject, recorded, remote.as_ref()) {
            Ok(expected) => expected,
            Err(err) => {
                problems.push(format!("{err:#}"));
                continue;
            }
        };
        for migration in &expected.migrations {
            let state = match (migration.version, migration.id) {
                (Some(version), Some(id)) => format!("version {version}, id {id}"),
                _ => "pending".to_owned(),
            };
            println!("  {}  {state}", migration.file);
        }
        println!("  registry versions: {:?}", expected.registry_versions);
        if let Some(remote) = &remote {
            problems.extend(drift(subject, recorded, remote));
        }
        let lines = stale(&subject.name, recorded, &expected);
        if !lines.is_empty() {
            problems.extend(lines);
            problems.push("refresh the lock with `cargo kafka-schema prepare`".to_owned());
        }
    }
    report(&problems, "status")
}
