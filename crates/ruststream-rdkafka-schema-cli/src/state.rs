//! The rules between the directory, the lock and the registry: what the lock records, what is
//! immutable, and what counts as drift.

use anyhow::{Result, bail};

use crate::catalog::Subject;
use crate::lock::{LockMigration, LockSubject};
use crate::normalize::checksum;
use crate::registry::Remote;

/// The lock entry of `subject`: every migration's checksum, and its registry version and id from
/// `remote` when the registry was asked, or as `previous` recorded them otherwise.
///
/// # Errors
///
/// A registered migration whose content changed, or that was deleted: an applied migration is
/// immutable, because a registry version cannot change.
pub(crate) fn reconcile(
    subject: &Subject,
    previous: Option<&LockSubject>,
    remote: Option<&Remote>,
) -> Result<LockSubject> {
    let mut migrations = Vec::with_capacity(subject.migrations.len());
    for (index, migration) in subject.migrations.iter().enumerate() {
        let sha256 = checksum(subject.format(), &migration.content).map_err(|err| {
            err.context(format!(
                "migration `{}` of subject `{}`",
                migration.file, subject.name
            ))
        })?;
        let recorded = previous.and_then(|lock| lock.migration(&migration.file));
        if let Some(recorded) = recorded
            && recorded.is_applied()
            && recorded.sha256 != sha256
        {
            bail!(
                "migration `{}` of subject `{}` changed after it was registered (the lock records \
                 another checksum); an applied migration is immutable: restore it, and describe \
                 the change with a new migration",
                migration.file,
                subject.name
            );
        }
        let (version, id) = remote.map_or_else(
            || recorded.map_or((None, None), |recorded| (recorded.version, recorded.id)),
            |remote| {
                remote.found[index]
                    .map_or((None, None), |found| (Some(found.version), Some(found.id)))
            },
        );
        migrations.push(LockMigration {
            file: migration.file.clone(),
            sha256,
            version,
            id,
        });
    }
    if let Some(previous) = previous {
        for recorded in &previous.migrations {
            let present = subject.migrations.iter().any(|m| m.file == recorded.file);
            if !present && recorded.is_applied() {
                bail!(
                    "migration `{}` of subject `{}` is registered (version {}) and missing from \
                     `{}`; an applied migration is never deleted: restore it",
                    recorded.file,
                    subject.name,
                    recorded.version.unwrap_or_default(),
                    subject.dir.display()
                );
            }
        }
    }
    let registry_versions = remote.map_or_else(
        || {
            previous
                .map(|lock| lock.registry_versions.clone())
                .unwrap_or_default()
        },
        |remote| remote.versions.clone(),
    );
    Ok(LockSubject {
        name: subject.name.clone(),
        registry_versions,
        migrations,
    })
}

/// What differs between the lock entry `recorded` and the one `expected` from the directory, one
/// line per migration.
pub(crate) fn stale(
    subject: &str,
    recorded: Option<&LockSubject>,
    expected: &LockSubject,
) -> Vec<String> {
    let mut lines = Vec::new();
    for migration in &expected.migrations {
        match recorded.and_then(|lock| lock.migration(&migration.file)) {
            None => lines.push(format!(
                "migration `{}` of subject `{subject}` is not in the lock",
                migration.file
            )),
            Some(entry) if entry.sha256 != migration.sha256 => lines.push(format!(
                "migration `{}` of subject `{subject}` differs from the checksum the lock records",
                migration.file
            )),
            Some(entry) if entry.version != migration.version || entry.id != migration.id => {
                lines.push(format!(
                    "migration `{}` of subject `{subject}` is recorded as {}, and the registry \
                     holds it as {}",
                    migration.file,
                    describe(entry),
                    describe(migration)
                ));
            }
            Some(_) => {}
        }
    }
    if let Some(recorded) = recorded {
        for entry in &recorded.migrations {
            if expected.migration(&entry.file).is_none() {
                lines.push(format!(
                    "the lock records migration `{}` of subject `{subject}`, which is gone",
                    entry.file
                ));
            }
        }
        if recorded.registry_versions != expected.registry_versions {
            lines.push(format!(
                "subject `{subject}` is recorded with registry versions {:?}, and the registry \
                 holds {:?}",
                recorded.registry_versions, expected.registry_versions
            ));
        }
    } else if !expected.registry_versions.is_empty() {
        lines.push(format!("subject `{subject}` is not in the lock"));
    }
    lines
}

fn describe(entry: &LockMigration) -> String {
    match (entry.version, entry.id) {
        (Some(version), Some(id)) => format!("version {version} (id {id})"),
        _ => "pending".to_owned(),
    }
}

/// The drift between the directory and the registry: versions no migration describes,
/// registered migrations the registry lost, and versions in another order than the migrations.
pub(crate) fn drift(
    subject: &Subject,
    recorded: Option<&LockSubject>,
    remote: &Remote,
) -> Vec<String> {
    let name = &subject.name;
    let mut lines = Vec::new();
    for version in &remote.versions {
        let described = remote
            .found
            .iter()
            .flatten()
            .any(|found| found.version == *version);
        if !described {
            lines.push(format!(
                "the registry holds version {version} of subject `{name}`, which no migration \
                 describes"
            ));
        }
    }
    for (migration, found) in subject.migrations.iter().zip(&remote.found) {
        let entry = recorded.and_then(|lock| lock.migration(&migration.file));
        if let Some(entry) = entry
            && found.is_none()
            && let Some(version) = entry.version
        {
            lines.push(format!(
                "migration `{}` of subject `{name}` is recorded as version {version}, and the \
                 registry no longer holds it",
                migration.file
            ));
        }
    }
    let mut last: Option<(&str, u32)> = None;
    let mut pending: Option<&str> = None;
    for (migration, found) in subject.migrations.iter().zip(&remote.found) {
        match found {
            None => {
                pending.get_or_insert(migration.file.as_str());
            }
            Some(found) => {
                if let Some(pending) = pending {
                    lines.push(format!(
                        "migration `{}` of subject `{name}` is registered (version {}) while the \
                         earlier `{pending}` is not",
                        migration.file, found.version
                    ));
                } else if let Some((previous, version)) = last
                    && found.version <= version
                {
                    lines.push(format!(
                        "the registry orders subject `{name}` differently from its migrations: \
                         `{previous}` is version {version} and `{}` is version {}",
                        migration.file, found.version
                    ));
                }
                last = Some((migration.file.as_str(), found.version));
            }
        }
    }
    if !lines.is_empty() {
        lines.push(format!(
            "fix it with `cargo kafka-schema migrate import {name}` or a registry cleanup; the \
             tool never overwrites the registry"
        ));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::registry::Found;
    use crate::testdir::TestDir;

    const V1: &str = r#"{"type":"record","name":"O","fields":[{"name":"id","type":"long"}]}"#;
    const V2: &str = r#"{"type":"record","name":"O","fields":[{"name":"id","type":"long"},{"name":"n","type":"int","default":0}]}"#;

    fn subject(migrations: &[&str]) -> (TestDir, Subject) {
        let dir = TestDir::new();
        dir.write("schemas/o-value/subject.toml", "format = \"avro\"\n");
        for (number, text) in (1..).zip(migrations) {
            dir.write(&format!("schemas/o-value/{number:04}_m.avsc"), text);
        }
        let catalog = Catalog::read(&dir.path("schemas")).expect("catalog");
        let subject = catalog.subjects.into_iter().next().expect("subject");
        (dir, subject)
    }

    const NONE: [String; 0] = [];

    // An `Option` because the registry answer it stands for is one.
    #[allow(clippy::unnecessary_wraps)]
    fn found(version: u32) -> Option<Found> {
        Some(Found {
            version,
            id: 100 + version,
        })
    }

    fn remote(versions: &[u32], found: &[Option<Found>]) -> Remote {
        Remote {
            versions: versions.to_vec(),
            found: found.to_vec(),
        }
    }

    #[test]
    fn online_the_lock_takes_the_registry_versions_and_ids() {
        let (_dir, subject) = subject(&[V1, V2]);
        let lock = reconcile(&subject, None, Some(&remote(&[1], &[found(1), None]))).expect("ok");
        assert_eq!(lock.registry_versions, [1]);
        assert_eq!(
            (lock.migrations[0].version, lock.migrations[0].id),
            (Some(1), Some(101))
        );
        assert_eq!(
            (lock.migrations[1].version, lock.migrations[1].id),
            (None, None)
        );
        assert_eq!(lock.migrations[0].sha256.len(), 64);
    }

    #[test]
    fn offline_the_lock_keeps_what_it_recorded() {
        let (_dir, subject) = subject(&[V1, V2]);
        let online = reconcile(&subject, None, Some(&remote(&[1], &[found(1), None]))).expect("on");
        let offline = reconcile(&subject, Some(&online), None).expect("off");
        assert_eq!(offline, online);
    }

    #[test]
    fn a_registered_migration_is_immutable() {
        let (dir, subject) = subject(&[V1]);
        let lock = reconcile(&subject, None, Some(&remote(&[1], &[found(1)]))).expect("ok");
        dir.write("schemas/o-value/0001_m.avsc", V2);
        let changed = Catalog::read(&dir.path("schemas"))
            .expect("catalog")
            .subjects
            .remove(0);
        let err = reconcile(&changed, Some(&lock), None)
            .expect_err("changed")
            .to_string();
        assert!(
            err.starts_with(
                "migration `0001_m.avsc` of subject `o-value` changed after it was registered"
            ),
            "{err}"
        );

        // Formatting is not a change: the checksum reads the normalized form.
        dir.write("schemas/o-value/0001_m.avsc", &format!("{V1}\r\n"));
        let reformatted = Catalog::read(&dir.path("schemas"))
            .expect("catalog")
            .subjects
            .remove(0);
        reconcile(&reformatted, Some(&lock), None).expect("same content");

        std::fs::remove_file(dir.path("schemas/o-value/0001_m.avsc")).expect("rm");
        let deleted = Catalog::read(&dir.path("schemas"))
            .expect("catalog")
            .subjects
            .remove(0);
        let err = reconcile(&deleted, Some(&lock), None)
            .expect_err("deleted")
            .to_string();
        assert!(
            err.contains("is registered (version 1) and missing from"),
            "{err}"
        );
    }

    #[test]
    fn a_pending_migration_may_change_and_the_lock_follows() {
        let (dir, subject) = subject(&[V1]);
        let lock = reconcile(&subject, None, None).expect("pending");
        dir.write("schemas/o-value/0001_m.avsc", V2);
        let changed = Catalog::read(&dir.path("schemas"))
            .expect("catalog")
            .subjects
            .remove(0);
        let next = reconcile(&changed, Some(&lock), None).expect("pending may change");
        let lines = stale("o-value", Some(&lock), &next);
        assert_eq!(
            lines,
            [
                "migration `0001_m.avsc` of subject `o-value` differs from the checksum the lock records"
            ]
        );
    }

    #[test]
    fn stale_names_each_difference() {
        let (_dir, subject) = subject(&[V1, V2]);
        let expected = reconcile(
            &subject,
            None,
            Some(&remote(&[1, 2], &[found(1), found(2)])),
        )
        .expect("ok");
        let mut recorded = expected.clone();
        recorded.migrations.pop();
        recorded.migrations[0].version = None;
        recorded.migrations[0].id = None;
        recorded.registry_versions = vec![1];
        assert_eq!(
            stale("o-value", Some(&recorded), &expected),
            [
                "migration `0001_m.avsc` of subject `o-value` is recorded as pending, and the registry holds it as version 1 (id 101)",
                "migration `0002_m.avsc` of subject `o-value` is not in the lock",
                "subject `o-value` is recorded with registry versions [1], and the registry holds [1, 2]",
            ]
        );
        assert_eq!(stale("o-value", Some(&expected), &expected), NONE);
    }

    #[test]
    fn drift_finds_its_three_cases() {
        let (_dir, subject) = subject(&[V1, V2]);
        let clean = drift(&subject, None, &remote(&[1, 2], &[found(1), found(2)]));
        assert_eq!(clean, NONE);
        let pending = drift(&subject, None, &remote(&[1], &[found(1), None]));
        assert_eq!(pending, NONE);
        let gaps = drift(&subject, None, &remote(&[3, 7], &[found(3), found(7)]));
        assert_eq!(gaps, NONE);

        // A version no migration describes.
        let lines = drift(&subject, None, &remote(&[1, 2, 3], &[found(1), found(2)]));
        assert_eq!(
            lines[0],
            "the registry holds version 3 of subject `o-value`, which no migration describes"
        );
        assert!(
            lines
                .last()
                .expect("fix")
                .contains("migrate import o-value")
        );

        // A registered migration the registry lost.
        let recorded = reconcile(
            &subject,
            None,
            Some(&remote(&[1, 2], &[found(1), found(2)])),
        )
        .expect("ok");
        let lines = drift(&subject, Some(&recorded), &remote(&[1], &[found(1), None]));
        assert_eq!(
            lines[0],
            "migration `0002_m.avsc` of subject `o-value` is recorded as version 2, and the registry no longer holds it"
        );

        // Versions in another order than the migrations.
        let lines = drift(&subject, None, &remote(&[1, 2], &[found(2), found(1)]));
        assert_eq!(
            lines[0],
            "the registry orders subject `o-value` differently from its migrations: `0001_m.avsc` is version 2 and `0002_m.avsc` is version 1"
        );
        let lines = drift(&subject, None, &remote(&[1], &[None, found(1)]));
        assert_eq!(
            lines[0],
            "migration `0002_m.avsc` of subject `o-value` is registered (version 1) while the earlier `0001_m.avsc` is not"
        );
    }
}
