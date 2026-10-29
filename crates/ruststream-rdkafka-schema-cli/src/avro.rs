//! The local Avro compatibility check: a new migration against the earlier ones, under the
//! subject's level, with `apache-avro`'s schema resolution rules. It needs no registry.

use anyhow::{Context, Result, bail};
use apache_avro::Schema;
use apache_avro::error::CompatibilityError;
use apache_avro::schema_compatibility::{Compatibility, SchemaCompatibility};

use crate::catalog::{Catalog, Migration, Subject};

/// Checks `candidate` (the text of migration `file`) against `earlier`, oldest first, under the
/// subject's effective level.
pub(crate) fn check(
    catalog: &Catalog,
    subject: &Subject,
    file: &str,
    candidate: &str,
    earlier: &[Migration],
) -> Result<()> {
    let level = subject.effective_level();
    let against: &[Migration] = if level.is_transitive() {
        earlier
    } else {
        earlier.last().map_or(&[], std::slice::from_ref)
    };
    if against.is_empty() || !(level.checks_backward() || level.checks_forward()) {
        // A first migration has nothing to stand against, but it still has to parse.
        parse(catalog, subject, file, candidate)?;
        return Ok(());
    }
    let new = parse(catalog, subject, file, candidate)?;
    for old_migration in against {
        let old = parse(
            catalog,
            subject,
            &old_migration.file,
            &old_migration.content,
        )?;
        if level.checks_backward() {
            verdict(SchemaCompatibility::can_read(&old, &new)).with_context(|| {
                format!(
                    "migration `{file}` of subject `{}` breaks {level}: a reader of it cannot \
                     read what `{}` wrote",
                    subject.name, old_migration.file
                )
            })?;
        }
        if level.checks_forward() {
            verdict(SchemaCompatibility::can_read(&new, &old)).with_context(|| {
                format!(
                    "migration `{file}` of subject `{}` breaks {level}: a reader of `{}` cannot \
                     read what it writes",
                    subject.name, old_migration.file
                )
            })?;
        }
    }
    Ok(())
}

fn verdict(result: Result<Compatibility, CompatibilityError>) -> Result<()> {
    match result {
        Ok(Compatibility::Full) => Ok(()),
        // A partial resolution fails on some data (a union branch or an enum symbol the reader
        // lacks), and the registry refuses it.
        Ok(Compatibility::Partial) => {
            bail!("resolution fails for some values: a union branch or an enum symbol is missing")
        }
        Err(err) => bail!("{err}"),
    }
}

/// Parses a migration, with the newest migration of every subject it references in scope.
pub(crate) fn parse(
    catalog: &Catalog,
    subject: &Subject,
    file: &str,
    text: &str,
) -> Result<Schema> {
    let mut referenced = Vec::new();
    for reference in &subject.settings.references {
        let Some(newest) = catalog
            .subject(&reference.subject)
            .and_then(Subject::newest)
        else {
            bail!(
                "subject `{}` references subject `{}`, which has no migration in `{}`",
                subject.name,
                reference.subject,
                catalog.root.display()
            );
        };
        referenced.push(newest.content.as_str());
    }
    let parsed = if referenced.is_empty() {
        Schema::parse_str(text).map_err(anyhow::Error::from)
    } else {
        Schema::parse_str_with_list(text, referenced)
            .map(|(schema, _)| schema)
            .map_err(anyhow::Error::from)
    };
    parsed.with_context(|| {
        format!(
            "migration `{file}` of subject `{}` is not an Avro schema",
            subject.name
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testdir::TestDir;

    const V1: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"}]}"#;
    const V2_DEFAULT: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"},{"name":"note","type":["null","string"],"default":null}]}"#;
    const V2_REQUIRED: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"},{"name":"qty","type":"int"}]}"#;
    const V3_NO_ID: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"note","type":["null","string"],"default":null}]}"#;

    fn catalog(level: &str, migrations: &[&str]) -> (TestDir, Catalog) {
        let dir = TestDir::new();
        dir.write(
            "schemas/orders-value/subject.toml",
            &format!("format = \"avro\"\ncompatibility = \"{level}\"\n"),
        );
        for (number, text) in (1..).zip(migrations) {
            dir.write(&format!("schemas/orders-value/{number:04}_m.avsc"), text);
        }
        let catalog = Catalog::read(&dir.path("schemas")).expect("catalog");
        (dir, catalog)
    }

    fn run(level: &str, earlier: &[&str], candidate: &str) -> Result<()> {
        let (_dir, catalog) = catalog(level, earlier);
        let subject = &catalog.subjects[0];
        check(
            &catalog,
            subject,
            "next.avsc",
            candidate,
            &subject.migrations,
        )
    }

    #[test]
    fn backward_accepts_a_field_with_a_default_and_refuses_one_without() {
        run("BACKWARD", &[V1], V2_DEFAULT).expect("defaulted field");
        let err = format!(
            "{:#}",
            run("BACKWARD", &[V1], V2_REQUIRED).expect_err("no default")
        );
        assert!(
            err.starts_with(
                "migration `next.avsc` of subject `orders-value` breaks BACKWARD: a reader of it \
                 cannot read what `0001_m.avsc` wrote"
            ),
            "{err}"
        );
        assert!(err.contains("qty"), "{err}");
    }

    #[test]
    fn forward_refuses_removing_a_field_old_readers_need() {
        let err = format!(
            "{:#}",
            run("FORWARD", &[V2_DEFAULT], V3_NO_ID).expect_err("id gone")
        );
        assert!(
            err.contains("breaks FORWARD: a reader of `0001_m.avsc` cannot read"),
            "{err}"
        );
        run("BACKWARD", &[V2_DEFAULT], V3_NO_ID).expect("a reader may drop a field");
    }

    #[test]
    fn transitive_levels_check_every_earlier_migration() {
        // 0002 gave `qty` a default; the candidate drops the default. Data 0002 wrote carries
        // `qty`, data 0001 wrote does not.
        let with_default = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"},{"name":"qty","type":"int","default":0}]}"#;
        run("BACKWARD", &[V1, with_default], V2_REQUIRED).expect("the latest carries qty");
        let err = format!(
            "{:#}",
            run("BACKWARD_TRANSITIVE", &[V1, with_default], V2_REQUIRED)
                .expect_err("0001 lacks qty")
        );
        assert!(
            err.contains("cannot read what `0001_m.avsc` wrote"),
            "{err}"
        );
    }

    #[test]
    fn none_checks_nothing_but_the_schema_must_parse() {
        run("NONE", &[V1], V2_REQUIRED).expect("NONE");
        let err = format!(
            "{:#}",
            run("NONE", &[V1], "{\"type\":\"recrd\"}").expect_err("parse")
        );
        assert!(err.contains("is not an Avro schema"), "{err}");
    }

    #[test]
    fn a_union_missing_a_writer_branch_is_refused() {
        let wide =
            r#"{"type":"record","name":"R","fields":[{"name":"v","type":["int","string"]}]}"#;
        let narrow = r#"{"type":"record","name":"R","fields":[{"name":"v","type":["int"]}]}"#;
        let err = format!(
            "{:#}",
            run("BACKWARD", &[wide], narrow).expect_err("partial")
        );
        assert!(err.contains("breaks BACKWARD"), "{err}");
    }
}
