//! `migrate add`: the next migration of a subject, from the crate's current schema.

use std::fs;

use anyhow::{Context, Result, bail};
use ruststream_rdkafka::schema_registry::admin::CompatibilityLevel;

use super::Workspace;
use crate::catalog::{self, Format, Migration, Owner, Settings, Subject};
use crate::export::Exports;
use crate::normalize::{normalize, pretty};
use crate::state::reconcile;
use crate::{avro, json, proto};

/// What a first migration says about its new subject.
pub(crate) struct NewSubject {
    pub(crate) record: Option<String>,
    pub(crate) format: Option<Format>,
    pub(crate) source: Option<String>,
    pub(crate) compatibility: Option<String>,
}

impl NewSubject {
    fn is_empty(&self) -> bool {
        self.record.is_none()
            && self.format.is_none()
            && self.source.is_none()
            && self.compatibility.is_none()
    }
}

pub(crate) fn run(
    workspace: &Workspace,
    name: &str,
    description: &str,
    new: NewSubject,
) -> Result<()> {
    catalog::check_description(description)?;
    let mut catalog = workspace.catalog()?;
    let mut exports = None;
    let subject = match catalog.subject(name) {
        Some(existing) => {
            if !new.is_empty() {
                bail!(
                    "subject `{name}` exists, and its `{}` holds its settings: edit it there \
                     instead of passing --record, --format, --source or --compatibility",
                    catalog::SUBJECT_FILE
                );
            }
            if !existing.is_owned() {
                bail!(
                    "subject `{name}` is external: another service registers it, and its versions \
                     arrive with `cargo kafka-schema migrate import {name}`"
                );
            }
            existing.clone()
        }
        None => new_subject(workspace, &catalog, name, new, &mut exports)?,
    };
    let content = current_schema(workspace, &subject, &mut exports)?;
    let format = subject.format();
    if let Some(newest) = subject.newest()
        && normalize(format, &newest.content)? == normalize(format, &content)?
    {
        println!(
            "subject `{name}`: the schema equals its newest migration `{}`; nothing to add",
            newest.file
        );
        return Ok(());
    }
    let file = subject.next_file(description);
    check_new(&catalog, &subject, &file, &content)?;

    let created = catalog.subject(name).is_none();
    if created {
        catalog::write_settings(&subject.dir, &subject.settings)?;
    }
    let path = subject.dir.join(&file);
    fs::write(&path, pretty(format, &content)?)
        .with_context(|| format!("writing `{}`", path.display()))?;

    catalog = workspace.catalog()?;
    let mut lock = catalog.lock()?;
    let written = catalog.subject(name).context("the subject just written")?;
    let entry = reconcile(written, lock.subject(name), None)?;
    *lock.subject_mut(name) = entry;
    lock.write(&catalog.lock_path())?;
    println!("wrote `{}` (pending)", path.display());
    Ok(())
}

/// The subject a first migration creates, from the flags and the export.
fn new_subject(
    workspace: &Workspace,
    catalog: &catalog::Catalog,
    name: &str,
    new: NewSubject,
    exports: &mut Option<Exports>,
) -> Result<Subject> {
    let level = new
        .compatibility
        .as_deref()
        .map(str::parse::<CompatibilityLevel>)
        .transpose()?;
    let format = match (new.format, &new.source) {
        (None | Some(Format::Protobuf), Some(_)) => Format::Protobuf,
        (Some(format), None) => format,
        (Some(format), Some(_)) => bail!(
            "--source names the `.proto` of a Protobuf subject, and --format says `{}`",
            format.name()
        ),
        (None, None) => {
            let record = new.record.as_deref().with_context(|| {
                format!(
                    "subject `{name}` is new: name the record it declares with `--record \
                     <full name>` (`com.acme.orders.Order`), or its `.proto` with `--source`"
                )
            })?;
            let exported = exports.insert(Exports::run(&workspace.krate)?);
            exported.format_of(record).with_context(|| {
                format!(
                    "no type in the crate exported record `{record}`: derive `Schema` on it \
                     (with `AvroSchema` or `JsonSchema`), or pass --format"
                )
            })?
        }
    };
    if format == Format::Protobuf && new.source.is_none() {
        bail!("a new Protobuf subject names its `.proto` with `--source <path>`");
    }
    let record = match (&new.record, format) {
        (Some(record), _) => record.clone(),
        (None, Format::Protobuf) => {
            let source = new.source.as_deref().unwrap_or_default();
            let text = read_source(workspace, name, source)?;
            proto::first_message(name, source, &text)?
        }
        (None, _) => bail!(
            "subject `{name}` is new: name the record it declares with `--record <full name>`"
        ),
    };
    Ok(Subject {
        name: name.to_owned(),
        dir: catalog.root.join(name),
        settings: Settings {
            format,
            compatibility: level.map(|level| level.as_api().to_owned()),
            owner: Owner::Service,
            record: Some(record),
            source: new.source,
            references: Vec::new(),
        },
        level,
        migrations: Vec::new(),
    })
}

/// The schema the crate has now: the exported one for Avro and JSON Schema, the source for
/// Protobuf.
fn current_schema(
    workspace: &Workspace,
    subject: &Subject,
    exports: &mut Option<Exports>,
) -> Result<String> {
    let format = subject.format();
    if format == Format::Protobuf {
        let source = subject.settings.source.as_deref().with_context(|| {
            format!(
                "Protobuf subject `{}` names no `source` in its `{}`",
                subject.name,
                catalog::SUBJECT_FILE
            )
        })?;
        return read_source(workspace, &subject.name, source);
    }
    let record = subject.settings.record.as_deref().with_context(|| {
        format!(
            "subject `{}` names no `record` in its `{}`: add `record = \"<full name>\"`",
            subject.name,
            catalog::SUBJECT_FILE
        )
    })?;
    let exported = match exports {
        Some(exported) => exported,
        None => exports.insert(Exports::run(&workspace.krate)?),
    };
    exported.schema(record, format)?.with_context(|| {
        format!(
            "no type in the crate exported record `{record}` of subject `{}` as {}: derive \
             `Schema` on it",
            subject.name,
            format.name()
        )
    })
}

fn read_source(workspace: &Workspace, subject: &str, source: &str) -> Result<String> {
    let path = workspace.manifest_dir.join(source);
    fs::read_to_string(&path).with_context(|| {
        format!(
            "subject `{subject}` compiles `{source}`, which cannot be read (`{}`)",
            path.display()
        )
    })
}

/// The local checks of a new migration against the subject's earlier ones.
pub(crate) fn check_new(
    catalog: &catalog::Catalog,
    subject: &Subject,
    file: &str,
    content: &str,
) -> Result<()> {
    check_line(catalog, subject, file, content, &subject.migrations)?;
    if subject.format() == Format::Json && subject.is_owned() {
        json::check_content_model(&subject.name, file, subject.effective_level(), content)?;
    }
    Ok(())
}

/// The checks every migration of the line passes against the ones before it: it parses (and a
/// JSON Schema validates under its metaschema), and for an owned subject Avro compatibility and
/// the Protobuf `reserved` rule hold.
pub(crate) fn check_line(
    catalog: &catalog::Catalog,
    subject: &Subject,
    file: &str,
    content: &str,
    earlier: &[Migration],
) -> Result<()> {
    let owned = subject.is_owned();
    match subject.format() {
        Format::Avro if owned => avro::check(catalog, subject, file, content, earlier),
        Format::Avro => avro::parse(catalog, subject, file, content).map(drop),
        Format::Json => json::check_metaschema(&subject.name, file, content),
        Format::Protobuf if owned => proto::check(&subject.name, file, content, earlier),
        Format::Protobuf => proto::parse_check(&subject.name, file, content),
    }
}
