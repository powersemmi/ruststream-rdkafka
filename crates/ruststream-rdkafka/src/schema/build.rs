//! The build-script check of a Protobuf subject: the `.proto` that `prost-build` compiles must
//! equal the subject's newest migration.
//!
//! `prost-build` generates the message types from the `.proto`, so a type equals its source by
//! construction. What can drift is the source against the migrations: an edited `.proto` with no
//! migration describing it would publish a schema the registry has never seen.

use std::env;
use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// The settings file of a subject directory.
const SUBJECT_FILE: &str = "subject.toml";

/// The variable that moves the migrations directory away from `schemas/`.
const SCHEMA_DIR_VAR: &str = "RUSTSTREAM_SCHEMA_DIR";

/// Why a subject's `.proto` failed the build-script check. Its `Display` and `Debug` both print
/// the whole message, because a build script that returns it prints the `Debug` form.
pub struct CheckProtoError {
    message: String,
}

impl CheckProtoError {
    fn new(message: String) -> Self {
        Self { message }
    }
}

impl fmt::Display for CheckProtoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl fmt::Debug for CheckProtoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CheckProtoError {}

/// The fields of `subject.toml` this check reads; the rest are accepted as written.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)] // The fields the check does not read still validate the file's shape.
struct Settings {
    format: String,
    compatibility: Option<String>,
    owner: Option<String>,
    record: Option<String>,
    source: Option<String>,
    #[serde(default)]
    references: Vec<Reference>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct Reference {
    name: String,
    subject: String,
}

/// Checks, from a build script, that the `.proto` a Protobuf subject compiles equals the
/// subject's newest migration.
///
/// It reads `schemas/<subject>/subject.toml` beside the crate's `Cargo.toml` (or under
/// `RUSTSTREAM_SCHEMA_DIR`, absolute or relative to the crate), takes the `source` it names, and
/// compares that file with the highest-numbered migration of the subject, line endings
/// normalized. It prints the `cargo:rerun-if-changed` lines for the source and the subject's
/// directory, so the check runs again whenever either changes.
///
/// Call it next to `prost-build`, which compiles the same source.
///
/// # Errors
///
/// Returns a [`CheckProtoError`] naming the subject, the source and the migration when the
/// source differs from the newest migration, and an error naming what is missing when the
/// subject directory, its `subject.toml`, its `source`, or its migrations cannot be read, or the
/// subject's format is not `protobuf`.
///
/// # Examples
///
/// The build script of a service whose `payments-value` subject is compiled from
/// `proto/payments.proto`:
///
/// ```no_run
/// # mod prost_build {
/// #     pub fn compile_protos(_: &[&str], _: &[&str]) -> std::io::Result<()> { Ok(()) }
/// # }
/// // build.rs
/// use ruststream_rdkafka::schema::build::check_proto;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     prost_build::compile_protos(&["proto/payments.proto"], &["proto"])?;
///     // Fails the build when `proto/payments.proto` is not the subject's newest migration.
///     check_proto("payments-value")?;
///     Ok(())
/// }
/// ```
///
/// with `schemas/payments-value/subject.toml`:
///
/// ```toml
/// format = "protobuf"
/// compatibility = "BACKWARD_TRANSITIVE"
/// record = "acme.payments.Payment"
/// source = "proto/payments.proto"
/// ```
pub fn check_proto(subject: &str) -> Result<(), CheckProtoError> {
    let manifest = env::var_os("CARGO_MANIFEST_DIR").map_or_else(PathBuf::new, PathBuf::from);
    check_in(
        &manifest,
        env::var_os(SCHEMA_DIR_VAR),
        subject,
        &mut io::stdout().lock(),
    )
}

fn check_in(
    manifest: &Path,
    schema_dir: Option<OsString>,
    subject: &str,
    cargo: &mut impl Write,
) -> Result<(), CheckProtoError> {
    let root = schema_dir.map_or_else(|| manifest.join("schemas"), |dir| manifest.join(dir));
    let dir = root.join(subject);
    // The lines go out before anything can fail, so a fix to any of these files reruns it.
    let _ = writeln!(cargo, "cargo:rerun-if-env-changed={SCHEMA_DIR_VAR}");
    let _ = writeln!(cargo, "cargo:rerun-if-changed={}", dir.display());
    let settings = read_settings(&dir, subject)?;
    if settings.format != "protobuf" {
        return Err(CheckProtoError::new(format!(
            "subject `{subject}` is a `{format}` subject, and `check_proto` checks Protobuf \
             subjects; its `{SUBJECT_FILE}` says `format = \"{format}\"`",
            format = settings.format,
        )));
    }
    let source = settings.source.ok_or_else(|| {
        CheckProtoError::new(format!(
            "subject `{subject}` names no `source` in `{}`: add \
             `source = \"<path to the .proto, relative to the crate>\"`",
            dir.join(SUBJECT_FILE).display(),
        ))
    })?;
    let source_path = manifest.join(&source);
    let _ = writeln!(cargo, "cargo:rerun-if-changed={}", source_path.display());
    let text = fs::read_to_string(&source_path).map_err(|err| {
        CheckProtoError::new(format!(
            "subject `{subject}` compiles `{source}`, which cannot be read: {err}"
        ))
    })?;
    let (migration, migration_text) = newest_migration(&dir, subject)?;
    let compiled = normalize(&text);
    let recorded = normalize(&migration_text);
    if compiled == recorded {
        return Ok(());
    }
    let line = first_difference(&compiled, &recorded);
    Err(CheckProtoError::new(format!(
        "`{source}` differs from `{migration}`, the newest migration of subject `{subject}` \
         (first difference at line {line}); describe the change with a migration \
         (`cargo kafka-schema migrate add {subject} <description>`) or restore the source",
    )))
}

fn read_settings(dir: &Path, subject: &str) -> Result<Settings, CheckProtoError> {
    let path = dir.join(SUBJECT_FILE);
    let text = fs::read_to_string(&path).map_err(|err| {
        CheckProtoError::new(format!(
            "subject `{subject}` has no readable `{}`: {err}; create the subject with \
             `cargo kafka-schema migrate add {subject} <description>`",
            path.display(),
        ))
    })?;
    toml::from_str(&text).map_err(|err| {
        CheckProtoError::new(format!(
            "`{}` of subject `{subject}` does not parse: {err}",
            path.display()
        ))
    })
}

/// The highest-numbered `NNNN[_description].proto` of the subject, by file name and content.
fn newest_migration(dir: &Path, subject: &str) -> Result<(String, String), CheckProtoError> {
    let unreadable =
        |err: io::Error| CheckProtoError::new(format!("reading `{}`: {err}", dir.display()));
    let mut newest: Option<(u32, PathBuf)> = None;
    for entry in fs::read_dir(dir).map_err(unreadable)? {
        let path = entry.map_err(unreadable)?.path();
        if path.extension().is_none_or(|ext| ext != "proto") {
            continue;
        }
        let Some(number) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.get(..4))
            .and_then(|digits| digits.parse::<u32>().ok())
        else {
            continue;
        };
        if newest.as_ref().is_none_or(|(highest, _)| number > *highest) {
            newest = Some((number, path));
        }
    }
    let Some((_, path)) = newest else {
        return Err(CheckProtoError::new(format!(
            "subject `{subject}` has no migration yet; snapshot its source with \
             `cargo kafka-schema migrate add {subject} <description>`"
        )));
    };
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let text = fs::read_to_string(&path)
        .map_err(|err| CheckProtoError::new(format!("reading `{}`: {err}", path.display())))?;
    Ok((name, text))
}

/// A `.proto` is compared as text with its line endings normalized, the way the migrations tool
/// checksums it.
fn normalize(text: &str) -> String {
    text.replace("\r\n", "\n")
}

fn first_difference(compiled: &str, recorded: &str) -> usize {
    let mut compiled_lines = compiled.lines();
    let mut recorded_lines = recorded.lines();
    let mut line = 1;
    loop {
        match (compiled_lines.next(), recorded_lines.next()) {
            (Some(a), Some(b)) if a == b => line += 1,
            _ => return line,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    const PAYMENT_V1: &str =
        "syntax = \"proto3\";\npackage acme.payments;\n\nmessage Payment {\n  int64 id = 1;\n}\n";
    const PAYMENT_V2: &str = "syntax = \"proto3\";\npackage acme.payments;\n\nmessage Payment {\n  int64 id = 1;\n  string refund = 2;\n}\n";

    /// A crate directory of its own under the system temp dir, removed when dropped.
    struct Crate(PathBuf);

    impl Crate {
        fn new() -> Self {
            static NEXT: AtomicU32 = AtomicU32::new(0);
            let dir = env::temp_dir().join(format!(
                "ruststream-check-proto-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&dir).expect("crate dir");
            Self(dir)
        }

        fn write(&self, relative: &str, text: &str) -> &Self {
            let path = self.0.join(relative);
            fs::create_dir_all(path.parent().expect("parent")).expect("dirs");
            fs::write(path, text).expect("write");
            self
        }

        fn payments(&self, source: &str, migrations: &[(&str, &str)]) -> &Self {
            self.write(
                "schemas/payments-value/subject.toml",
                "format = \"protobuf\"\nrecord = \"acme.payments.Payment\"\n\
                 source = \"proto/payments.proto\"\n",
            );
            self.write("proto/payments.proto", source);
            for (name, text) in migrations {
                self.write(&format!("schemas/payments-value/{name}"), text);
            }
            self
        }

        fn check(&self, schema_dir: Option<&str>) -> (Result<(), CheckProtoError>, String) {
            let mut cargo = Vec::new();
            let result = check_in(
                &self.0,
                schema_dir.map(OsString::from),
                "payments-value",
                &mut cargo,
            );
            (result, String::from_utf8(cargo).expect("utf-8"))
        }
    }

    impl Drop for Crate {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn a_source_equal_to_the_newest_migration_passes_and_asks_for_reruns() {
        let krate = Crate::new();
        krate.payments(
            PAYMENT_V2,
            &[
                ("0001_init.proto", PAYMENT_V1),
                ("0002_add_refund.proto", PAYMENT_V2),
            ],
        );
        let (result, cargo) = krate.check(None);
        result.expect("equal to 0002");
        assert!(
            cargo.contains("cargo:rerun-if-env-changed=RUSTSTREAM_SCHEMA_DIR"),
            "{cargo}"
        );
        let subject_dir = krate.0.join("schemas/payments-value");
        assert!(
            cargo.contains(&format!("cargo:rerun-if-changed={}", subject_dir.display())),
            "{cargo}"
        );
        let source = krate.0.join("proto/payments.proto");
        assert!(
            cargo.contains(&format!("cargo:rerun-if-changed={}", source.display())),
            "{cargo}"
        );
    }

    #[test]
    fn a_source_ahead_of_its_migrations_fails_naming_subject_file_and_migration() {
        let krate = Crate::new();
        krate.payments(PAYMENT_V2, &[("0001_init.proto", PAYMENT_V1)]);
        let (result, _) = krate.check(None);
        let message = result.expect_err("0001 lacks the refund").to_string();
        assert_eq!(
            message,
            "`proto/payments.proto` differs from `0001_init.proto`, the newest migration of \
             subject `payments-value` (first difference at line 6); describe the change with a \
             migration (`cargo kafka-schema migrate add payments-value <description>`) or \
             restore the source"
        );
    }

    #[test]
    fn the_newest_migration_is_the_highest_number_not_the_last_listed() {
        let krate = Crate::new();
        krate.payments(
            PAYMENT_V1,
            &[
                ("0002_add_refund.proto", PAYMENT_V2),
                ("0010.proto", PAYMENT_V1),
            ],
        );
        krate
            .check(None)
            .0
            .expect("0010 is the newest and equals the source");
    }

    #[test]
    fn line_endings_do_not_count() {
        let krate = Crate::new();
        krate.payments(
            &PAYMENT_V1.replace('\n', "\r\n"),
            &[("0001_init.proto", PAYMENT_V1)],
        );
        krate.check(None).0.expect("CRLF equals LF");
    }

    #[test]
    fn the_schema_dir_variable_moves_the_migrations() {
        let krate = Crate::new();
        krate.write(
            "registry/payments-value/subject.toml",
            "format = \"protobuf\"\nsource = \"proto/payments.proto\"\n",
        );
        krate.write("registry/payments-value/0001_init.proto", PAYMENT_V1);
        krate.write("proto/payments.proto", PAYMENT_V1);
        krate
            .check(Some("registry"))
            .0
            .expect("relative to the crate");
        let absolute = krate.0.join("registry");
        krate.check(absolute.to_str()).0.expect("absolute");
        assert!(krate.check(None).0.is_err(), "schemas/ holds nothing");
    }

    #[test]
    fn a_subject_of_another_format_is_refused() {
        let krate = Crate::new();
        krate.write("schemas/payments-value/subject.toml", "format = \"avro\"\n");
        let message = krate.check(None).0.expect_err("avro").to_string();
        assert!(
            message.contains("subject `payments-value` is a `avro` subject"),
            "{message}"
        );
    }

    #[test]
    fn what_is_missing_is_named() {
        let krate = Crate::new();
        let message = krate.check(None).0.expect_err("no subject").to_string();
        assert!(
            message.contains("subject `payments-value` has no readable"),
            "{message}"
        );

        krate.write(
            "schemas/payments-value/subject.toml",
            "format = \"protobuf\"\n",
        );
        let message = krate.check(None).0.expect_err("no source").to_string();
        assert!(message.contains("names no `source`"), "{message}");

        krate.payments(PAYMENT_V1, &[]);
        let message = krate.check(None).0.expect_err("no migration").to_string();
        assert!(message.contains("has no migration yet"), "{message}");

        krate.write(
            "schemas/payments-value/subject.toml",
            "format = \"protobuf\"\nsorce = \"x\"\n",
        );
        let message = krate.check(None).0.expect_err("typo").to_string();
        assert!(message.contains("does not parse"), "{message}");
    }

    #[test]
    fn debug_prints_the_message_a_build_script_shows() {
        let err = CheckProtoError::new("the message".to_owned());
        assert_eq!(format!("{err:?}"), "the message");
    }
}
