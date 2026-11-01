//! The export: the crate's tests built with `RUSTSTREAM_SCHEMA_EXPORT` set, so the hidden test
//! the `Schema` derive emits per type writes the runtime's own schema of each record.

use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::ErrorKind;
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use tempfile::TempDir;

use crate::CrateArgs;
use crate::catalog::Format;

/// The variable that switches the derive to export mode.
pub(crate) const EXPORT_VAR: &str = "RUSTSTREAM_SCHEMA_EXPORT";

/// The name every hidden export test starts with.
pub(crate) const EXPORT_TEST: &str = "__ruststream_schema_export";

/// The schemas one export run wrote, by record full name.
pub(crate) struct Exports {
    dir: TempDir,
}

impl Exports {
    /// Builds and runs the crate's export tests.
    pub(crate) fn run(krate: &CrateArgs) -> Result<Self> {
        let dir = TempDir::new().context("creating the export directory")?;
        let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
        let mut command = Command::new(cargo);
        command
            .arg("test")
            .arg("--manifest-path")
            .arg(&krate.manifest_path)
            .arg("--tests");
        if let Some(features) = &krate.features {
            command.arg("--features").arg(features);
        }
        if krate.all_features {
            command.arg("--all-features");
        }
        if krate.no_default_features {
            command.arg("--no-default-features");
        }
        command
            .arg("--")
            .arg(EXPORT_TEST)
            .env(EXPORT_VAR, dir.path())
            .stdin(Stdio::null())
            .stderr(Stdio::inherit());
        eprintln!("exporting schemas: cargo test --tests -- {EXPORT_TEST}");
        let output = command
            .output()
            .context("running `cargo test` for the schema export")?;
        if !output.status.success() {
            eprintln!("{}", String::from_utf8_lossy(&output.stdout));
            bail!(
                "the schema export failed: `cargo test --tests -- {EXPORT_TEST}` exited with {}",
                output.status
            );
        }
        Ok(Self { dir })
    }

    /// The exported schema of `record`, or `None` when no derived type exported it.
    pub(crate) fn schema(&self, record: &str, format: Format) -> Result<Option<String>> {
        let path = self
            .dir
            .path()
            .join(format!("{record}.{}", format.extension()));
        match fs::read_to_string(&path) {
            Ok(text) => Ok(Some(text)),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err).with_context(|| format!("reading `{}`", path.display())),
        }
    }

    /// The format of the record's export, when exactly one format exported it.
    pub(crate) fn format_of(&self, record: &str) -> Option<Format> {
        let mut formats = [Format::Avro, Format::Json].into_iter().filter(|format| {
            self.dir
                .path()
                .join(format!("{record}.{}", format.extension()))
                .is_file()
        });
        let first = formats.next()?;
        formats.next().is_none().then_some(first)
    }
}
