//! A crate directory the tool runs in, and the tool itself.

use std::fmt::Write;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use tempfile::TempDir;

/// The tool's binary.
const TOOL: &str = env!("CARGO_BIN_EXE_cargo-kafka-schema");

/// The switch a job sets to say it stood a registry up, so a skip is a defect.
const REQUIRE_LIVE: &str = "RUSTSTREAM_REQUIRE_LIVE";

/// The registry of the live scenarios, or `None` to skip them.
///
/// # Panics
///
/// Panics when [`REQUIRE_LIVE`] is set and the registry address is not.
pub(crate) fn registry_url() -> Option<String> {
    match std::env::var("SCHEMA_REGISTRY_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            let required = std::env::var(REQUIRE_LIVE).is_ok_and(|value| !value.is_empty());
            assert!(
                !required,
                "{REQUIRE_LIVE} is set, so the live scenarios must run, but \
                 SCHEMA_REGISTRY_TEST_URL is unset or empty"
            );
            None
        }
    }
}

/// A subject name no other run uses.
pub(crate) fn unique(base: &str) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_nanos());
    format!(
        "{base}-{}-{}-{nanos}-value",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// What one run of the tool printed.
pub(crate) struct Run {
    pub(crate) success: bool,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

impl Run {
    fn from(output: &Output) -> Self {
        Self {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }

    /// Asserts success and returns the run.
    #[track_caller]
    pub(crate) fn ok(self) -> Self {
        assert!(
            self.success,
            "the tool failed\nstdout:\n{}\nstderr:\n{}",
            self.stdout, self.stderr
        );
        self
    }

    /// Asserts failure and returns its standard error.
    #[track_caller]
    pub(crate) fn fails(self) -> String {
        assert!(
            !self.success,
            "the tool passed\nstdout:\n{}\nstderr:\n{}",
            self.stdout, self.stderr
        );
        self.stderr
    }
}

/// A crate the tool runs in: a manifest, `src/lib.rs`, and `schemas/`.
pub(crate) struct Crate {
    dir: TempDir,
    registry: Option<String>,
}

impl Crate {
    /// A crate with no export tests.
    pub(crate) fn new() -> Self {
        let krate = Self {
            dir: TempDir::new().expect("crate dir"),
            registry: None,
        };
        krate.write(
            "Cargo.toml",
            "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n",
        );
        krate.write("src/lib.rs", "//! A service.\n");
        krate
    }

    /// The same crate, talking to `url`.
    pub(crate) fn online(mut self, url: &str) -> Self {
        self.registry = Some(url.to_owned());
        self
    }

    /// Replaces `src/lib.rs` with export tests that write `schemas` (record full name, extension,
    /// text) the way the derive's hidden tests do.
    pub(crate) fn exporting(&self, schemas: &[(&str, &str, &str)]) {
        let mut lib =
            String::from("//! A service.\n\n#[cfg(test)]\nmod __ruststream_schema_export {\n");
        for (index, (record, ext, text)) in schemas.iter().enumerate() {
            let _ = write!(
                lib,
                "    #[test]\n    fn __ruststream_schema_export_{index}() {{\n        \
                 if let Some(dir) = std::env::var_os(\"RUSTSTREAM_SCHEMA_EXPORT\") {{\n            \
                 std::fs::write(std::path::Path::new(&dir).join(\"{record}.{ext}\"), {text:?})\
                 .expect(\"export\");\n        }}\n    }}\n"
            );
        }
        lib.push_str("}\n");
        self.write("src/lib.rs", &lib);
    }

    pub(crate) fn path(&self, relative: &str) -> PathBuf {
        self.dir.path().join(relative)
    }

    pub(crate) fn root(&self) -> &Path {
        self.dir.path()
    }

    pub(crate) fn write(&self, relative: &str, text: &str) {
        let path = self.path(relative);
        fs::create_dir_all(path.parent().expect("parent")).expect("dirs");
        fs::write(path, text).expect("write");
    }

    pub(crate) fn read(&self, relative: &str) -> String {
        fs::read_to_string(self.path(relative)).unwrap_or_else(|err| panic!("{relative}: {err}"))
    }

    pub(crate) fn exists(&self, relative: &str) -> bool {
        self.path(relative).exists()
    }

    /// Runs `cargo kafka-schema <args>` in the crate's directory.
    pub(crate) fn tool(&self, args: &[&str]) -> Run {
        let mut command = Command::new(TOOL);
        command
            .current_dir(self.dir.path())
            .arg("kafka-schema")
            .args(args)
            .env_remove("RUSTSTREAM_SCHEMA_DIR")
            .env_remove("RUSTSTREAM_SCHEMA_OFFLINE")
            .env_remove("RUSTSTREAM_SCHEMA_EXPORT")
            // The export builds the fixture crate. Its build directory stays inside the fixture,
            // so it goes with it, whatever build directory the machine's cargo config names.
            .env("CARGO_BUILD_BUILD_DIR", self.path("target/build"));
        match &self.registry {
            Some(url) => command.env("RUSTSTREAM_SCHEMA_REGISTRY", url),
            None => command.env_remove("RUSTSTREAM_SCHEMA_REGISTRY"),
        };
        Run::from(&command.output().expect("run the tool"))
    }
}
