//! The commands, over one crate's migrations directory, its lock and an optional registry.

pub(crate) mod add;
pub(crate) mod apply;
pub(crate) mod check;
pub(crate) mod import;
pub(crate) mod prepare;
pub(crate) mod status;

use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::catalog::Catalog;
use crate::registry::{self, Registry};
use crate::{CrateArgs, RegistryArgs};

/// The variable that moves the migrations directory away from `schemas/`.
const SCHEMA_DIR_VAR: &str = "RUSTSTREAM_SCHEMA_DIR";

/// One crate's migrations, and the registry when one is configured.
pub(crate) struct Workspace {
    pub(crate) krate: CrateArgs,
    pub(crate) manifest_dir: PathBuf,
    pub(crate) root: PathBuf,
    pub(crate) registry: Option<Registry>,
}

impl Workspace {
    pub(crate) fn open(krate: CrateArgs, registry: &RegistryArgs) -> Result<Self> {
        if !krate.manifest_path.is_file() {
            bail!(
                "`{}` is not a crate manifest: run the tool in the crate's directory, or name it \
                 with `--manifest-path`",
                krate.manifest_path.display()
            );
        }
        let manifest_dir = krate
            .manifest_path
            .parent()
            .filter(|dir| !dir.as_os_str().is_empty())
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
        let root = env::var_os(SCHEMA_DIR_VAR).map_or_else(
            || manifest_dir.join("schemas"),
            |dir| manifest_dir.join(dir),
        );
        Ok(Self {
            krate,
            manifest_dir,
            root,
            registry: registry::connect(registry)?,
        })
    }

    pub(crate) fn catalog(&self) -> Result<Catalog> {
        Catalog::read(&self.root)
    }

    /// The registry, for a command that cannot work offline.
    pub(crate) fn registry(&self, command: &str) -> Result<&Registry> {
        self.registry.as_ref().with_context(|| {
            format!(
                "`{command}` needs a registry: set RUSTSTREAM_SCHEMA_REGISTRY or pass \
                 `--registry <URL>` (and unset RUSTSTREAM_SCHEMA_OFFLINE)"
            )
        })
    }
}

/// Fails with every line of `problems`, printed first, or passes when there are none.
pub(crate) fn report(problems: &[String], what: &str) -> Result<()> {
    if problems.is_empty() {
        return Ok(());
    }
    for problem in problems {
        eprintln!("error: {problem}");
    }
    bail!("{what}: {} problem(s)", problems.len())
}
