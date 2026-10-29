//! `cargo kafka-schema`: schema migrations for a Confluent Schema Registry, kept in the
//! repository of the service that owns them.

mod avro;
mod catalog;
mod commands;
mod export;
mod json;
mod lock;
mod normalize;
mod proto;
mod registry;
mod state;
#[cfg(test)]
mod testdir;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};

use crate::catalog::{Format, Owner};

/// The entry point cargo calls: `cargo kafka-schema ..` runs `cargo-kafka-schema kafka-schema ..`.
#[derive(Debug, Parser)]
#[command(name = "cargo", bin_name = "cargo")]
enum Cargo {
    KafkaSchema(KafkaSchema),
}

/// Schema migrations for a Confluent Schema Registry, kept in `schemas/` beside the crate.
///
/// A directory under `schemas/` is a subject; a migration is the full schema of one subject
/// version, numbered `NNNN_<description>`. `kafka-schema.lock` records each migration's checksum,
/// registry version and schema id, and is committed. Run it from the crate's directory, or name
/// the crate's manifest with `--manifest-path`.
#[derive(Debug, Args)]
#[command(version, about, long_about)]
struct KafkaSchema {
    #[command(flatten)]
    krate: CrateArgs,
    #[command(flatten)]
    registry: RegistryArgs,
    #[command(subcommand)]
    command: Command,
}

/// Which crate the schemas belong to, and how it is built for the export.
#[derive(Debug, Clone, Args)]
pub(crate) struct CrateArgs {
    /// The crate's `Cargo.toml`; its `schemas/` (or `RUSTSTREAM_SCHEMA_DIR`) holds the migrations
    #[arg(long, global = true, value_name = "PATH", default_value = "Cargo.toml")]
    pub(crate) manifest_path: PathBuf,
    /// Features to build the crate with for the schema export (comma-separated)
    #[arg(long, global = true, value_name = "FEATURES")]
    pub(crate) features: Option<String>,
    /// Build the crate with all its features for the schema export
    #[arg(long, global = true)]
    pub(crate) all_features: bool,
    /// Build the crate without its default features for the schema export
    #[arg(long, global = true)]
    pub(crate) no_default_features: bool,
}

/// Where the registry is. Credentials are never printed.
#[derive(Debug, Clone, Args)]
pub(crate) struct RegistryArgs {
    /// The registry URL; credentials may ride its user part (`https://user:pass@host:8081`).
    /// Without it, every command but `migrate import` and `migrate run` works offline
    #[arg(
        long = "registry",
        global = true,
        env = "RUSTSTREAM_SCHEMA_REGISTRY",
        hide_env_values = true,
        value_name = "URL"
    )]
    pub(crate) url: Option<String>,
    /// The basic-auth user, when it is not in the URL
    #[arg(
        long = "registry-user",
        global = true,
        env = "RUSTSTREAM_SCHEMA_REGISTRY_USER",
        hide_env_values = true,
        value_name = "USER"
    )]
    pub(crate) user: Option<String>,
    /// The basic-auth password, when it is not in the URL
    #[arg(
        long = "registry-password",
        global = true,
        env = "RUSTSTREAM_SCHEMA_REGISTRY_PASSWORD",
        hide_env_values = true,
        value_name = "PASSWORD"
    )]
    pub(crate) password: Option<String>,
    /// A bearer token instead of basic auth
    #[arg(
        long = "registry-token",
        global = true,
        env = "RUSTSTREAM_SCHEMA_REGISTRY_TOKEN",
        hide_env_values = true,
        value_name = "TOKEN",
        conflicts_with_all = ["user", "password"]
    )]
    pub(crate) token: Option<String>,
    /// Work offline even when a registry URL is set
    #[arg(long, global = true, env = "RUSTSTREAM_SCHEMA_OFFLINE", value_name = "BOOL",
          num_args = 0..=1, default_missing_value = "true", require_equals = true)]
    pub(crate) offline: Option<bool>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Add, import, list and apply migrations
    #[command(subcommand)]
    Migrate(Migrate),
    /// Refresh the lock from the registry (offline: from the migrations alone)
    ///
    /// Records every migration's checksum, and with a registry its version and schema id and the
    /// subject's version list. Drift, or a change to a registered migration, fails it and writes
    /// nothing.
    Prepare {
        /// Write nothing; fail when the lock differs from what `prepare` would write (for CI)
        #[arg(long)]
        check: bool,
    },
    /// The CI gate: exports equal migrations, checksums hold, local compatibility holds, and
    /// with a registry, no drift
    ///
    /// Builds the crate's tests with `RUSTSTREAM_SCHEMA_EXPORT` set and compares each owned Avro
    /// and JSON Schema subject's exported schema with its newest migration; compares a Protobuf
    /// subject's `source` with its newest migration. With a registry it also checks drift and
    /// asks the registry whether each pending migration is compatible.
    Check,
}

#[derive(Debug, Subcommand)]
enum Migrate {
    /// Write the next migration of a subject from the crate's current schema
    ///
    /// For Avro and JSON Schema it builds the crate's tests with checks off, exports the schema
    /// of the record the subject declares, and writes `NNNN_<description>.<ext>` when it differs
    /// from the newest migration. For Protobuf it snapshots the subject's `source`. The new
    /// migration is checked locally (Avro compatibility under the subject's level, the Protobuf
    /// `reserved` rule, the JSON metaschema and content model) and recorded in the lock as
    /// pending.
    Add {
        /// The subject (`orders-value`)
        subject: String,
        /// What the migration does, in snake case (`add_item`)
        description: String,
        /// The record a new subject declares (`com.acme.orders.Order`); later migrations read
        /// it from `subject.toml`
        #[arg(long)]
        record: Option<String>,
        /// The format of a new subject; inferred from the exported file when not given
        #[arg(long, value_enum)]
        format: Option<Format>,
        /// The `.proto` file of a new Protobuf subject, relative to the crate
        #[arg(long, value_name = "PATH")]
        source: Option<String>,
        /// The compatibility level of a new subject (`BACKWARD_TRANSITIVE`, ..)
        #[arg(long, value_name = "LEVEL")]
        compatibility: Option<String>,
    },
    /// Pull every registry version of a subject as migrations (a baseline, or an external
    /// subject)
    ///
    /// Versions the directory already describes are kept; the versions after them are written
    /// as `NNNN.<ext>` and recorded in the lock as registered. Needs a registry.
    Import {
        /// The subject (`inventory-value`)
        subject: String,
        /// Who writes a new subject: another service (`external`), or this one (`service`, a
        /// baseline of a subject registered before its migrations existed)
        #[arg(long, value_enum, default_value = "external")]
        owner: Owner,
        /// The record a new JSON Schema subject declares; Avro and Protobuf read it from the
        /// newest version
        #[arg(long)]
        record: Option<String>,
    },
    /// Per subject: its migrations, registry versions, pending migrations and drift
    ///
    /// Offline it reports what the lock records. Exits with an error when it finds drift or a
    /// stale lock.
    Status,
    /// Register every pending migration, in order, after the registry's compatibility check
    ///
    /// Per owned subject, in reference order: sets the declared compatibility level, then for
    /// each pending migration asks `/compatibility/subjects/{subject}/versions?verbose=true`
    /// (every version the level names), prints the registry's messages as they come, registers,
    /// and records the version and id in the lock. Drift anywhere stops it before it registers
    /// anything. Needs a registry.
    Run {
        /// Ask the compatibility questions and print the plan; set and register nothing
        #[arg(long)]
        dry_run: bool,
    },
}

fn main() -> ExitCode {
    let Cargo::KafkaSchema(args) = Cargo::parse();
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("error: starting the async runtime: {err}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(dispatch(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn dispatch(args: KafkaSchema) -> anyhow::Result<()> {
    let workspace = commands::Workspace::open(args.krate, &args.registry)?;
    match args.command {
        Command::Migrate(Migrate::Add {
            subject,
            description,
            record,
            format,
            source,
            compatibility,
        }) => {
            let new = commands::add::NewSubject {
                record,
                format,
                source,
                compatibility,
            };
            commands::add::run(&workspace, &subject, &description, new)
        }
        Command::Migrate(Migrate::Import {
            subject,
            owner,
            record,
        }) => commands::import::run(&workspace, &subject, owner, record).await,
        Command::Migrate(Migrate::Status) => commands::status::run(&workspace).await,
        Command::Migrate(Migrate::Run { dry_run }) => {
            commands::apply::run(&workspace, dry_run).await
        }
        Command::Prepare { check } => commands::prepare::run(&workspace, check).await,
        Command::Check => commands::check::run(&workspace).await,
    }
}
