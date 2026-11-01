//! The registry drift guard: what the derive checks against the schema registry when the build
//! is online, and the tokens that rebuild the crate when the lock or a switch changes.
//!
//! The derive and the JSON Schema path call one function:
//!
//! ```text
//! pub(crate) fn guard(
//!     subjects: &[&migrations::Subject],
//!     span: proc_macro2::Span,
//! ) -> syn::Result<proc_macro2::TokenStream>
//! ```
//!
//! - `subjects` are the subjects the derived type is checked against; `span` is where errors and
//!   warnings point (the type's name).
//! - The returned tokens are one module-level item, `const _: () = { .. };`, erased from the
//!   binary. They `include_bytes!` the lock when it exists and read every switch with
//!   `option_env!`, so a new lock or a changed switch rebuilds the crate with no build script.
//!   The caller places them at module level beside its other generated items.
//! - Online (`RUSTSTREAM_SCHEMA_REGISTRY` set and `RUSTSTREAM_SCHEMA_OFFLINE` not `true`), each
//!   subject's version list is fetched once per compiler process and compared with the lock.
//!   Drift, or a registry that cannot be reached, is the returned error. A pending migration (a
//!   file with no registry version in the lock) is a warning: the tokens use a deprecated item
//!   whose note names it.
//! - Offline, nothing is fetched and the lock is not parsed.

use std::collections::HashMap;
use std::env;
use std::fmt::{self, Debug, Formatter};
use std::path::Path;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, quote_spanned};
use serde::Deserialize;

use crate::lock::{self, LockedSubject};
use crate::migrations::{self, Subject};

/// The switch that names the registry and turns the online check on.
const REGISTRY: &str = "RUSTSTREAM_SCHEMA_REGISTRY";

/// The switch that turns the online check off while a registry is named.
const OFFLINE: &str = "RUSTSTREAM_SCHEMA_OFFLINE";

/// The bound on one request, connection included.
const TIMEOUT: Duration = Duration::from_secs(5);

/// The registry's error code for a subject it has never seen.
const SUBJECT_NOT_FOUND: u32 = 40401;

/// What a subject name keeps unescaped in a URL path (RFC 3986 unreserved).
const PATH_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Checks `subjects` against the registry when the build is online, and returns the tokens that
/// tie the crate's rebuild to the lock and the switches.
///
/// # Errors
///
/// Drift between the lock and the registry, a registry that cannot be reached, a lock that does
/// not parse (online only), or a lock path that is not UTF-8.
pub(crate) fn guard(subjects: &[&Subject], span: Span) -> syn::Result<TokenStream> {
    let lock_path = migrations::schema_dir().join(migrations::LOCK_FILE);
    let registry = Registry::online(
        env::var(REGISTRY).ok().as_deref(),
        env::var(OFFLINE).ok().as_deref(),
    );
    guard_with(
        subjects,
        span,
        &lock_path,
        registry.as_ref(),
        |registry, subject| {
            memoized(registry, subject, || {
                fetch_versions(registry, subject, TIMEOUT)
            })
        },
    )
}

/// [`guard`] with the lock's path, the switches and the fetch passed in, so tests need no
/// environment and no network.
fn guard_with(
    subjects: &[&Subject],
    span: Span,
    lock_path: &Path,
    registry: Option<&Registry>,
    mut fetch: impl FnMut(&Registry, &str) -> Fetched,
) -> syn::Result<TokenStream> {
    let lock_file = if lock_path.is_file() {
        let path = lock_path.to_str().ok_or_else(|| {
            syn::Error::new(
                span,
                format!(
                    "the schema lock path `{}` is not UTF-8",
                    lock_path.display()
                ),
            )
        })?;
        Some(path)
    } else {
        None
    };
    let mut pending = Vec::new();
    if let Some(registry) = registry {
        let lock = lock::read(lock_path).map_err(|err| syn::Error::new(span, err))?;
        let mut errors: Option<syn::Error> = None;
        for subject in subjects {
            let versions =
                fetch(registry, &subject.name).map_err(|err| syn::Error::new(span, err))?;
            let locked = lock.as_ref().and_then(|lock| lock.subject(&subject.name));
            let report = drift(&subject.name, &migration_files(subject), locked, &versions);
            for message in report.errors {
                let error = syn::Error::new(span, message);
                match &mut errors {
                    Some(errors) => errors.combine(error),
                    None => errors = Some(error),
                }
            }
            pending.extend(report.pending.into_iter().map(|file| {
                format!(
                    "subject `{}`: migration `{file}` is pending, the schema registry does not \
                     hold it yet; `cargo kafka-schema migrate run` registers it",
                    subject.name
                )
            }));
        }
        if let Some(errors) = errors {
            return Err(errors);
        }
    }
    Ok(tokens(lock_file, &pending, span))
}

/// The file names of a subject's migrations, in number order.
fn migration_files(subject: &Subject) -> Vec<&str> {
    subject
        .migrations
        .iter()
        .map(|migration| {
            migration
                .path
                .file_name()
                .and_then(|file| file.to_str())
                .unwrap_or(&migration.stem)
        })
        .collect()
}

/// The item that ties the rebuild to the lock and the switches, and warns about each pending
/// migration: a deprecated constant the block uses, because a stable proc macro cannot emit a
/// warning of its own. The constant carries the type's span, so rustc reports the warning as
/// the user's code, not as an expansion of a foreign macro.
fn tokens(lock_file: Option<&str>, pending: &[String], span: Span) -> TokenStream {
    let lock = lock_file.map(|path| quote! { const _: &[u8] = ::core::include_bytes!(#path); });
    let warnings = pending.iter().enumerate().map(|(index, note)| {
        let name = format_ident!("PENDING_SCHEMA_MIGRATION_{}", index, span = span);
        quote_spanned! {span=>
            #[deprecated(note = #note)]
            const #name: () = ();
            const _: () = #name;
        }
    });
    quote! {
        const _: () = {
            #lock
            const _: ::core::option::Option<&str> =
                ::core::option_env!("RUSTSTREAM_SCHEMA_REGISTRY");
            const _: ::core::option::Option<&str> =
                ::core::option_env!("RUSTSTREAM_SCHEMA_OFFLINE");
            const _: ::core::option::Option<&str> =
                ::core::option_env!("RUSTSTREAM_SCHEMA_EXPORT");
            const _: ::core::option::Option<&str> =
                ::core::option_env!("RUSTSTREAM_SCHEMA_DIR");
            #(#warnings)*
        };
    }
}

/// What a subject's comparison with the registry found.
#[derive(Debug, Default, PartialEq, Eq)]
struct Report {
    /// One message per drift, each naming its fix.
    errors: Vec<String>,
    /// The migration files the lock holds no registry version for.
    pending: Vec<String>,
}

/// Compares a subject's migrations (`files`, in number order) and its lock entry with the
/// version list the registry holds.
fn drift(
    subject: &str,
    files: &[&str],
    locked: Option<&LockedSubject>,
    registry: &[u32],
) -> Report {
    let mut applied: Vec<(&str, u32)> = locked
        .map(|locked| {
            locked
                .migrations
                .iter()
                .filter_map(|migration| Some((migration.file.as_str(), migration.version?)))
                .collect()
        })
        .unwrap_or_default();
    // The four-digit prefix makes the file name order the migration number order.
    applied.sort_unstable_by_key(|(file, _)| *file);
    let mut errors = Vec::new();

    let undescribed: Vec<String> = registry
        .iter()
        .filter(|version| !applied.iter().any(|(_, applied)| applied == *version))
        .map(u32::to_string)
        .collect();
    if !undescribed.is_empty() {
        errors.push(format!(
            "schema drift in subject `{subject}`: the registry holds version {}, which no \
             migration in `{}` describes; run `cargo kafka-schema migrate import {subject}` to \
             record it as a migration, or delete it from the registry",
            undescribed.join(", "),
            migrations::LOCK_FILE,
        ));
    }

    for (file, version) in &applied {
        if !registry.contains(version) {
            errors.push(format!(
                "schema drift in subject `{subject}`: migration `{file}` is applied as version \
                 {version} in `{}`, and the registry holds no version {version} (it holds [{}]); \
                 run `cargo kafka-schema migrate import {subject}` to rebuild the lock from the \
                 registry, or restore the version in the registry",
                migrations::LOCK_FILE,
                registry
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }
    }

    for pair in applied.windows(2) {
        let [(earlier, earlier_version), (later, later_version)] = pair else {
            continue;
        };
        if later_version <= earlier_version {
            errors.push(format!(
                "schema drift in subject `{subject}`: migration `{later}` is version \
                 {later_version} and the earlier migration `{earlier}` is version \
                 {earlier_version}, so the registry's version order differs from the migration \
                 order; run `cargo kafka-schema migrate import {subject}` to rebuild the \
                 migrations from the registry, or clean the registry up",
            ));
        }
    }

    let pending = files
        .iter()
        .filter(|file| {
            locked
                .and_then(|locked| locked.migration(file))
                .and_then(|migration| migration.version)
                .is_none()
        })
        .map(|file| (*file).to_owned())
        .collect();
    Report { errors, pending }
}

/// A registry the build reaches: its base URL without credentials, and the credentials as a
/// basic-auth header value.
#[derive(Clone, PartialEq, Eq, Hash)]
struct Registry {
    base: String,
    authorization: Option<String>,
}

// By hand, so the credentials never reach a log or a panic message.
impl Debug for Registry {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Registry")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl Registry {
    /// The registry the switches name, or `None` when the build is offline.
    fn online(registry: Option<&str>, offline: Option<&str>) -> Option<Self> {
        let url = registry.map(str::trim).filter(|url| !url.is_empty())?;
        if offline.is_some_and(|offline| offline.trim() == "true") {
            return None;
        }
        Some(Self::parse(url))
    }

    /// Splits the credentials of `scheme://user:pass@host` off the URL.
    fn parse(url: &str) -> Self {
        let (scheme, rest) = url.split_once("://").unwrap_or(("http", url));
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let (authority, path) = rest.split_at(authority_end);
        let (authorization, host) = match authority.rsplit_once('@') {
            Some((userinfo, host)) => {
                let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
                let decode = |part: &str| percent_decode_str(part).decode_utf8_lossy().into_owned();
                let token = STANDARD.encode(format!("{}:{}", decode(user), decode(password)));
                (Some(format!("Basic {token}")), host)
            }
            None => (None, authority),
        };
        Self {
            base: format!("{scheme}://{host}{}", path.trim_end_matches('/')),
            authorization,
        }
    }
}

/// The registry's error body.
#[derive(Deserialize)]
struct RegistryError {
    error_code: u32,
}

/// `GET {base}/subjects/{subject}/versions`; a subject the registry has never seen holds no
/// versions.
fn fetch_versions(registry: &Registry, subject: &str, timeout: Duration) -> Fetched {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .http_status_as_error(false)
        .build()
        .into();
    let url = format!(
        "{}/subjects/{}/versions",
        registry.base,
        utf8_percent_encode(subject, PATH_SEGMENT)
    );
    let mut request = agent
        .get(&url)
        .header("Accept", "application/vnd.schemaregistry.v1+json");
    if let Some(authorization) = &registry.authorization {
        request = request.header("Authorization", authorization);
    }
    let unreachable = |err: ureq::Error| {
        format!(
            "the schema registry `{}` cannot be reached for subject `{subject}`: {err}; set \
             `{OFFLINE}=true` to build without the registry check",
            registry.base
        )
    };
    let mut response = request.call().map_err(unreachable)?;
    let status = response.status().as_u16();
    let body = response.body_mut().read_to_string().map_err(unreachable)?;
    if status == 200 {
        return serde_json::from_str(&body).map_err(|err| {
            format!(
                "the schema registry `{}` answered the version list of subject `{subject}` with \
                 something that is not a list of versions ({err}): {body}",
                registry.base
            )
        });
    }
    if status == 404
        && serde_json::from_str::<RegistryError>(&body)
            .is_ok_and(|error| error.error_code == SUBJECT_NOT_FOUND)
    {
        return Ok(Vec::new());
    }
    Err(format!(
        "the schema registry `{}` answered {status} to the version list of subject `{subject}`: \
         {body}; set `{OFFLINE}=true` to build without the registry check",
        registry.base
    ))
}

/// A subject's version list, or why the registry did not give it.
type Fetched = Result<Vec<u32>, String>;

/// `fetch` once per registry and subject for the life of the compiler process: every derived
/// type of the crate checks against the same answer, and an unreachable registry costs one
/// timeout, not one per type.
fn memoized(registry: &Registry, subject: &str, fetch: impl FnOnce() -> Fetched) -> Fetched {
    static FETCHED: OnceLock<Mutex<HashMap<(Registry, String), Fetched>>> = OnceLock::new();
    let cache = FETCHED.get_or_init(Mutex::default);
    let key = (registry.clone(), subject.to_owned());
    if let Some(fetched) = cache
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&key)
    {
        return fetched.clone();
    }
    let fetched = fetch();
    cache
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(key, fetched.clone());
    fetched
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::thread::{self, JoinHandle};
    use std::time::Instant;

    use super::*;
    use crate::lock::LockedMigration;
    use crate::migrations::{Format, Migration, Owner, Settings};

    fn locked(versions: &[(&str, Option<u32>)]) -> LockedSubject {
        LockedSubject {
            name: "orders-value".into(),
            registry_versions: versions
                .iter()
                .filter_map(|(_, version)| *version)
                .collect(),
            migrations: versions
                .iter()
                .map(|(file, version)| LockedMigration {
                    file: (*file).into(),
                    sha256: "00".into(),
                    version: *version,
                    id: version.map(|version| 100_000 + version),
                })
                .collect(),
        }
    }

    const FILES: [&str; 2] = ["0001_create_order.avsc", "0002_add_item.avsc"];

    #[test]
    fn a_registry_in_step_with_the_lock_is_no_drift() {
        let lock = locked(&[(FILES[0], Some(1)), (FILES[1], Some(2))]);
        assert_eq!(
            drift("orders-value", &FILES, Some(&lock), &[1, 2]),
            Report::default()
        );
    }

    #[test]
    fn a_registry_version_no_migration_describes_is_drift() {
        let lock = locked(&[(FILES[0], Some(1)), (FILES[1], Some(2))]);
        let report = drift("orders-value", &FILES, Some(&lock), &[1, 2, 3, 4]);
        assert_eq!(report.errors.len(), 1, "{report:?}");
        let error = &report.errors[0];
        assert!(error.contains("subject `orders-value`"), "{error}");
        assert!(
            error.contains("holds version 3, 4, which no migration"),
            "{error}"
        );
        assert!(error.contains("kafka-schema.lock"), "{error}");
        assert!(
            error.contains("cargo kafka-schema migrate import orders-value"),
            "{error}"
        );
    }

    #[test]
    fn an_applied_migration_missing_from_the_registry_is_drift() {
        let lock = locked(&[(FILES[0], Some(1)), (FILES[1], Some(2))]);
        let report = drift("orders-value", &FILES, Some(&lock), &[1]);
        assert_eq!(report.errors.len(), 1, "{report:?}");
        let error = &report.errors[0];
        assert!(
            error.contains("migration `0002_add_item.avsc` is applied as version 2"),
            "{error}"
        );
        assert!(
            error.contains("holds no version 2 (it holds [1])"),
            "{error}"
        );
        assert!(
            error.contains("cargo kafka-schema migrate import orders-value"),
            "{error}"
        );
    }

    #[test]
    fn versions_out_of_migration_order_are_drift() {
        let lock = locked(&[(FILES[1], Some(1)), (FILES[0], Some(2))]);
        let report = drift("orders-value", &FILES, Some(&lock), &[1, 2]);
        assert_eq!(report.errors.len(), 1, "{report:?}");
        let error = &report.errors[0];
        assert!(
            error.contains(
                "migration `0002_add_item.avsc` is version 1 and the earlier migration \
                 `0001_create_order.avsc` is version 2"
            ),
            "{error}"
        );
        assert!(error.contains("clean the registry up"), "{error}");
    }

    #[test]
    fn a_migration_with_no_registry_version_in_the_lock_is_pending() {
        let lock = locked(&[(FILES[0], Some(1)), (FILES[1], None)]);
        let report = drift("orders-value", &FILES, Some(&lock), &[1]);
        assert_eq!(report.errors, Vec::<String>::new());
        assert_eq!(report.pending, ["0002_add_item.avsc"]);

        let unlocked = drift("orders-value", &FILES, None, &[]);
        assert_eq!(unlocked.errors, Vec::<String>::new());
        assert_eq!(unlocked.pending, FILES);
    }

    #[test]
    fn the_build_is_online_only_with_a_url_and_no_offline_switch() {
        let url = Some("http://registry:8081");
        assert!(Registry::online(url, None).is_some());
        assert!(Registry::online(url, Some("false")).is_some());
        assert!(Registry::online(url, Some("true")).is_none());
        assert!(Registry::online(None, None).is_none());
        assert!(Registry::online(Some(""), None).is_none());
    }

    #[test]
    fn credentials_leave_the_url_for_a_basic_auth_header() {
        let registry = Registry::parse("https://user:p%40ss@registry:8081/");
        assert_eq!(registry.base, "https://registry:8081");
        // base64("user:p@ss")
        assert_eq!(
            registry.authorization.as_deref(),
            Some("Basic dXNlcjpwQHNz")
        );
        assert!(!format!("{registry:?}").contains("dXNlcjpwQHNz"));

        let plain = Registry::parse("http://registry:8081/prefix/");
        assert_eq!(plain.base, "http://registry:8081/prefix");
        assert_eq!(plain.authorization, None);
    }

    /// A one-shot HTTP server: it answers the first request with `response` and returns the
    /// request it read.
    fn serve(response: String) -> Result<(String, JoinHandle<String>), String> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
        let port = listener.local_addr().map_err(|err| err.to_string())?.port();
        let server = thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return String::new();
            };
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                match stream.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => request.extend_from_slice(&buffer[..read]),
                }
            }
            if response.is_empty() {
                // Never answers: holds the connection until the client gives up.
                let _ = stream.read(&mut buffer);
            } else {
                let _ = stream.write_all(response.as_bytes());
            }
            String::from_utf8_lossy(&request).into_owned()
        });
        Ok((format!("127.0.0.1:{port}"), server))
    }

    /// Joins the server; a client that never connected leaves it in `accept`, so a connection of
    /// the test's own releases it first.
    fn finish(address: &str, server: JoinHandle<String>) -> Result<String, String> {
        if !server.is_finished() {
            let _ = TcpStream::connect(address);
        }
        server.join().map_err(|_| String::from("server panicked"))
    }

    fn http(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/vnd.schemaregistry.v1+json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    #[test]
    fn fetches_the_version_list_with_basic_auth() -> Result<(), String> {
        let (address, server) = serve(http("200 OK", "[1,2,3]"))?;
        let registry = Registry::parse(&format!("http://user:secret@{address}"));
        let versions = fetch_versions(&registry, "orders value", TIMEOUT)?;
        let request = finish(&address, server)?;
        assert_eq!(versions, [1, 2, 3]);
        assert!(
            request.starts_with("GET /subjects/orders%20value/versions HTTP/1.1"),
            "{request}"
        );
        // base64("user:secret")
        let authorization = request.lines().find_map(|line| {
            let (name, value) = line.split_once(": ")?;
            name.eq_ignore_ascii_case("authorization").then_some(value)
        });
        assert_eq!(authorization, Some("Basic dXNlcjpzZWNyZXQ="), "{request}");
        Ok(())
    }

    #[test]
    fn a_subject_the_registry_never_saw_holds_no_versions() -> Result<(), String> {
        let (address, server) = serve(http(
            "404 Not Found",
            r#"{"error_code":40401,"message":"Subject 'orders-value' not found."}"#,
        ))?;
        let versions = fetch_versions(
            &Registry::parse(&format!("http://{address}")),
            "orders-value",
            TIMEOUT,
        )?;
        finish(&address, server)?;
        assert_eq!(versions, Vec::<u32>::new());
        Ok(())
    }

    #[test]
    fn another_answer_is_an_error_without_the_credentials() -> Result<(), String> {
        let (address, server) = serve(http(
            "404 Not Found",
            r#"{"error_code":40403,"message":"Schema not found"}"#,
        ))?;
        let registry = Registry::parse(&format!("http://user:secret@{address}"));
        let err = fetch_versions(&registry, "orders-value", TIMEOUT).unwrap_err();
        finish(&address, server)?;
        assert!(
            err.contains(&format!("`http://{address}` answered 404")),
            "{err}"
        );
        assert!(err.contains("RUSTSTREAM_SCHEMA_OFFLINE=true"), "{err}");
        assert!(!err.contains("secret"), "{err}");
        Ok(())
    }

    #[test]
    fn an_unreachable_registry_names_the_url_and_the_offline_switch() -> Result<(), String> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
        let port = listener.local_addr().map_err(|err| err.to_string())?.port();
        drop(listener);
        let registry = Registry::parse(&format!("http://user:secret@127.0.0.1:{port}"));
        let err = fetch_versions(&registry, "orders-value", TIMEOUT).unwrap_err();
        assert!(
            err.starts_with(&format!(
                "the schema registry `http://127.0.0.1:{port}` cannot be reached for subject \
                 `orders-value`"
            )),
            "{err}"
        );
        assert!(
            err.contains("set `RUSTSTREAM_SCHEMA_OFFLINE=true`"),
            "{err}"
        );
        assert!(!err.contains("secret") && !err.contains("user"), "{err}");
        Ok(())
    }

    #[test]
    fn a_registry_that_never_answers_is_cut_off_at_the_timeout() -> Result<(), String> {
        let (address, server) = serve(String::new())?;
        let started = Instant::now();
        let err = fetch_versions(
            &Registry::parse(&format!("http://{address}")),
            "orders-value",
            Duration::from_millis(300),
        )
        .unwrap_err();
        let elapsed = started.elapsed();
        finish(&address, server)?;
        assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
        assert!(err.contains("cannot be reached"), "{err}");
        Ok(())
    }

    #[test]
    fn a_subject_is_fetched_once_per_process() {
        let registry = Registry::parse("http://memo-test:1");
        let mut calls = 0;
        for _ in 0..3 {
            let fetched = memoized(&registry, "memo-value", || {
                calls += 1;
                Ok(vec![7])
            });
            assert_eq!(fetched, Ok(vec![7]));
        }
        assert_eq!(calls, 1);
    }

    fn subject(dir: &Path, files: &[&str]) -> Subject {
        Subject {
            name: "orders-value".into(),
            settings: Settings {
                format: Format::Avro,
                compatibility: None,
                owner: Owner::Service,
                record: None,
                source: None,
                references: Vec::new(),
            },
            migrations: files
                .iter()
                .zip(1..)
                .map(|(file, number)| Migration {
                    number,
                    stem: file.trim_end_matches(".avsc").into(),
                    path: dir.join("orders-value").join(file),
                    content: String::new(),
                })
                .collect(),
        }
    }

    fn scratch(name: &str) -> Result<PathBuf, String> {
        let dir = env::temp_dir().join(format!("rs-online-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
        Ok(dir)
    }

    const LOCK: &str = r#"
version = 1

[[subject]]
name = "orders-value"
registry-versions = [1]

[[subject.migration]]
file = "0001_create_order.avsc"
sha256 = "00"
version = 1
id = 100001
"#;

    #[test]
    fn offline_tokens_tie_the_rebuild_to_the_lock_and_the_switches() -> Result<(), String> {
        let dir = scratch("offline")?;
        let lock_path = dir.join("kafka-schema.lock");
        let subject = subject(&dir, &FILES);
        let unfetched = |_: &Registry, _: &str| -> Fetched { Err("fetched offline".into()) };

        let without_lock = guard_with(&[&subject], Span::call_site(), &lock_path, None, unfetched)
            .map_err(|err| err.to_string())?
            .to_string();
        assert!(!without_lock.contains("include_bytes"), "{without_lock}");

        fs::write(&lock_path, "not a lock").map_err(|err| err.to_string())?;
        let tokens = guard_with(&[&subject], Span::call_site(), &lock_path, None, unfetched)
            .map_err(|err| err.to_string())?
            .to_string();
        fs::remove_dir_all(&dir).map_err(|err| err.to_string())?;
        assert!(tokens.starts_with("const _ : () = {"), "{tokens}");
        assert!(
            tokens.contains(&format!(
                "include_bytes ! ({:?})",
                lock_path.display().to_string()
            )),
            "{tokens}"
        );
        for switch in [
            "RUSTSTREAM_SCHEMA_REGISTRY",
            "RUSTSTREAM_SCHEMA_OFFLINE",
            "RUSTSTREAM_SCHEMA_EXPORT",
            "RUSTSTREAM_SCHEMA_DIR",
        ] {
            assert!(
                tokens.contains(&format!("option_env ! (\"{switch}\")")),
                "{tokens}"
            );
        }
        assert!(!tokens.contains("deprecated"), "{tokens}");
        Ok(())
    }

    #[test]
    fn online_a_pending_migration_becomes_a_deprecated_item_the_block_uses() -> Result<(), String> {
        let dir = scratch("pending")?;
        let lock_path = dir.join("kafka-schema.lock");
        fs::write(&lock_path, LOCK).map_err(|err| err.to_string())?;
        let subject = subject(&dir, &FILES);
        let registry = Registry::parse("http://registry:8081");
        let result = guard_with(
            &[&subject],
            Span::call_site(),
            &lock_path,
            Some(&registry),
            |_, name| {
                assert_eq!(name, "orders-value");
                Ok(vec![1])
            },
        );
        fs::remove_dir_all(&dir).map_err(|err| err.to_string())?;
        let tokens = result.map_err(|err| err.to_string())?.to_string();
        assert!(
            tokens.contains(
                "# [deprecated (note = \"subject `orders-value`: migration `0002_add_item.avsc` \
                 is pending, the schema registry does not hold it yet; `cargo kafka-schema \
                 migrate run` registers it\")] const PENDING_SCHEMA_MIGRATION_0 : () = () ; \
                 const _ : () = PENDING_SCHEMA_MIGRATION_0 ;"
            ),
            "{tokens}"
        );
        Ok(())
    }

    #[test]
    fn online_drift_and_an_unreachable_registry_fail_the_build() -> Result<(), String> {
        let dir = scratch("drift")?;
        let lock_path = dir.join("kafka-schema.lock");
        fs::write(&lock_path, LOCK).map_err(|err| err.to_string())?;
        let subject = subject(&dir, &FILES[..1]);
        let registry = Registry::parse("http://registry:8081");
        let drifted = guard_with(
            &[&subject],
            Span::call_site(),
            &lock_path,
            Some(&registry),
            |_, _| Ok(vec![1, 2]),
        );
        let unreachable = guard_with(
            &[&subject],
            Span::call_site(),
            &lock_path,
            Some(&registry),
            |_, _| Err("the schema registry is down".into()),
        );
        fs::remove_dir_all(&dir).map_err(|err| err.to_string())?;
        let drifted = drifted
            .map(|tokens| tokens.to_string())
            .unwrap_err()
            .to_string();
        assert!(
            drifted.contains("the registry holds version 2,"),
            "{drifted}"
        );
        let unreachable = unreachable
            .map(|tokens| tokens.to_string())
            .unwrap_err()
            .to_string();
        assert_eq!(unreachable, "the schema registry is down");
        Ok(())
    }

    /// Against the stand's registry (`just test-brokers`): a registered subject lists its
    /// version, an unknown one lists none.
    #[test]
    fn live_registry_lists_versions() -> Result<(), String> {
        let Some(url) = live_url() else {
            return Ok(());
        };
        let registry = Registry::parse(&url);
        let subject = format!("ruststream-online-guard-{}", std::process::id());
        let schema = r#"{"schema":"{\"type\":\"record\",\"name\":\"Probe\",\"fields\":[]}"}"#;
        ureq::post(&format!("{}/subjects/{subject}/versions", registry.base))
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .send(schema)
            .map_err(|err| err.to_string())?;
        let versions = fetch_versions(&registry, &subject, TIMEOUT);
        let _ = ureq::delete(&format!("{}/subjects/{subject}", registry.base)).call();
        let _ = ureq::delete(&format!(
            "{}/subjects/{subject}?permanent=true",
            registry.base
        ))
        .call();
        assert_eq!(versions?, [1]);
        assert_eq!(
            fetch_versions(&registry, &format!("{subject}-absent"), TIMEOUT)?,
            Vec::<u32>::new()
        );
        Ok(())
    }

    /// The live gate of the repository's suites: `SCHEMA_REGISTRY_TEST_URL` names the stand,
    /// and under `RUSTSTREAM_REQUIRE_LIVE` a skip is a failure.
    fn live_url() -> Option<String> {
        match env::var("SCHEMA_REGISTRY_TEST_URL") {
            Ok(url) if !url.is_empty() => Some(url),
            _ => {
                assert!(
                    !env::var("RUSTSTREAM_REQUIRE_LIVE").is_ok_and(|flag| !flag.is_empty()),
                    "RUSTSTREAM_REQUIRE_LIVE is set, so this test must run, but \
                     SCHEMA_REGISTRY_TEST_URL is unset or empty",
                );
                None
            }
        }
    }
}
