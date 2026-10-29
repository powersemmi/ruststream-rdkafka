//! The registry as the tool sees it: the connection, and what the registry holds for a subject
//! of the catalog.

use anyhow::{Context, Result, bail};
use percent_encoding::percent_decode_str;
use ruststream_rdkafka::SchemaRegistry;
use ruststream_rdkafka::schema_registry::admin::{
    CompatibilityVerdict, RegistryAdmin, SchemaDraft, SchemaReference,
};
use url::Url;

use crate::RegistryArgs;
use crate::catalog::{Migration, Subject};

/// At most this many combinations of reference versions are tried when looking a migration up
/// by its content: an older migration was registered against older versions of what it imports.
const REFERENCE_COMBINATIONS: usize = 32;

/// A registry the tool talks to.
pub(crate) struct Registry {
    admin: RegistryAdmin,
    /// The URL without its user part, for messages.
    shown: String,
}

/// Where a migration sits in the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Found {
    pub(crate) version: u32,
    pub(crate) id: u32,
}

/// What the registry holds for one subject: its versions, and per migration (in number order)
/// the version whose content it is.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Remote {
    pub(crate) versions: Vec<u32>,
    pub(crate) found: Vec<Option<Found>>,
}

/// The registry the arguments name, or `None` to work offline.
pub(crate) fn connect(args: &RegistryArgs) -> Result<Option<Registry>> {
    if args.offline == Some(true) {
        return Ok(None);
    }
    let Some(raw) = args.url.as_deref().filter(|url| !url.is_empty()) else {
        return Ok(None);
    };
    // The URL is never echoed: its user part may hold the password.
    let Ok(mut url) = Url::parse(raw) else {
        bail!("the registry URL does not parse");
    };
    let user = percent_decode_str(url.username())
        .decode_utf8_lossy()
        .into_owned();
    let password = url.password().map(|password| {
        percent_decode_str(password)
            .decode_utf8_lossy()
            .into_owned()
    });
    let _ = url.set_username("");
    let _ = url.set_password(None);
    let shown = url.as_str().trim_end_matches('/').to_owned();
    let mut registry = SchemaRegistry::new(shown.clone());
    if let Some(token) = &args.token {
        registry = registry.bearer_token(token.clone());
    } else if let Some(user) = args
        .user
        .clone()
        .or_else(|| (!user.is_empty()).then_some(user))
    {
        let password = args.password.clone().or(password).unwrap_or_default();
        registry = registry.basic_auth(user, password);
    }
    let admin = registry
        .admin()
        .context("the shipped registry client has an administrative surface")?;
    Ok(Some(Registry { admin, shown }))
}

impl Registry {
    pub(crate) fn shown(&self) -> &str {
        &self.shown
    }

    pub(crate) fn admin(&self) -> &RegistryAdmin {
        &self.admin
    }

    /// What the registry holds for `subject`.
    pub(crate) async fn remote(&self, subject: &Subject) -> Result<Remote> {
        let versions = self
            .admin
            .versions(&subject.name)
            .await
            .with_context(|| format!("listing the versions of subject `{}`", subject.name))?;
        let mut found = Vec::with_capacity(subject.migrations.len());
        if versions.is_empty() {
            found.resize(subject.migrations.len(), None);
        } else {
            let candidates = self.reference_candidates(subject).await?;
            for migration in &subject.migrations {
                found.push(self.find(subject, migration, &candidates).await?);
            }
        }
        Ok(Remote { versions, found })
    }

    /// The version whose content is `migration`, trying each set of reference versions.
    async fn find(
        &self,
        subject: &Subject,
        migration: &Migration,
        candidates: &[Vec<SchemaReference>],
    ) -> Result<Option<Found>> {
        for references in candidates {
            let draft = draft(subject, migration, references.clone());
            let hit = self
                .admin
                .find(&subject.name, &draft)
                .await
                .with_context(|| {
                    format!(
                        "looking migration `{}` of subject `{}` up in the registry",
                        migration.file, subject.name
                    )
                })?;
            if let Some(version) = hit {
                return Ok(Some(Found {
                    version: version.version(),
                    id: version.id(),
                }));
            }
        }
        Ok(None)
    }

    /// The reference sets to look a migration up with: the latest version of every referenced
    /// subject first, then older combinations, newest first.
    pub(crate) async fn reference_candidates(
        &self,
        subject: &Subject,
    ) -> Result<Vec<Vec<SchemaReference>>> {
        let mut per_reference = Vec::new();
        for reference in &subject.settings.references {
            let mut versions =
                self.admin
                    .versions(&reference.subject)
                    .await
                    .with_context(|| {
                        format!("listing the versions of subject `{}`", reference.subject)
                    })?;
            versions.reverse();
            if versions.is_empty() {
                // Nothing to resolve against yet: the referenced subject registers first.
                return Ok(vec![Vec::new()]);
            }
            per_reference.push(
                versions
                    .into_iter()
                    .map(|version| SchemaReference {
                        name: reference.name.clone(),
                        subject: reference.subject.clone(),
                        version,
                    })
                    .collect::<Vec<_>>(),
            );
        }
        let mut combinations: Vec<Vec<SchemaReference>> = vec![Vec::new()];
        for options in per_reference {
            let mut next = Vec::new();
            for prefix in &combinations {
                for option in &options {
                    if next.len() == REFERENCE_COMBINATIONS {
                        break;
                    }
                    let mut combination = prefix.clone();
                    combination.push(option.clone());
                    next.push(combination);
                }
            }
            combinations = next;
        }
        Ok(combinations)
    }

    /// The registry's compatibility verdict on `draft`, its messages printed as they come.
    pub(crate) async fn verdict(
        &self,
        subject: &Subject,
        file: &str,
        draft: &SchemaDraft,
    ) -> Result<CompatibilityVerdict> {
        let verdict = self
            .admin
            .check_compatibility(&subject.name, draft)
            .await
            .with_context(|| {
                format!(
                    "asking the registry whether migration `{file}` of subject `{}` is compatible",
                    subject.name
                )
            })?;
        for message in verdict.messages() {
            eprintln!("  registry: {message}");
        }
        Ok(verdict)
    }
}

/// The draft of `migration` with `references`.
pub(crate) fn draft(
    subject: &Subject,
    migration: &Migration,
    references: Vec<SchemaReference>,
) -> SchemaDraft {
    let mut draft = SchemaDraft::new(subject.format().schema_type(), migration.content.clone());
    for reference in references {
        draft = draft.reference(reference);
    }
    draft
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(url: &str) -> RegistryArgs {
        RegistryArgs {
            url: Some(url.to_owned()),
            user: None,
            password: None,
            token: None,
            offline: None,
        }
    }

    #[test]
    fn credentials_leave_the_url_and_never_reach_what_is_shown() {
        let registry = connect(&args("https://svc:p%40ss@registry.acme:8081/"))
            .expect("connect")
            .expect("online");
        assert_eq!(registry.shown(), "https://registry.acme:8081");
        assert!(!format!("{:?}", registry.admin).contains("p@ss"));
    }

    #[test]
    fn no_url_or_the_offline_switch_is_offline() {
        let mut offline = args("http://registry:8081");
        offline.offline = Some(true);
        assert!(connect(&offline).expect("offline").is_none());
        offline.url = None;
        offline.offline = None;
        assert!(connect(&offline).expect("no url").is_none());
    }

    #[test]
    fn a_url_that_does_not_parse_is_not_echoed() {
        let err = connect(&args("://svc:secret@registry"))
            .err()
            .expect("bad")
            .to_string();
        assert_eq!(err, "the registry URL does not parse");
    }
}
