//! The compatibility levels a registry holds a subject to.

use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// The compatibility level a registry holds a subject to.
///
/// A `_TRANSITIVE` level checks a new schema against every version of the subject; the plain
/// level checks it against the latest one only.
///
/// # Examples
///
/// ```
/// use ruststream_rdkafka::schema_registry::admin::CompatibilityLevel;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // What `subject.toml` says, parsed the way the registry spells it.
///     let level: CompatibilityLevel = "BACKWARD_TRANSITIVE".parse()?;
///     println!("new schemas must read every version: {}", level.checks_backward());
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CompatibilityLevel {
    /// No check.
    None,
    /// A reader of the new schema reads data written with the latest version.
    Backward,
    /// A reader of the new schema reads data written with every version.
    BackwardTransitive,
    /// A reader of the latest version reads data written with the new schema.
    Forward,
    /// A reader of every version reads data written with the new schema.
    ForwardTransitive,
    /// Both [`Backward`](Self::Backward) and [`Forward`](Self::Forward).
    Full,
    /// Both [`BackwardTransitive`](Self::BackwardTransitive) and
    /// [`ForwardTransitive`](Self::ForwardTransitive).
    FullTransitive,
}

impl CompatibilityLevel {
    /// The name the registry's API uses (`BACKWARD_TRANSITIVE`, ..).
    #[must_use]
    pub const fn as_api(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Backward => "BACKWARD",
            Self::BackwardTransitive => "BACKWARD_TRANSITIVE",
            Self::Forward => "FORWARD",
            Self::ForwardTransitive => "FORWARD_TRANSITIVE",
            Self::Full => "FULL",
            Self::FullTransitive => "FULL_TRANSITIVE",
        }
    }

    /// Whether a reader of the new schema must read what the older versions wrote.
    #[must_use]
    pub const fn checks_backward(self) -> bool {
        matches!(
            self,
            Self::Backward | Self::BackwardTransitive | Self::Full | Self::FullTransitive
        )
    }

    /// Whether a reader of the older versions must read what the new schema writes.
    #[must_use]
    pub const fn checks_forward(self) -> bool {
        matches!(
            self,
            Self::Forward | Self::ForwardTransitive | Self::Full | Self::FullTransitive
        )
    }

    /// Whether the check covers every version rather than the latest one.
    #[must_use]
    pub const fn is_transitive(self) -> bool {
        matches!(
            self,
            Self::BackwardTransitive | Self::ForwardTransitive | Self::FullTransitive
        )
    }
}

impl fmt::Display for CompatibilityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_api())
    }
}

/// A compatibility level name the registry does not know.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error(
    "`{0}` is not a compatibility level; the registry knows NONE, BACKWARD, \
     BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL and FULL_TRANSITIVE"
)]
pub struct ParseCompatibilityLevelError(String);

impl FromStr for CompatibilityLevel {
    type Err = ParseCompatibilityLevelError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Ok(match name {
            "NONE" => Self::None,
            "BACKWARD" => Self::Backward,
            "BACKWARD_TRANSITIVE" => Self::BackwardTransitive,
            "FORWARD" => Self::Forward,
            "FORWARD_TRANSITIVE" => Self::ForwardTransitive,
            "FULL" => Self::Full,
            "FULL_TRANSITIVE" => Self::FullTransitive,
            other => return Err(ParseCompatibilityLevelError(other.to_owned())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_parse_from_their_api_names_and_back() {
        for level in [
            CompatibilityLevel::None,
            CompatibilityLevel::Backward,
            CompatibilityLevel::BackwardTransitive,
            CompatibilityLevel::Forward,
            CompatibilityLevel::ForwardTransitive,
            CompatibilityLevel::Full,
            CompatibilityLevel::FullTransitive,
        ] {
            assert_eq!(level.as_api().parse::<CompatibilityLevel>(), Ok(level));
        }
        let err = "backward"
            .parse::<CompatibilityLevel>()
            .expect_err("lower case");
        assert!(
            err.to_string()
                .contains("`backward` is not a compatibility level")
        );
    }

    #[test]
    fn levels_name_their_directions() {
        assert!(CompatibilityLevel::Backward.checks_backward());
        assert!(!CompatibilityLevel::Backward.checks_forward());
        assert!(!CompatibilityLevel::Backward.is_transitive());
        assert!(CompatibilityLevel::ForwardTransitive.checks_forward());
        assert!(!CompatibilityLevel::ForwardTransitive.checks_backward());
        assert!(CompatibilityLevel::FullTransitive.checks_backward());
        assert!(CompatibilityLevel::FullTransitive.checks_forward());
        assert!(CompatibilityLevel::FullTransitive.is_transitive());
        assert!(!CompatibilityLevel::None.checks_backward());
        assert!(!CompatibilityLevel::None.checks_forward());
    }
}
