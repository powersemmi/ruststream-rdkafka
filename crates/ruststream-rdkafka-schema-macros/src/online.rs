//! The online drift check and the rebuild triggers. A stub until the entry lands.

use proc_macro2::{Span, TokenStream};

use crate::migrations::Subject;

/// The tokens that check `subjects` against the registry and rebuild the crate when a switch or
/// the lock changes.
// The online check fails the build on drift, so the stub keeps its fallible signature.
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn guard(subjects: &[&Subject], span: Span) -> syn::Result<TokenStream> {
    let _ = (subjects, span);
    Ok(TokenStream::new())
}
