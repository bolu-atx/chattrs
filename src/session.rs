//! chattrs — the session settings that `env` and `exec` turn into variables.

use crate::cli;
use crate::commands::write;
use crate::model;
use crate::output;
use crate::project;
use crate::RunError;

/// A fresh session id: 16 random bytes as 32 hex digits. Each
/// `RandomState` is seeded from the OS, so no crate is needed.
pub(crate) fn new_session_id() -> String {
    use std::hash::{BuildHasher as _, Hasher as _};
    let word = || std::collections::hash_map::RandomState::new().build_hasher().finish();
    format!("{:016x}{:016x}", word(), word())
}

/// The session settings that `env` and `exec` both turn into variables.
pub(crate) struct Session {
    pub(crate) address: String,
    pub(crate) project: Option<String>,
    pub(crate) lead: Option<String>,
    pub(crate) server: String,
    pub(crate) tags: Option<String>,
    pub(crate) token: Option<String>,
}

pub(crate) type SessionPairs = (Vec<(&'static str, String)>, &'static str);

/// The `(name, value)` pairs of one session, and the word for the project
/// source. The token is in the pairs only with `with_token`.
pub(crate) fn session_pairs(s: Session, with_token: bool) -> Result<SessionPairs, RunError> {
    if !model::is_valid_address(&s.address) {
        return Err(RunError::Message(format!("`{}` is not a user@host address", s.address)));
    }
    let mut pairs = vec![
        ("CHATTRS_SERVER", s.server),
        ("CHATTRS_ADDRESS", s.address),
        ("CHATTRS_SESSION", new_session_id()),
    ];
    let (explicit, from_config) = project::split_applied(s.project);
    let cwd = std::env::current_dir().unwrap_or_default();
    let (project, source) = project::resolve(explicit.as_deref(), from_config.as_deref(), &cwd);
    let project = (source != project::ProjectSource::Default).then_some(project);
    let optional = [
        ("CHATTRS_PROJECT", project),
        ("CHATTRS_LEAD", s.lead),
        ("CHATTRS_TAGS", s.tags),
        ("CHATTRS_TOKEN", s.token.filter(|_| with_token)),
    ];
    pairs.extend(optional.into_iter().filter_map(|(name, value)| Some((name, value?))));
    Ok((pairs, source.word()))
}

pub(crate) fn cmd_env(c: cli::EnvArgs) -> Result<(), RunError> {
    let session = Session {
        address: c.address,
        project: c.project,
        lead: c.lead,
        server: c.server,
        tags: c.tags,
        token: c.token,
    };
    let (pairs, source) = session_pairs(session, c.with_token)?;
    write(&(output::export_lines(&pairs) + &format!("# project source: {source}\n")))
}
