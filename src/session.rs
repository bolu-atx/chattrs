//! chattrs — the session settings that `env` and `exec` turn into variables.

use crate::cli;
use crate::commands::write;
use crate::model;
use crate::output;
use crate::project::ProjectSource;
use crate::settings::Settings;
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
    /// The resolved project and where it came from.
    pub(crate) project: (String, ProjectSource),
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
    let (project, source) = s.project;
    let project = (source != ProjectSource::Default).then_some(project);
    let optional = [
        ("CHATTRS_PROJECT", project),
        ("CHATTRS_LEAD", s.lead),
        ("CHATTRS_TAGS", s.tags),
        ("CHATTRS_TOKEN", s.token.filter(|_| with_token)),
    ];
    pairs.extend(optional.into_iter().filter_map(|(name, value)| Some((name, value?))));
    Ok((pairs, source.word()))
}

pub(crate) fn cmd_env(c: cli::EnvArgs, settings: &Settings) -> Result<(), RunError> {
    let cwd = std::env::current_dir().unwrap_or_default();
    let session = Session {
        address: c.address,
        project: settings.project(c.project.as_deref(), &cwd),
        lead: c.lead,
        server: c.server,
        tags: c.tags,
        token: c.token,
    };
    let (pairs, source) = session_pairs(session, c.with_token)?;
    write(&(output::export_lines(&pairs) + &format!("# project source: {source}\n")))
}
