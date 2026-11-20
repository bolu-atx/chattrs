//! chattrs — the token table (SEC-4): one entry per token, each naming an
//! address pattern and a role, and optionally a project.
//!
//! The broker reads the table from a TOML file at start. The parser accepts
//! a small subset of TOML (see `Tokens::parse`), so the crate needs no TOML
//! dependency. This module is pure apart from `Tokens::load`, which reads
//! the file and checks its mode.

use std::path::{Path, PathBuf};

use crate::model;

/// Why the token table could not be loaded. `Display` gives the text the
/// broker prints at start. Line numbers count from 1.
#[derive(Debug)]
pub enum TokensError {
    /// The file could not be read.
    Read {
        file: PathBuf,
        source: std::io::Error,
    },
    /// The file is wider than 0600.
    Mode {
        file: PathBuf,
        mode: u32,
    },
    /// The file has a bad table; `source` says where.
    File {
        file: PathBuf,
        source: Box<TokensError>,
    },
    /// An entry lacks a required key. `line` is the entry header.
    MissingKey {
        line: usize,
        key: &'static str,
    },
    BadRole {
        line: usize,
    },
    /// `project = "*"` on a role other than observer.
    WildcardProject {
        line: usize,
    },
    BadProject {
        line: usize,
    },
    BadAddr {
        line: usize,
    },
    UnknownKey {
        line: usize,
        key: String,
    },
    DuplicateKey {
        line: usize,
        key: String,
    },
    EmptyToken {
        line: usize,
    },
    /// A line is neither `[[token]]` nor `key = "value"`.
    Syntax {
        line: usize,
    },
    KeyBeforeHeader {
        line: usize,
    },
    NoEntries,
    /// Entry number `entry` (from 1) repeats a token; `project` is the
    /// project that the earlier copy already covers.
    Duplicate {
        entry: usize,
        project: Option<String>,
    },
}

impl std::fmt::Display for TokensError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokensError::Read { file, source } => {
                write!(f, "tokens file {}: {source}", file.display())
            }
            TokensError::Mode { file, mode } => write!(
                f,
                "tokens file {} has mode {mode:o}, wider than 600; run chmod 600",
                file.display()
            ),
            TokensError::File { file, source } => {
                write!(f, "tokens file {}: {source}", file.display())
            }
            TokensError::MissingKey { line, key } => {
                write!(f, "line {line}: entry has no `{key}`")
            }
            TokensError::BadRole { line } => {
                write!(f, "line {line}: role must be lead, agent, or observer")
            }
            TokensError::WildcardProject { line } => {
                write!(f, "line {line}: project `*` is only for the observer role")
            }
            TokensError::BadProject { line } => {
                write!(f, "line {line}: project is not a valid project name")
            }
            TokensError::BadAddr { line } => {
                write!(f, "line {line}: addr is not an address or a `*` pattern")
            }
            TokensError::UnknownKey { line, key } => write!(f, "line {line}: unknown key `{key}`"),
            TokensError::DuplicateKey { line, key } => {
                write!(f, "line {line}: duplicate key `{key}`")
            }
            TokensError::EmptyToken { line } => write!(f, "line {line}: token is empty"),
            TokensError::Syntax { line } => {
                write!(f, "line {line}: expected [[token]] or key = \"value\"")
            }
            TokensError::KeyBeforeHeader { line } => {
                write!(f, "line {line}: key before the first [[token]]")
            }
            TokensError::NoEntries => write!(f, "no [[token]] entries"),
            TokensError::Duplicate { entry, project: Some(project) } => {
                write!(f, "entry {entry} repeats a token that already covers project {project}")
            }
            TokensError::Duplicate { entry, project: None } => {
                write!(f, "entry {entry} repeats a token; a token with no project must appear once")
            }
        }
    }
}

impl std::error::Error for TokensError {}

/// What a token may do on the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Lead,
    Agent,
    Observer,
}

impl Role {
    /// The role name as written in the file and in `from_role`.
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Lead => "lead",
            Role::Agent => "agent",
            Role::Observer => "observer",
        }
    }

    fn parse(value: &str) -> Option<Role> {
        match value {
            "lead" => Some(Role::Lead),
            "agent" => Some(Role::Agent),
            "observer" => Some(Role::Observer),
            _ => None,
        }
    }
}

/// One validated row of the token table.
#[derive(Debug)]
pub struct Entry {
    token: String,
    /// `None` means the token works in every project.
    project: Option<String>,
    addr: String,
    pub role: Role,
}

/// Why `Tokens::find` found no entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Miss {
    /// No entry has this token.
    BadToken,
    /// Some entry has this token, but every one names other projects.
    WrongProject,
}

/// The token table. It is never empty.
#[derive(Debug)]
pub struct Tokens {
    entries: Vec<Entry>,
}

/// One `[[token]]` block while parsing: the header line and the values.
#[derive(Default)]
struct Draft {
    line: usize,
    token: Option<String>,
    project: Option<String>,
    addr: Option<String>,
    role: Option<String>,
}

/// Compare two byte strings without stopping at the first difference. A
/// length difference counts too, and the loop always covers the longer one.
pub(crate) fn ct_eq(got: &[u8], want: &[u8]) -> bool {
    let longest = got.len().max(want.len());
    let mut diff = got.len() ^ want.len();
    for i in 0..longest {
        let pair = got.get(i).copied().unwrap_or(0) ^ want.get(i).copied().unwrap_or(0);
        diff |= pair as usize;
    }
    diff == 0
}

/// Match `text` against `pattern`, where `*` matches any run of bytes
/// (including none) and every other byte matches itself.
pub(crate) fn glob_matches(pattern: &str, text: &str) -> bool {
    let (pattern, text) = (pattern.as_bytes(), text.as_bytes());
    let (mut p, mut t) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        if p < pattern.len() && pattern[p] == b'*' {
            star = Some((p, t));
            p += 1;
        } else if p < pattern.len() && pattern[p] == text[t] {
            p += 1;
            t += 1;
        } else if let Some((star_p, star_t)) = star {
            p = star_p + 1;
            t = star_t + 1;
            star = Some((star_p, star_t + 1));
        } else {
            return false;
        }
    }
    pattern[p..].iter().all(|byte| *byte == b'*')
}

/// A pattern is an address shape with `*` allowed anywhere.
pub(crate) fn is_valid_pattern(addr: &str) -> bool {
    let allowed =
        |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'@' | b'*');
    !addr.is_empty()
        && addr.len() <= model::MAX_ADDRESS_BYTES
        && addr.matches('@').count() == 1
        && addr.bytes().all(allowed)
}

impl Draft {
    /// Turn a finished block into an entry, or say which line is wrong.
    fn finish(self) -> Result<Entry, TokensError> {
        let line = self.line;
        let field = |value: Option<String>, key: &'static str| {
            value.ok_or(TokensError::MissingKey { line, key })
        };
        let token = field(self.token, "token")?;
        let addr = field(self.addr, "addr")?;
        let role_text = field(self.role, "role")?;
        let role = Role::parse(&role_text).ok_or(TokensError::BadRole { line })?;
        // `*` is the old spelling of "no project", for observers only.
        let project = match self.project {
            Some(project) if project == model::ALL_PROJECTS => {
                if role != Role::Observer {
                    return Err(TokensError::WildcardProject { line });
                }
                None
            }
            Some(project) if !model::is_valid_topic(&project) => {
                return Err(TokensError::BadProject { line });
            }
            other => other,
        };
        if !is_valid_pattern(&addr) {
            return Err(TokensError::BadAddr { line });
        }
        Ok(Entry { token, project, addr, role })
    }

    fn set(&mut self, line: usize, key: &str, value: String) -> Result<(), TokensError> {
        let slot = match key {
            "token" => &mut self.token,
            "project" => &mut self.project,
            "addr" => &mut self.addr,
            "role" => &mut self.role,
            _ => return Err(TokensError::UnknownKey { line, key: key.to_string() }),
        };
        if slot.is_some() {
            return Err(TokensError::DuplicateKey { line, key: key.to_string() });
        }
        if key == "token" && value.is_empty() {
            return Err(TokensError::EmptyToken { line });
        }
        *slot = Some(value);
        Ok(())
    }
}

/// Parse `key = "value"` with an optional `# comment` after the closing
/// quote. The value has no escape sequences.
pub(crate) fn parse_pair(text: &str) -> Option<(&str, String)> {
    let (key, rest) = text.split_once('=')?;
    let rest = rest.trim().strip_prefix('"')?;
    let (value, tail) = rest.split_once('"')?;
    let tail = tail.trim();
    if value.contains('\\') || !(tail.is_empty() || tail.starts_with('#')) {
        return None;
    }
    Some((key.trim(), value.to_string()))
}

/// True for a `[[token]]` header line, with an optional trailing comment.
fn is_header(text: &str) -> bool {
    text.strip_prefix("[[token]]").is_some_and(|tail| {
        let tail = tail.trim();
        tail.is_empty() || tail.starts_with('#')
    })
}

impl Tokens {
    /// Read and parse the file at `path`. On unix, a file with any group
    /// or other permission bit set (wider than 0600) is refused.
    pub fn load(path: &Path) -> Result<Tokens, TokensError> {
        let file = || path.to_path_buf();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta = std::fs::metadata(path)
                .map_err(|source| TokensError::Read { file: file(), source })?;
            let mode = meta.permissions().mode() & 0o777;
            if mode & 0o177 != 0 {
                return Err(TokensError::Mode { file: file(), mode });
            }
        }
        let text = std::fs::read_to_string(path)
            .map_err(|source| TokensError::Read { file: file(), source })?;
        Tokens::parse(&text)
            .map_err(|source| TokensError::File { file: file(), source: Box::new(source) })
    }

    /// Parse the table. The accepted subset of TOML is: blank lines, `#`
    /// comment lines, `[[token]]` headers, and `key = "value"` lines with
    /// an optional trailing `# comment`. Keys are `token`, `project`,
    /// `addr`, `role`; each appears once per entry. Values are
    /// double-quoted strings without escapes. Nothing else is accepted.
    pub fn parse(text: &str) -> Result<Tokens, TokensError> {
        let mut entries: Vec<Entry> = Vec::new();
        let mut draft: Option<Draft> = None;
        for (index, raw) in text.lines().enumerate() {
            let line = index + 1;
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if is_header(trimmed) {
                entries.extend(draft.take().map(Draft::finish).transpose()?);
                draft = Some(Draft { line, ..Draft::default() });
                continue;
            }
            let Some((key, value)) = parse_pair(trimmed) else {
                return Err(TokensError::Syntax { line });
            };
            let Some(current) = draft.as_mut() else {
                return Err(TokensError::KeyBeforeHeader { line });
            };
            current.set(line, key, value)?;
        }
        entries.extend(draft.map(Draft::finish).transpose()?);
        if entries.is_empty() {
            return Err(TokensError::NoEntries);
        }
        Tokens::check_unique(&entries)?;
        Ok(Tokens { entries })
    }

    /// The same token may appear again only when every copy names a
    /// distinct project (one lead token for several projects). A copy with
    /// no project overlaps every other copy.
    fn check_unique(entries: &[Entry]) -> Result<(), TokensError> {
        for (i, entry) in entries.iter().enumerate() {
            let clash = entries[..i].iter().any(|other| {
                other.token == entry.token
                    && (other.project.is_none()
                        || entry.project.is_none()
                        || other.project == entry.project)
            });
            if clash {
                return Err(TokensError::Duplicate {
                    entry: i + 1,
                    project: entry.project.clone(),
                });
            }
        }
        Ok(())
    }

    /// The entry for a presented token and the hello project. The compare
    /// runs against every entry, in constant time each, and never stops at
    /// a match. An entry for the project wins over one with no project.
    pub fn find(&self, presented: Option<&str>, project: &str) -> Result<&Entry, Miss> {
        let got = presented.unwrap_or_default().as_bytes();
        let (mut exact, mut any, mut token_known) = (None, None, false);
        for entry in &self.entries {
            if !ct_eq(got, entry.token.as_bytes()) {
                continue;
            }
            token_known = true;
            match entry.project.as_deref() {
                Some(own) if own == project => exact = Some(entry),
                Some(_) => {}
                None => any = Some(entry),
            }
        }
        match (exact.or(any), token_known) {
            (Some(entry), _) => Ok(entry),
            (None, true) => Err(Miss::WrongProject),
            (None, false) => Err(Miss::BadToken),
        }
    }

    /// Decide whether `entry` may open a connection for `address` in
    /// `project`. The error is a human message for a `forbidden` frame.
    pub fn authorize(
        &self,
        entry: &Entry,
        project: &str,
        address: &str,
        observer: bool,
    ) -> Result<(), &'static str> {
        if entry.project.as_deref().is_some_and(|own| own != project) {
            return Err("token is not valid for this project");
        }
        if !glob_matches(&entry.addr, address) {
            return Err("token is not valid for this address");
        }
        match (entry.role, observer) {
            (Role::Observer, false) => return Err("an observer token needs an observer hello"),
            (Role::Agent, true) => return Err("an agent token cannot open an observer hello"),
            _ => {}
        }
        if !observer && entry.role != Role::Lead && self.is_lead_address(address) {
            return Err("that address belongs to a lead token");
        }
        Ok(())
    }

    /// True when some lead entry names `address` exactly. The project of
    /// the lead entry does not matter: a lead address is reserved across
    /// the whole broker.
    fn is_lead_address(&self, address: &str) -> bool {
        self.entries.iter().any(|entry| entry.role == Role::Lead && entry.addr == address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = r##"
# The lead.
[[token]]
token = "t-lead"
project = "alpha"
addr = "lead@box1"
role = "lead"

[[token]]   # A worker on box1.
token = "t-agent"   # Made with openssl rand -hex 32.
project = "alpha"
addr = "*@box1"
role = "agent"

[[token]]
token = "t-obs"
project = "*"
addr = "*@*"
role = "observer"
"##;

    #[test]
    fn parses_a_good_file() {
        let tokens = Tokens::parse(GOOD).unwrap();
        assert_eq!(tokens.entries.len(), 3);
        assert_eq!(tokens.find(Some("t-agent"), "alpha").unwrap().role, Role::Agent);
        assert!(tokens.find(Some("t-agen"), "alpha").unwrap_err() == Miss::BadToken);
        assert!(tokens.find(None, "alpha").unwrap_err() == Miss::BadToken);
    }

    #[test]
    fn bad_entries_are_refused_with_a_line_number() {
        let cases = [
            ("", "no [[token]] entries"),
            ("token = \"x\"\n", "line 1: key before"),
            ("[[token]]\ntoken = x\n", "line 2: expected"),
            ("[[token]]\ncolor = \"red\"\n", "line 2: unknown key"),
            ("[[token]]\ntoken = \"a\"\ntoken = \"b\"\n", "line 3: duplicate key"),
            ("[[token]]\ntoken = \"\"\n", "line 2: token is empty"),
            ("[[token]]\ntoken = \"a\"\n", "line 1: entry has no `addr`"),
            (
                "[[token]]\ntoken=\"a\"\nproject=\"p\"\naddr=\"a@h\"\nrole=\"boss\"\n",
                "line 1: role must be",
            ),
            (
                "\n[[token]]\ntoken=\"a\"\nproject=\"*\"\naddr=\"a@h\"\nrole=\"lead\"\n",
                "line 2: project `*`",
            ),
            (
                "[[token]]\ntoken=\"a\"\nproject=\"p\"\naddr=\"no-at\"\nrole=\"agent\"\n",
                "line 1: addr is not",
            ),
            (
                "[[token]]\ntoken=\"a\"\nproject=\"P x\"\naddr=\"a@h\"\nrole=\"agent\"\n",
                "line 1: project is not",
            ),
            ("[[token]]\ntoken = \"a\\n\"\n", "line 2: expected"),
            ("[[token]]\ntoken = \"a\" junk\n", "line 2: expected"),
        ];
        for (text, want) in cases {
            let err = Tokens::parse(text).unwrap_err().to_string();
            assert!(err.contains(want), "{text:?} gave {err:?}, wanted {want:?}");
        }
        let dup = "[[token]]\ntoken=\"a\"\nproject=\"p\"\naddr=\"a@h\"\nrole=\"agent\"\n";
        let err = Tokens::parse(&format!("{dup}{dup}")).unwrap_err().to_string();
        assert!(err.contains("repeats a token that already covers project p"), "{err}");
    }

    #[test]
    fn one_token_may_cover_several_projects() {
        let block = |token: &str, project: &str, addr: &str, role: &str| {
            format!(
                "[[token]]\ntoken=\"{token}\"\nproject=\"{project}\"\naddr=\"{addr}\"\nrole=\"{role}\"\n"
            )
        };
        let text = format!(
            "{}{}{}",
            block("t-l", "alpha", "lead@h", "lead"),
            block("t-l", "beta", "lead@h", "lead"),
            block("t-a", "alpha", "*@h", "agent"),
        );
        let tokens = Tokens::parse(&text).unwrap();
        assert_eq!(tokens.find(Some("t-l"), "alpha").unwrap().project.as_deref(), Some("alpha"));
        assert_eq!(tokens.find(Some("t-l"), "beta").unwrap().project.as_deref(), Some("beta"));
        assert_eq!(tokens.find(Some("t-l"), "gamma").unwrap_err(), Miss::WrongProject);
        assert_eq!(tokens.find(Some("nope"), "alpha").unwrap_err(), Miss::BadToken);
        for other in
            [block("t-l", "beta", "other@h", "lead"), block("t-l", "beta", "lead@h", "agent")]
        {
            let err = Tokens::parse(&format!("{text}{other}")).unwrap_err().to_string();
            assert!(
                err.contains("entry 4 repeats a token that already covers project beta"),
                "{err}"
            );
        }
        // The same token may have a different role in another project.
        let mixed = format!(
            "{}{}",
            block("t-m", "alpha", "lead@h", "lead"),
            block("t-m", "beta", "*@h", "agent")
        );
        assert!(Tokens::parse(&mixed).is_ok());
    }

    fn block(token: &str, project: Option<&str>, addr: &str, role: &str) -> String {
        let project = project.map(|p| format!("project=\"{p}\"\n")).unwrap_or_default();
        format!("[[token]]\ntoken=\"{token}\"\n{project}addr=\"{addr}\"\nrole=\"{role}\"\n")
    }

    #[test]
    fn an_unscoped_token_works_in_every_project() {
        let tokens = Tokens::parse(&block("t-a", None, "*@h", "agent")).unwrap();
        for project in ["alpha", "beta"] {
            let entry = tokens.find(Some("t-a"), project).unwrap();
            assert!(tokens.authorize(entry, project, "w1@h", false).is_ok());
        }
    }

    #[test]
    fn a_scoped_token_is_refused_outside_its_project() {
        let tokens = Tokens::parse(&block("t-a", Some("alpha"), "*@h", "agent")).unwrap();
        assert!(tokens.find(Some("t-a"), "alpha").is_ok());
        assert_eq!(tokens.find(Some("t-a"), "beta").unwrap_err(), Miss::WrongProject);
    }

    #[test]
    fn an_old_multi_project_file_still_loads() {
        let text = format!(
            "{}{}{}\n[[token]]\ntoken=\"t-obs\"\nproject=\"*\"\naddr=\"tray@*\"\nrole=\"observer\"\n",
            block("t-l", Some("alpha"), "lead@h", "lead"),
            block("t-l", Some("beta"), "lead@h", "lead"),
            block("t-a", Some("alpha"), "*@h", "agent"),
        );
        let tokens = Tokens::parse(&text).unwrap();
        let lead = tokens.find(Some("t-l"), "beta").unwrap();
        assert!(tokens.authorize(lead, "beta", "lead@h", false).is_ok());
        assert_eq!(tokens.find(Some("t-l"), "gamma").unwrap_err(), Miss::WrongProject);
        let obs = tokens.find(Some("t-obs"), "gamma").unwrap();
        assert!(tokens.authorize(obs, "gamma", "tray@x", true).is_ok());
    }

    #[test]
    fn duplicate_tokens_need_distinct_projects() {
        let un = block("t", None, "*@h", "agent");
        let sc = block("t", Some("alpha"), "*@h", "agent");
        let err = Tokens::parse(&format!("{un}{un}")).unwrap_err().to_string();
        assert!(err.contains("entry 2 repeats a token"), "{err}");
        assert!(Tokens::parse(&format!("{un}{sc}")).is_err());
        assert!(Tokens::parse(&format!("{sc}{un}")).is_err());
        assert!(Tokens::parse(&format!("{sc}{sc}")).is_err());
    }

    #[test]
    fn an_unscoped_lead_address_is_protected_in_every_project() {
        let text = format!(
            "{}{}",
            block("t-l", None, "lead@h", "lead"),
            block("t-a", None, "*@h", "agent")
        );
        let tokens = Tokens::parse(&text).unwrap();
        let lead = tokens.find(Some("t-l"), "beta").unwrap();
        let agent = tokens.find(Some("t-a"), "beta").unwrap();
        for project in ["alpha", "beta"] {
            assert!(tokens.authorize(lead, project, "lead@h", false).is_ok());
            assert!(tokens.authorize(agent, project, "lead@h", false).is_err());
            assert!(tokens.authorize(agent, project, "w1@h", false).is_ok());
        }
        assert!(tokens.authorize(agent, "beta", "w1@h", true).is_err());
        assert!(tokens.authorize(lead, "beta", "other@h", false).is_err());
    }

    #[test]
    fn a_lead_address_is_reserved_across_the_broker() {
        let text = format!(
            "{}{}",
            block("t-l", Some("alpha"), "lead@mac", "lead"),
            block("t-a", None, "*@mac", "agent")
        );
        let tokens = Tokens::parse(&text).unwrap();
        let agent = tokens.find(Some("t-a"), "beta").unwrap();
        assert!(tokens.authorize(agent, "alpha", "lead@mac", false).is_err());
        assert!(tokens.authorize(agent, "beta", "lead@mac", false).is_err());
        assert!(tokens.authorize(agent, "beta", "w1@mac", false).is_ok());
        // The scoped lead cannot claim it outside its own project either.
        let lead = tokens.find(Some("t-l"), "alpha").unwrap();
        assert!(tokens.authorize(lead, "alpha", "lead@mac", false).is_ok());
        assert!(tokens.authorize(lead, "beta", "lead@mac", false).is_err());
        assert_eq!(tokens.find(Some("t-l"), "beta").unwrap_err(), Miss::WrongProject);
    }

    #[test]
    fn globs_match_stars_only() {
        assert!(glob_matches("a@h", "a@h"));
        assert!(!glob_matches("a@h", "b@h"));
        assert!(glob_matches("*@box1", "codex@box1"));
        assert!(!glob_matches("*@box1", "codex@box2"));
        assert!(glob_matches("w-*@*", "w-1@anything"));
        assert!(glob_matches("*", "x@y"));
        assert!(glob_matches("a*b*c", "aXbYbZc"));
        assert!(!glob_matches("a*b", "a@c"));
        assert!(!glob_matches("a?@h", "ab@h"));
    }

    #[test]
    fn constant_time_compare_agrees_with_equality() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(ct_eq(b"", b""));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"abcd"));
        assert!(!ct_eq(b"abcd", b"abc"));
        assert!(!ct_eq(b"", b"a"));
    }

    #[test]
    fn authorize_applies_project_address_role_and_lead_rules() {
        let tokens = Tokens::parse(GOOD).unwrap();
        let lead = tokens.find(Some("t-lead"), "alpha").unwrap();
        let agent = tokens.find(Some("t-agent"), "alpha").unwrap();
        let obs = tokens.find(Some("t-obs"), "alpha").unwrap();
        assert!(tokens.authorize(lead, "alpha", "lead@box1", false).is_ok());
        assert!(tokens.authorize(lead, "beta", "lead@box1", false).is_err());
        assert!(tokens.authorize(lead, "alpha", "other@box1", false).is_err());
        assert!(tokens.authorize(agent, "alpha", "w1@box1", false).is_ok());
        assert!(tokens.authorize(agent, "alpha", "w1@box2", false).is_err());
        assert!(tokens.authorize(agent, "alpha", "lead@box1", false).is_err());
        assert!(tokens.authorize(agent, "alpha", "w1@box1", true).is_err());
        assert!(tokens.authorize(obs, "beta", "x@y", true).is_ok());
        assert!(tokens.authorize(obs, "*", "x@y", true).is_ok());
        assert!(tokens.authorize(obs, "beta", "x@y", false).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn load_refuses_a_file_wider_than_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("tokens.toml");
        std::fs::write(&path, GOOD).unwrap();
        let modes = [(0o600, true), (0o400, true), (0o640, false), (0o644, false), (0o700, false)];
        for (mode, ok) in modes {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            let result = Tokens::load(&path);
            assert_eq!(result.is_ok(), ok, "mode {mode:o}: {result:?}");
        }
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let err = Tokens::load(&path).unwrap_err().to_string();
        assert!(err.contains("chmod 600") && !err.contains("t-lead"), "{err}");
    }
}
