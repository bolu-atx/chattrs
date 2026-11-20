//! chattrs — the token table (SEC-4): one entry per token, each naming a
//! project, an address pattern, and a role.
//!
//! The broker reads the table from a TOML file at start. The parser accepts
//! a small subset of TOML (see `Tokens::parse`), so the crate needs no TOML
//! dependency. This module is pure apart from `Tokens::load`, which reads
//! the file and checks its mode.

use std::path::Path;

use crate::model;

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
    project: String,
    addr: String,
    pub role: Role,
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
fn is_valid_pattern(addr: &str) -> bool {
    let allowed =
        |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'@' | b'*');
    !addr.is_empty()
        && addr.len() <= model::MAX_ADDRESS_BYTES
        && addr.matches('@').count() == 1
        && addr.bytes().all(allowed)
}

impl Draft {
    /// Turn a finished block into an entry, or say which line is wrong.
    fn finish(self) -> Result<Entry, String> {
        let line = self.line;
        let field = |value: Option<String>, name: &str| {
            value.ok_or_else(|| format!("line {line}: entry has no `{name}`"))
        };
        let token = field(self.token, "token")?;
        let project = field(self.project, "project")?;
        let addr = field(self.addr, "addr")?;
        let role_text = field(self.role, "role")?;
        let role = Role::parse(&role_text)
            .ok_or_else(|| format!("line {line}: role must be lead, agent, or observer"))?;
        if project == model::ALL_PROJECTS {
            if role != Role::Observer {
                return Err(format!("line {line}: project `*` is only for the observer role"));
            }
        } else if !model::is_valid_topic(&project) {
            return Err(format!("line {line}: project is not a valid project name"));
        }
        if !is_valid_pattern(&addr) {
            return Err(format!("line {line}: addr is not an address or a `*` pattern"));
        }
        Ok(Entry { token, project, addr, role })
    }

    fn set(&mut self, line: usize, key: &str, value: String) -> Result<(), String> {
        let slot = match key {
            "token" => &mut self.token,
            "project" => &mut self.project,
            "addr" => &mut self.addr,
            "role" => &mut self.role,
            _ => return Err(format!("line {line}: unknown key `{key}`")),
        };
        if slot.is_some() {
            return Err(format!("line {line}: duplicate key `{key}`"));
        }
        if key == "token" && value.is_empty() {
            return Err(format!("line {line}: token is empty"));
        }
        *slot = Some(value);
        Ok(())
    }
}

/// Parse `key = "value"` with an optional `# comment` after the closing
/// quote. The value has no escape sequences.
fn parse_pair(text: &str) -> Option<(&str, String)> {
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
    pub fn load(path: &Path) -> Result<Tokens, String> {
        let shown = path.display();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta = std::fs::metadata(path).map_err(|e| format!("tokens file {shown}: {e}"))?;
            let mode = meta.permissions().mode() & 0o777;
            if mode & 0o177 != 0 {
                return Err(format!(
                    "tokens file {shown} has mode {mode:o}, wider than 600; run chmod 600"
                ));
            }
        }
        let text =
            std::fs::read_to_string(path).map_err(|e| format!("tokens file {shown}: {e}"))?;
        Tokens::parse(&text).map_err(|e| format!("tokens file {shown}: {e}"))
    }

    /// Parse the table. The accepted subset of TOML is: blank lines, `#`
    /// comment lines, `[[token]]` headers, and `key = "value"` lines with
    /// an optional trailing `# comment`. Keys are `token`, `project`,
    /// `addr`, `role`; each appears once per entry. Values are
    /// double-quoted strings without escapes. Nothing else is accepted.
    pub fn parse(text: &str) -> Result<Tokens, String> {
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
                return Err(format!("line {line}: expected [[token]] or key = \"value\""));
            };
            let Some(current) = draft.as_mut() else {
                return Err(format!("line {line}: key before the first [[token]]"));
            };
            current.set(line, key, value)?;
        }
        entries.extend(draft.map(Draft::finish).transpose()?);
        if entries.is_empty() {
            return Err("no [[token]] entries".to_string());
        }
        Tokens::check_unique(&entries)?;
        Ok(Tokens { entries })
    }

    /// Two entries with one token would make the match ambiguous.
    fn check_unique(entries: &[Entry]) -> Result<(), String> {
        for (i, entry) in entries.iter().enumerate() {
            if entries[..i].iter().any(|other| other.token == entry.token) {
                return Err(format!("entry {} repeats an earlier token", i + 1));
            }
        }
        Ok(())
    }

    /// The entry for a presented token. The compare runs against every
    /// entry, in constant time each, and never stops at a match.
    pub fn find(&self, presented: Option<&str>) -> Option<&Entry> {
        let got = presented.unwrap_or_default().as_bytes();
        let mut found = None;
        for entry in &self.entries {
            if ct_eq(got, entry.token.as_bytes()) {
                found = Some(entry);
            }
        }
        found
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
        if entry.project != model::ALL_PROJECTS && entry.project != project {
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
        if !observer && entry.role != Role::Lead && self.is_lead_address(project, address) {
            return Err("that address belongs to a lead token");
        }
        Ok(())
    }

    /// True when some lead entry of `project` names `address` exactly.
    fn is_lead_address(&self, project: &str, address: &str) -> bool {
        self.entries.iter().any(|entry| {
            entry.role == Role::Lead && entry.project == project && entry.addr == address
        })
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
        assert_eq!(tokens.find(Some("t-agent")).unwrap().role, Role::Agent);
        assert!(tokens.find(Some("t-agen")).is_none());
        assert!(tokens.find(None).is_none());
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
            ("[[token]]\ntoken = \"a\"\n", "line 1: entry has no `project`"),
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
            let err = Tokens::parse(text).unwrap_err();
            assert!(err.contains(want), "{text:?} gave {err:?}, wanted {want:?}");
        }
        let dup = "[[token]]\ntoken=\"a\"\nproject=\"p\"\naddr=\"a@h\"\nrole=\"agent\"\n";
        let err = Tokens::parse(&format!("{dup}{dup}")).unwrap_err();
        assert!(err.contains("repeats an earlier token"), "{err}");
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
        let lead = tokens.find(Some("t-lead")).unwrap();
        let agent = tokens.find(Some("t-agent")).unwrap();
        let obs = tokens.find(Some("t-obs")).unwrap();
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
        let err = Tokens::load(&path).unwrap_err();
        assert!(err.contains("chmod 600") && !err.contains("t-lead"), "{err}");
    }
}
