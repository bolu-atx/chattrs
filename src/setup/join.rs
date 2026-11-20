//! chattrs — `setup --join` (SETUP-2): store the tokens of join strings.
//!
//! A join string is `chattrs://<token>@<ip>:<port>?as=<pattern>`. The
//! `as` value is percent-encoded. Each string adds one token file under
//! `~/.config/chattrs/` (mode 0600) and one `[tokens]` entry in the user
//! config. The edit is textual and keeps every other line. A join string
//! is a secret: errors never show it, and nothing writes it to disk.

use std::fs;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::cli::SetupArgs;
use crate::tokens::{is_valid_pattern, parse_pair};
use crate::{hook, setup::broker, setup::SetupError};

const SCHEME: &str = "chattrs://";

/// A parsed join string.
#[derive(Debug, PartialEq, Eq)]
pub struct Join {
    pub token: String,
    pub server: SocketAddr,
    /// The decoded `as` pattern, when the string has one.
    pub pattern: Option<String>,
}

/// Percent-encode every byte except letters, digits, `.`, `_`, `-`.
fn encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = text.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Build the join string for a token, a broker address, and a pattern.
pub fn format(token: &str, server: SocketAddr, pattern: &str) -> String {
    format!("{SCHEME}{token}@{server}?as={}", encode(pattern))
}

/// Parse a join string. The errors never include the input.
pub fn parse(text: &str) -> Result<Join, SetupError> {
    let bad = |why: &str| SetupError::BadJoin(why.to_string());
    let rest =
        text.trim().strip_prefix(SCHEME).ok_or_else(|| bad("it must start with chattrs://"))?;
    let (token, rest) = rest.split_once('@').ok_or_else(|| bad("expected <token>@<ip>:<port>"))?;
    let valid_token = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.');
    if token.is_empty() || !token.bytes().all(valid_token) {
        return Err(bad("the token is empty or has a bad character"));
    }
    let (authority, query) = match rest.split_once('?') {
        Some((authority, query)) => (authority, Some(query)),
        None => (rest, None),
    };
    let server: SocketAddr =
        authority.parse().map_err(|_| bad("the server must be <ip>:<port>"))?;
    let pattern = match query {
        None => None,
        Some(query) => {
            let value = query.strip_prefix("as=").ok_or_else(|| bad("the only option is ?as="))?;
            let pattern = decode(value).ok_or_else(|| bad("the as value is not valid"))?;
            if !is_valid_pattern(&pattern) {
                return Err(bad("the as value is not an address pattern"));
            }
            Some(pattern)
        }
    };
    Ok(Join { token: token.to_string(), server, pattern })
}

/// The token file name for a pattern: `@` becomes `-` and `*` becomes
/// `any`, then `.token`. `lead@mac` gives `lead-mac.token`; `*@bobox`
/// gives `any-bobox.token`. Two patterns can map to one name (`a-b@c` and
/// `a@b-c`); the second join then refuses because the file exists.
fn file_name(pattern: &str) -> String {
    let stem: String = pattern
        .chars()
        .map(|c| match c {
            '@' => "-".to_string(),
            '*' => "any".to_string(),
            other => other.to_string(),
        })
        .collect();
    format!("{stem}.token")
}

/// Add `server` and the `[tokens]` entries to config text. Other lines stay
/// as they are. An existing entry for a pattern is an error unless `force`,
/// which rewrites that line. A different existing `server` is an error.
fn edit_config(
    text: &str,
    server: &str,
    entries: &[(String, String)],
    force: bool,
) -> Result<String, SetupError> {
    let mut lines: Vec<String> = text.lines().map(String::from).collect();
    let first_table = lines.iter().position(|l| l.trim().starts_with('[')).unwrap_or(lines.len());
    let current = lines[..first_table]
        .iter()
        .filter_map(|l| parse_pair(l.trim()))
        .find(|(key, _)| *key == "server")
        .map(|(_, value)| value);
    match current {
        Some(value) if value != server => {
            return Err(SetupError::Invalid(format!(
                "config.toml has server = \"{value}\", not \"{server}\""
            )));
        }
        Some(_) => {}
        None => lines.insert(0, format!("server = \"{server}\"")),
    }
    for (pattern, path) in entries {
        let line = format!("\"{pattern}\" = \"{path}\"");
        let Some(header) = lines.iter().position(|l| l.trim() == "[tokens]") else {
            if lines.last().is_some_and(|l| !l.trim().is_empty()) {
                lines.push(String::new());
            }
            lines.push("[tokens]".to_string());
            lines.push(line);
            continue;
        };
        let end = lines[header + 1..]
            .iter()
            .position(|l| l.trim().starts_with('['))
            .map_or(lines.len(), |n| header + 1 + n);
        let found = (header + 1..end).find(|&i| {
            parse_pair(lines[i].trim()).is_some_and(|(key, _)| key.trim_matches('"') == pattern)
        });
        match found {
            Some(_) if !force => {
                return Err(SetupError::Invalid(format!(
                    "config.toml already has a token entry for {pattern}; use --force to replace it"
                )));
            }
            Some(i) => lines[i] = line,
            None => lines.insert(header + 1, line),
        }
    }
    Ok(lines.join("\n") + "\n")
}

/// The `[tokens]` entries of config text, as pattern and path.
fn token_entries(text: &str) -> Vec<(String, String)> {
    let mut in_tokens = false;
    let mut out = Vec::new();
    for line in text.lines().map(str::trim) {
        if line.starts_with('[') {
            in_tokens = line == "[tokens]";
        } else if let Some((key, value)) = parse_pair(line).filter(|_| in_tokens) {
            out.push((key.trim_matches('"').to_string(), value));
        }
    }
    out
}

/// Refuse a token file that another identity owns. `files` are the
/// pattern and file path of each join. Two patterns of one run may not
/// share a file, and a file that config.toml lists for a different
/// pattern is never replaced, with or without `--force`.
fn check_collisions(config: &str, files: &[(String, String)]) -> Result<(), SetupError> {
    let existing = token_entries(config);
    for (i, (pattern, path)) in files.iter().enumerate() {
        if let Some((other, _)) = files[..i].iter().find(|(_, other)| other == path) {
            return Err(SetupError::Invalid(format!(
                "{pattern} and {other} map to the same token file {path}"
            )));
        }
        if let Some((other, _)) = existing.iter().find(|(p, v)| v == path && p != pattern) {
            return Err(SetupError::Invalid(format!(
                "{path} already holds the token for {other}, not {pattern}"
            )));
        }
    }
    Ok(())
}

/// The join strings from `--join` values; `-` expands to the lines of stdin.
fn collect(values: &[String], stdin: &mut dyn Read) -> Result<Vec<String>, SetupError> {
    let mut all = Vec::new();
    for value in values {
        if value == "-" {
            let mut text = String::new();
            stdin.read_to_string(&mut text).map_err(SetupError::Stdin)?;
            all.extend(text.lines().map(str::trim).filter(|l| !l.is_empty()).map(String::from));
        } else {
            all.push(value.clone());
        }
    }
    if all.is_empty() {
        return Err(SetupError::Invalid("no join string given".to_string()));
    }
    Ok(all)
}

/// Parse the strings and settle each pattern from `?as=` and `--as`.
fn resolve(strings: &[String], flag: Option<&str>) -> Result<Vec<(Join, String)>, SetupError> {
    let invalid = |message: String| SetupError::Invalid(message);
    if flag.is_some() && strings.len() != 1 {
        return Err(invalid("--as works with exactly one join string".to_string()));
    }
    let mut out: Vec<(Join, String)> = Vec::new();
    for text in strings {
        let join = parse(text)?;
        let pattern = match (&join.pattern, flag) {
            (Some(inside), Some(flag)) if inside != flag => {
                return Err(invalid(format!(
                    "--as {flag} differs from the join string's ?as={inside}"
                )));
            }
            (Some(inside), _) => inside.clone(),
            (None, Some(flag)) if is_valid_pattern(flag) => flag.to_string(),
            (None, Some(_)) => return Err(invalid("--as is not an address pattern".to_string())),
            (None, None) => {
                return Err(invalid(
                    "a join string has no ?as= pattern; give --as PATTERN".to_string(),
                ))
            }
        };
        if out.iter().any(|(other, _)| other.server != join.server) {
            return Err(invalid("the join strings name different servers".to_string()));
        }
        if out.iter().any(|(_, seen)| *seen == pattern) {
            return Err(invalid(format!("the pattern {pattern} is given twice")));
        }
        out.push((join, pattern));
    }
    Ok(out)
}

/// Store the tokens and edit the config under `home`.
fn store(home: &Path, joins: &[(Join, String)], force: bool) -> Result<(), SetupError> {
    let dir = home.join(".config/chattrs");
    let config = dir.join("config.toml");
    let text = if config.exists() {
        fs::read_to_string(&config).map_err(|source| SetupError::Fs {
            verb: "read",
            path: config.clone(),
            source,
        })?
    } else {
        String::new()
    };
    let files: Vec<(&Join, &String, PathBuf)> =
        joins.iter().map(|(join, pattern)| (join, pattern, dir.join(file_name(pattern)))).collect();
    let entries: Vec<(String, String)> = files
        .iter()
        .map(|(_, pattern, path)| ((*pattern).clone(), path.display().to_string()))
        .collect();
    if joins.iter().any(|(join, _)| join.server != joins[0].0.server) {
        return Err(SetupError::Invalid("the join strings name different servers".to_string()));
    }
    check_collisions(&text, &entries)?;
    let edited = edit_config(&text, &joins[0].0.server.to_string(), &entries, force)?;
    for (_, pattern, path) in &files {
        if path.exists() && !force {
            return Err(SetupError::Invalid(format!(
                "{} exists, so {pattern} was not joined; use --force to replace it",
                path.display()
            )));
        }
    }
    for (join, pattern, path) in &files {
        broker::write_file(path, &format!("{}\n", join.token), 0o600, force)?;
        broker::restrict(path)?;
        eprintln!("chattrs: stored the token for {pattern} in {}", path.display());
    }
    broker::write_file(&config, &edited, 0o600, true)?;
    eprintln!("chattrs: updated {}", config.display());
    Ok(())
}

/// Run `setup --join`.
pub fn run(args: &SetupArgs) -> Result<(), SetupError> {
    let strings = collect(&args.join, &mut std::io::stdin())?;
    let joins = resolve(&strings, args.join_as.as_deref())?;
    store(&hook::install::home()?, &joins, args.force)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SERVER: &str = "100.64.0.1:4646";

    fn addr() -> SocketAddr {
        SERVER.parse().unwrap()
    }

    #[test]
    fn join_strings_round_trip() {
        for pattern in ["*@bobox", "lead@mac", "a.b-c_d@h.o-s_t", "tray@*"] {
            let text = format("tok123", addr(), pattern);
            assert!(!text.contains('*') && text.matches('@').count() == 1, "{text}");
            let join = parse(&text).unwrap();
            assert_eq!(join.pattern.as_deref(), Some(pattern));
            assert_eq!((join.token.as_str(), join.server), ("tok123", addr()));
        }
        assert_eq!(format("t", addr(), "*@bobox"), "chattrs://t@100.64.0.1:4646?as=%2A%40bobox");
    }

    #[test]
    fn as_is_optional_and_ipv6_works() {
        let join = parse("chattrs://abc@[::1]:4646").unwrap();
        assert_eq!((join.pattern, join.server.to_string()), (None, "[::1]:4646".to_string()));
    }

    #[test]
    fn bad_strings_fail_without_the_token() {
        for text in [
            "http://secret@1.2.3.4:1",
            "chattrs://secret",
            "chattrs://@1.2.3.4:1",
            "chattrs://secret@host:1",
            "chattrs://secret@1.2.3.4",
            "chattrs://sec ret@1.2.3.4:1",
            "chattrs://secret@1.2.3.4:1?x=1",
            "chattrs://secret@1.2.3.4:1?as=%zz",
            "chattrs://secret@1.2.3.4:1?as=nope",
            "chattrs://secret@1.2.3.4:1?as=a%2Fb%40c",
        ] {
            let error = parse(text).unwrap_err().to_string();
            assert!(!error.contains("secret"), "{text}: {error}");
        }
    }

    #[test]
    fn file_names_are_safe() {
        assert_eq!(file_name("lead@mac"), "lead-mac.token");
        assert_eq!(file_name("*@bobox"), "any-bobox.token");
        assert_eq!(file_name("tray@*"), "tray-any.token");
    }

    fn entry(pattern: &str, path: &str) -> Vec<(String, String)> {
        vec![(pattern.to_string(), path.to_string())]
    }

    #[test]
    fn config_edit_keeps_user_lines() {
        let old = "# mine\ntoken_file = \"/t\"\ntags = \"x\"\n";
        let new = edit_config(old, SERVER, &entry("lead@mac", "/l"), false).unwrap();
        assert!(new.contains("# mine\ntoken_file = \"/t\"\ntags = \"x\"\n"), "{new}");
        assert!(new.starts_with(&format!("server = \"{SERVER}\"\n")), "{new}");
        assert!(new.ends_with("\n[tokens]\n\"lead@mac\" = \"/l\"\n"), "{new}");
        let more = edit_config(&new, SERVER, &entry("*@mac", "/m"), false).unwrap();
        assert!(more.contains("[tokens]\n\"*@mac\" = \"/m\"\n\"lead@mac\" = \"/l\"\n"), "{more}");
    }

    #[test]
    fn config_edit_refuses_conflicts() {
        let old = format!("server = \"{SERVER}\"\n[tokens]\n\"lead@mac\" = \"/l\"\n");
        assert!(edit_config(&old, SERVER, &entry("*@mac", "/m"), false).is_ok());
        let err =
            edit_config(&old, SERVER, &entry("lead@mac", "/l"), false).unwrap_err().to_string();
        assert!(err.contains("--force"), "{err}");
        let forced = edit_config(&old, SERVER, &entry("lead@mac", "/new"), true).unwrap();
        assert!(forced.contains("\"lead@mac\" = \"/new\"") && !forced.contains("\"/l\""));
        assert!(edit_config(&old, "10.0.0.1:1", &[], false).is_err());
    }

    #[test]
    fn resolve_settles_the_pattern() {
        let with = format("t1", addr(), "lead@mac");
        let without = "chattrs://t2@100.64.0.1:4646".to_string();
        assert_eq!(resolve(std::slice::from_ref(&with), None).unwrap()[0].1, "lead@mac");
        assert_eq!(resolve(std::slice::from_ref(&with), Some("lead@mac")).unwrap().len(), 1);
        assert!(resolve(std::slice::from_ref(&with), Some("x@mac")).is_err());
        assert_eq!(resolve(std::slice::from_ref(&without), Some("*@a")).unwrap()[0].1, "*@a");
        assert!(resolve(std::slice::from_ref(&without), None).is_err());
        assert!(resolve(&[with.clone(), with], None).is_err());
    }

    #[test]
    fn different_servers_are_refused() {
        let one = format("t1", addr(), "lead@mac");
        let two = format("t2", "10.0.0.9:4646".parse().unwrap(), "*@bobox");
        let error = resolve(&[one, two], None).unwrap_err().to_string();
        assert!(error.contains("different servers") && !error.contains("t1"), "{error}");
    }

    #[test]
    fn colliding_file_names_are_refused() {
        let pair = |pattern: &str| (pattern.to_string(), format!("/c/{}", file_name(pattern)));
        let both = [pair("a-b@c"), pair("a@b-c")];
        assert_eq!(both[0].1, both[1].1);
        assert!(check_collisions("", &both).unwrap_err().to_string().contains("same token file"));
        // config.toml lists the file for another pattern: no replacing it.
        let config = format!("[tokens]\n\"a-b@c\" = \"{}\"\n", both[0].1);
        let err = check_collisions(&config, &both[1..]).unwrap_err().to_string();
        assert!(err.contains("a-b@c"), "{err}");
        // The same pattern may be replaced; an unrelated one is fine.
        assert!(check_collisions(&config, &both[..1]).is_ok());
        assert!(check_collisions(&config, &[pair("x@y")]).is_ok());
    }

    #[test]
    fn stdin_lines_expand() {
        let mut input: &[u8] = b"one\n\n two \n";
        let got = collect(&["-".to_string(), "x".to_string()], &mut input).unwrap();
        assert_eq!(got, ["one", "two", "x"]);
    }
}
