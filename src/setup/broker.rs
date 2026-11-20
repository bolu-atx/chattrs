//! chattrs — `setup --broker` (SETUP-2): prepare the broker host.
//!
//! It writes the broker tokens file (mode 0600) and a service unit, and
//! issues one join string per host plus one for the lead and one for the
//! observer. It never overwrites the tokens file, a unit, or a join file.
//! The join strings are secrets. With `--join-dir` they go to 0600 files,
//! and stdout lists only the paths. Without it they go to a terminal, and
//! a run with stdout not a terminal refuses.

use std::fs;
use std::io::{IsTerminal, Read, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::cli::{SetupArgs, DEFAULT_PORT};
use crate::{hook, model, setup::join, setup::SetupError, store::Store};

const LABEL: &str = "com.chattrs.broker";
const OBSERVER_ADDR: &str = "tray@*";

/// What the user asked for, after validation.
struct Plan {
    hosts: Vec<String>,
    lead: String,
    /// Empty means the tokens carry no project line.
    projects: Vec<String>,
}

/// One join string to hand out: a label, a file stem, the pattern, and the token.
struct Issued {
    label: String,
    /// The join file is `<name>.join`.
    name: String,
    pattern: String,
    token: String,
}

/// Fill `buf` from the operating system's random source.
#[cfg(unix)]
fn fill_random(buf: &mut [u8]) -> Result<(), SetupError> {
    fs::File::open("/dev/urandom").and_then(|mut file| file.read_exact(buf)).map_err(|source| {
        SetupError::Fs { verb: "read", path: PathBuf::from("/dev/urandom"), source }
    })
}

#[cfg(windows)]
fn fill_random(buf: &mut [u8]) -> Result<(), SetupError> {
    #[link(name = "bcrypt")]
    extern "system" {
        fn BCryptGenRandom(algorithm: *mut u8, buffer: *mut u8, len: u32, flags: u32) -> i32;
    }
    // Flag 2 is BCRYPT_USE_SYSTEM_PREFERRED_RNG.
    let status =
        unsafe { BCryptGenRandom(std::ptr::null_mut(), buf.as_mut_ptr(), buf.len() as u32, 2) };
    if status == 0 {
        Ok(())
    } else {
        Err(SetupError::Invalid(format!("BCryptGenRandom failed: {status:#x}")))
    }
}

/// A token: 32 random bytes as 64 hex characters.
fn random_token() -> Result<String, SetupError> {
    let mut bytes = [0u8; 32];
    fill_random(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// Write `text` to `path`, creating parent directories. With `overwrite`
/// false, an existing file is an error. `mode` applies to a new file.
pub fn write_file(path: &Path, text: &str, mode: u32, overwrite: bool) -> Result<(), SetupError> {
    let fail = |source| SetupError::Fs { verb: "write", path: path.to_path_buf(), source };
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|source| SetupError::Fs {
            verb: "create",
            path: dir.to_path_buf(),
            source,
        })?;
    }
    let mut options = fs::OpenOptions::new();
    options.write(true);
    if overwrite {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, mode);
    #[cfg(not(unix))]
    let _ = mode;
    options.open(path).map_err(fail)?.write_all(text.as_bytes()).map_err(fail)
}

/// Set mode 0600 on an existing file (unix).
pub fn restrict(path: &Path) -> Result<(), SetupError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|source| SetupError::Fs { verb: "chmod", path: path.to_path_buf(), source })?;
    }
    let _ = path;
    Ok(())
}

fn valid_host(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 100
        && host.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Check the flags and fill in the defaults.
fn plan(args: &SetupArgs) -> Result<Plan, SetupError> {
    let invalid = |message: String| SetupError::Invalid(message);
    let hosts = args.hosts.clone();
    for (i, host) in hosts.iter().enumerate() {
        if !valid_host(host) {
            return Err(invalid(format!("`{host}` is not a valid host name")));
        }
        if hosts[..i].contains(host) {
            return Err(invalid(format!("host `{host}` is listed twice")));
        }
    }
    let first =
        hosts.first().ok_or_else(|| invalid("--hosts needs at least one host".to_string()))?;
    let lead = args.lead.clone().unwrap_or_else(|| format!("lead@{first}"));
    if !model::is_valid_address(&lead) {
        return Err(invalid(format!("`{lead}` is not a valid lead address")));
    }
    let projects = args.projects.clone();
    if let Some(bad) = projects.iter().find(|p| !model::is_valid_topic(p)) {
        return Err(invalid(format!("`{bad}` is not a valid project name")));
    }
    Ok(Plan { hosts, lead, projects })
}

/// Build the tokens file text and the list of issued tokens. `next_token`
/// supplies each new token.
fn generate(
    plan: &Plan,
    mut next_token: impl FnMut() -> Result<String, SetupError>,
) -> Result<(String, Vec<Issued>), SetupError> {
    let mut text = String::from(
        "# chattrs broker tokens, written by `chattrs setup --broker`. Keep mode 0600.\n",
    );
    let mut issued = Vec::new();
    let mut add = |token: &str, project: Option<&str>, addr: &str, role: &str| {
        let project = project.map(|p| format!("project = \"{p}\"\n")).unwrap_or_default();
        text.push_str(&format!(
            "\n[[token]]\ntoken = \"{token}\"\n{project}addr = \"{addr}\"\nrole = \"{role}\"\n"
        ));
    };
    // No `--projects`: one entry with no project. Otherwise one per project.
    let scopes: Vec<Option<&str>> = if plan.projects.is_empty() {
        vec![None]
    } else {
        plan.projects.iter().map(|p| Some(p.as_str())).collect()
    };
    for host in &plan.hosts {
        let token = next_token()?;
        let pattern = format!("*@{host}");
        for scope in &scopes {
            add(&token, *scope, &pattern, "agent");
        }
        issued.push(Issued { label: format!("agent {host}"), name: host.clone(), pattern, token });
    }
    let token = next_token()?;
    for scope in &scopes {
        add(&token, *scope, &plan.lead, "lead");
    }
    issued.push(Issued {
        label: format!("lead {}", plan.lead),
        name: "lead".to_string(),
        pattern: plan.lead.clone(),
        token,
    });
    let token = next_token()?;
    add(&token, None, OBSERVER_ADDR, "observer");
    issued.push(Issued {
        label: format!("observer {OBSERVER_ADDR}"),
        name: "observer".to_string(),
        pattern: OBSERVER_ADDR.to_string(),
        token,
    });
    Ok((text, issued))
}

/// The IP to bind: `--bind`, else the first line of `tailscale ip -4`. A
/// wildcard address is refused (SEC-1).
fn resolve_bind(
    bind: Option<&str>,
    tailscale: impl FnOnce() -> Option<String>,
) -> Result<IpAddr, SetupError> {
    let invalid = |message: &str| SetupError::Invalid(message.to_string());
    let text = match bind {
        Some(text) => text.to_string(),
        None => tailscale().ok_or_else(|| {
            invalid("no Tailscale IPv4 found (`tailscale ip -4`); pass --bind <ip>")
        })?,
    };
    let ip: IpAddr = text.trim().parse().map_err(|_| invalid("--bind must be an IP address"))?;
    if ip.is_unspecified() {
        return Err(invalid("refusing to bind 0.0.0.0 or ::; give one interface IP"));
    }
    Ok(ip)
}

fn tailscale_ipv4() -> Option<String> {
    let output = Command::new("tailscale").args(["ip", "-4"]).output().ok()?;
    let text = String::from_utf8(output.stdout).ok()?;
    output.status.success().then(|| text.lines().next().map(str::to_string))?
}

/// The arguments after the program name, in the order `serve` reads them.
fn serve_args(listen: SocketAddr, db: &Path, tokens: &Path) -> Vec<String> {
    vec![
        "serve".to_string(),
        "--listen".to_string(),
        listen.to_string(),
        "--db".to_string(),
        db.display().to_string(),
        "--tokens".to_string(),
        tokens.display().to_string(),
    ]
}

fn systemd_quote(arg: &str) -> String {
    let plain = arg.bytes().all(|b| b.is_ascii_alphanumeric() || b"/._:@-".contains(&b));
    if plain {
        return arg.to_string();
    }
    let escaped = arg.replace('\\', "\\\\").replace('"', "\\\"").replace('%', "%%");
    format!("\"{escaped}\"")
}

fn systemd_unit(exe: &Path, args: &[String]) -> String {
    let command: Vec<String> = std::iter::once(exe.display().to_string())
        .chain(args.iter().cloned())
        .map(|arg| systemd_quote(&arg))
        .collect();
    format!(
        "[Unit]\nDescription=chattrs broker\nAfter=network-online.target\n\n[Service]\n\
         ExecStart={}\nRestart=on-failure\nRestartSec=5\n\n[Install]\nWantedBy=default.target\n",
        command.join(" ")
    )
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn launchd_plist(exe: &Path, args: &[String]) -> String {
    let items: String = std::iter::once(exe.display().to_string())
        .chain(args.iter().cloned())
        .map(|arg| format!("    <string>{}</string>\n", xml_escape(&arg)))
        .collect();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
         \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n<dict>\n  <key>Label</key>\n  <string>{LABEL}</string>\n  \
         <key>ProgramArguments</key>\n  <array>\n{items}  </array>\n  \
         <key>RunAtLoad</key>\n  <true/>\n  <key>KeepAlive</key>\n  <true/>\n</dict>\n</plist>\n"
    )
}

/// The service unit for this OS: its path, its text, the command that
/// enables it, and the same command as an argument list.
struct Unit {
    path: PathBuf,
    text: String,
    shown: String,
    steps: Vec<Vec<String>>,
}

fn unit(home: &Path, exe: &Path, args: &[String]) -> Result<Unit, SetupError> {
    if cfg!(windows) {
        return Err(SetupError::Invalid(
            "setup --broker writes a systemd or launchd unit; this OS has neither".to_string(),
        ));
    }
    let words = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    if cfg!(target_os = "macos") {
        let path = home.join(format!("Library/LaunchAgents/{LABEL}.plist"));
        let shown = format!("launchctl bootstrap gui/$UID {}", path.display());
        let uid = Command::new("id").arg("-u").output().ok();
        let uid = uid.and_then(|o| String::from_utf8(o.stdout).ok()).unwrap_or_default();
        let step = vec![
            "launchctl".to_string(),
            "bootstrap".to_string(),
            format!("gui/{}", uid.trim()),
            path.display().to_string(),
        ];
        Ok(Unit { path, text: launchd_plist(exe, args), shown, steps: vec![step] })
    } else {
        Ok(Unit {
            path: home.join(".config/systemd/user/chattrs.service"),
            text: systemd_unit(exe, args),
            shown:
                "systemctl --user daemon-reload && systemctl --user enable --now chattrs.service"
                    .to_string(),
            steps: vec![
                words(&["systemctl", "--user", "daemon-reload"]),
                words(&["systemctl", "--user", "enable", "--now", "chattrs.service"]),
            ],
        })
    }
}

/// The join file for each issued token, in order. Names keep only
/// `[A-Za-z0-9._-]`. Two tokens with one file name are an error.
fn join_files(dir: &Path, issued: &[Issued]) -> Result<Vec<PathBuf>, SetupError> {
    let mut files: Vec<PathBuf> = Vec::new();
    for item in issued {
        let stem: String = item
            .name
            .chars()
            .map(
                |c| if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') { c } else { '_' },
            )
            .collect();
        let path = dir.join(format!("{stem}.join"));
        if files.contains(&path) {
            return Err(SetupError::Invalid(format!(
                "two join strings map to {}; rename a host",
                path.display()
            )));
        }
        files.push(path);
    }
    Ok(files)
}

/// Create `dir` (mode 0700 on unix) and its missing parents.
fn make_private_dir(dir: &Path) -> Result<(), SetupError> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir).map_err(|source| SetupError::Fs {
        verb: "create",
        path: dir.to_path_buf(),
        source,
    })
}

/// Run `setup --broker`.
pub fn run(args: &SetupArgs) -> Result<(), SetupError> {
    run_with(args, std::io::stdout().is_terminal())
}

/// `run`, with the terminal check as a parameter so tests can set it.
fn run_with(args: &SetupArgs, stdout_is_terminal: bool) -> Result<(), SetupError> {
    let plan = plan(args)?;
    if args.join_dir.is_none() && !stdout_is_terminal {
        return Err(SetupError::Invalid(
            "stdout is not a terminal; pass --join-dir DIR so join strings go to files".to_string(),
        ));
    }
    let ip = resolve_bind(args.bind.as_deref(), tailscale_ipv4)?;
    let listen = SocketAddr::new(ip, args.port.unwrap_or(DEFAULT_PORT));
    let home = hook::install::home()?;
    let tokens_path = home.join(".config/chattrs/tokens.toml");
    let exe = std::env::current_exe().map_err(SetupError::CurrentExe)?;
    let unit = unit(&home, &exe, &serve_args(listen, &Store::default_path(), &tokens_path))?;
    if tokens_path.exists() {
        return Err(SetupError::Exists(tokens_path));
    }
    if unit.path.exists() {
        return Err(SetupError::Exists(unit.path));
    }
    let (text, issued) = generate(&plan, random_token)?;
    // Check every join file before the first write, so a refusal leaves nothing behind.
    let files = match &args.join_dir {
        Some(dir) => {
            let files = join_files(dir, &issued)?;
            if let Some(path) = files.iter().find(|path| path.exists()) {
                return Err(SetupError::Exists(path.clone()));
            }
            make_private_dir(dir)?;
            files
        }
        None => Vec::new(),
    };
    write_file(&tokens_path, &text, 0o600, false)?;
    write_file(&unit.path, &unit.text, 0o644, false)?;
    eprintln!("chattrs: wrote {}", tokens_path.display());
    eprintln!("chattrs: wrote {}", unit.path.display());
    if files.is_empty() {
        println!("Treat these like passwords.");
        for item in &issued {
            println!("{}: {}", item.label, join::format(&item.token, listen, &item.pattern));
        }
    } else {
        for (item, path) in issued.iter().zip(&files) {
            let line = format!("{}\n", join::format(&item.token, listen, &item.pattern));
            write_file(path, &line, 0o600, false)?;
            println!("{}: {}", item.label, path.display());
        }
    }
    eprintln!("chattrs: to start the broker, run: {}", unit.shown);
    if args.enable {
        for step in &unit.steps {
            let status = Command::new(&step[0])
                .args(&step[1..])
                .status()
                .map_err(|source| SetupError::Spawn { program: step[0].clone(), source })?;
            if !status.success() {
                return Err(SetupError::StepFailed { command: step.join(" "), status });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::Tokens;

    fn args(hosts: &[&str]) -> SetupArgs {
        SetupArgs {
            broker: true,
            hosts: hosts.iter().map(|h| h.to_string()).collect(),
            ..SetupArgs::default()
        }
    }

    fn counter() -> impl FnMut() -> Result<String, SetupError> {
        let mut n = 0;
        move || {
            n += 1;
            Ok(format!("tok{n}"))
        }
    }

    #[test]
    fn generated_file_authorizes_as_intended() {
        let mut a = args(&["a", "b"]);
        a.projects = vec!["p1".to_string(), "p2".to_string()];
        let (text, issued) = generate(&plan(&a).unwrap(), counter()).unwrap();
        let tokens = Tokens::parse(&text).unwrap();
        let tok = |label: &str| issued.iter().find(|i| i.label.starts_with(label)).unwrap();
        let check = |label: &str, project: &str, address: &str, observer: bool| {
            let entry = tokens.find(Some(&tok(label).token), project).map_err(|_| "unknown")?;
            tokens.authorize(entry, project, address, observer)
        };
        for project in ["p1", "p2"] {
            assert_eq!(check("lead", project, "lead@a", false), Ok(()));
            assert_eq!(check("agent a", project, "w1@a", false), Ok(()));
            assert!(check("agent a", project, "w1@b", false).is_err());
            assert!(check("agent a", project, "lead@a", false).is_err());
            assert!(check("agent b", project, "w1@a", false).is_err());
            assert_eq!(check("observer", project, "tray@x", true), Ok(()));
        }
        assert!(check("observer", "p1", "tray@x", false).is_err());
        assert!(check("agent a", "p3", "w1@a", false).is_err());
    }

    #[test]
    fn default_file_has_no_project_lines_and_works_in_any_project() {
        let (text, issued) = generate(&plan(&args(&["a", "b"])).unwrap(), counter()).unwrap();
        assert!(!text.contains("project"), "{text}");
        let tokens = Tokens::parse(&text).unwrap();
        let agent = issued.iter().find(|i| i.label == "agent a").unwrap();
        for project in ["alpha", "beta"] {
            let entry = tokens.find(Some(&agent.token), project).unwrap();
            assert_eq!(tokens.authorize(entry, project, "w1@a", false), Ok(()));
            assert!(tokens.authorize(entry, project, "lead@a", false).is_err());
        }
    }

    #[test]
    fn defaults_and_validation() {
        let plan_ = plan(&args(&["mac", "bobox"])).unwrap();
        assert_eq!((plan_.lead.as_str(), plan_.projects), ("lead@mac", Vec::<String>::new()));
        assert!(plan(&args(&["a", "a"])).is_err());
        assert!(plan(&args(&["a b"])).is_err());
        let mut bad = args(&["a"]);
        bad.projects = vec!["Bad_Name".to_string()];
        assert!(plan(&bad).is_err());
    }

    #[test]
    fn bind_refuses_wildcards() {
        for bad in ["0.0.0.0", "::", "[::]", "mac"] {
            assert!(resolve_bind(Some(bad), || None).is_err(), "{bad}");
        }
        assert_eq!(resolve_bind(Some("127.0.0.1"), || None).unwrap().to_string(), "127.0.0.1");
        assert_eq!(
            resolve_bind(None, || Some("100.1.2.3".into())).unwrap().to_string(),
            "100.1.2.3"
        );
        assert!(resolve_bind(None, || None).unwrap_err().to_string().contains("--bind"));
    }

    #[test]
    fn random_tokens_differ() {
        let (one, two) = (random_token().unwrap(), random_token().unwrap());
        assert_eq!(one.len(), 64);
        assert_ne!(one, two);
    }

    #[cfg(unix)]
    #[test]
    fn files_are_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a/tokens.toml");
        write_file(&path, "x", 0o600, false).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        assert!(write_file(&path, "y", 0o600, false).is_err());
    }

    #[test]
    fn join_files_refuse_collisions() {
        let (_, issued) = generate(&plan(&args(&["lead"])).unwrap(), counter()).unwrap();
        let err = join_files(Path::new("/d"), &issued).unwrap_err().to_string();
        assert!(err.contains("/d/lead.join"), "{err}");
        let (_, issued) = generate(&plan(&args(&["a", "b"])).unwrap(), counter()).unwrap();
        let files = join_files(Path::new("/d"), &issued).unwrap();
        assert_eq!(files[0], Path::new("/d/a.join"));
        assert_eq!(files[3], Path::new("/d/observer.join"));
    }

    #[test]
    fn refuses_without_terminal_or_join_dir_before_writing() {
        let err = run_with(&args(&["a"]), false).unwrap_err().to_string();
        assert!(err.contains("--join-dir"), "{err}");
    }

    #[test]
    fn units_carry_the_serve_command() {
        let listen: SocketAddr = "100.1.2.3:4646".parse().unwrap();
        let args = serve_args(listen, Path::new("/d/chattrs.db"), Path::new("/t s/tokens.toml"));
        let exe = Path::new("/bin/chattrs");
        let service = systemd_unit(exe, &args);
        assert!(service.contains(
            "ExecStart=/bin/chattrs serve --listen 100.1.2.3:4646 --db /d/chattrs.db \
             --tokens \"/t s/tokens.toml\"\n"
        ));
        let plist = launchd_plist(exe, &args);
        assert!(plist.contains("<string>100.1.2.3:4646</string>") && plist.contains(LABEL));
    }
}
