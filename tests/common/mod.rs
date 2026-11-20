//! Test helper shared by the CLI tests in `tests/` and the broker unit tests
//! (`src/server/tests/fixture.rs` includes this file by path).

use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::hash::{BuildHasher as _, Hasher as _};
use std::net::TcpListener;
use std::sync::Mutex;

/// Ports below the OS ephemeral range (macOS and Windows start at 49152,
/// Linux at 32768), so an outgoing client connection never takes one.
const FIRST_PORT: u16 = 20_000;
const PORT_SPAN: u64 = 10_000;

static TAKEN: Mutex<Option<HashSet<u16>>> = Mutex::new(None);

/// A loopback port nothing listens on, for a code path that binds it
/// later (a broker spawned by `serve` or by an auto-start). Picking `:0`
/// and dropping the probe is racy: the kernel hands that port to any
/// outgoing connection or other test that asks next. This avoids both.
/// The port lies outside the ephemeral range. It is random, so parallel
/// test processes rarely pick the same one. A process never hands out
/// the same port twice. A bind check skips a port that is in use.
pub fn free_port() -> u16 {
    loop {
        let roll = RandomState::new().build_hasher().finish();
        let port = FIRST_PORT + (roll % PORT_SPAN) as u16;
        let fresh = TAKEN.lock().unwrap().get_or_insert_with(HashSet::new).insert(port);
        if fresh && TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

/// The developer's own home, read once before any test changes anything.
static REAL_HOME: std::sync::OnceLock<Option<std::ffi::OsString>> = std::sync::OnceLock::new();

/// One scratch home per test process. A spawned chattrs sees it as `HOME`,
/// the XDG dirs, and `CODEX_HOME`, so no test reads or writes the user's
/// `~/.config`, `~/.claude`, or `~/.codex`.
fn scratch_home() -> &'static std::path::Path {
    static HOME: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    let real = REAL_HOME.get_or_init(|| std::env::var_os("HOME"));
    HOME.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("chattrs-test-home-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("make the scratch home");
        assert!(
            real.as_deref() != Some(dir.as_os_str()),
            "the scratch home must not be the real HOME"
        );
        dir
    })
}

/// A `Command` for a spawned chattrs (or any child) that is cut off from
/// the user's environment: `HOME`, `XDG_CONFIG_HOME`, `XDG_DATA_HOME`, and
/// `CODEX_HOME` point into a scratch dir, the cwd is that dir (not a repo),
/// the variables that name a repo are removed, and every inherited `CHATTRS_*`
/// variable is removed. Set test variables on the result, after this call.
pub fn command(program: impl AsRef<std::ffi::OsStr>) -> std::process::Command {
    let home = scratch_home();
    let mut command = std::process::Command::new(program);
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("CHATTRS_") {
            command.env_remove(name);
        }
    }
    // No test derives a git project unless it means to: the cwd is not a
    // repo, no variable names one, and the search stops at the temp dir.
    for name in ["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR"] {
        command.env_remove(name);
    }
    command
        .current_dir(home)
        .env("GIT_CEILING_DIRECTORIES", std::env::temp_dir())
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("XDG_DATA_HOME", home.join(".local/share"))
        .env("CODEX_HOME", home.join(".codex"));
    command
}
