//! Test helper shared by `tests/cli.rs` and the broker unit tests
//! (`src/server/tests/helpers.rs` includes this file by path).

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
