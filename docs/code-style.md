# Code style — chattrs

Rust edition 2021. `cargo build` and `cargo clippy --all-targets` must be
warning-free; CI runs clippy with `-D warnings`. Fix the code, never silence
the lint: no `#[allow(clippy::...)]` to make a gate pass.

## Principles

What good code looks like here:

- Data first: types in `model.rs` carry the wire shape; behavior follows.
- Functional core, imperative shell: `model`/`protocol` stay pure; sockets
  and IO live in `server.rs`/`client.rs` behind tokio.
- Parse, don't validate: decode a frame once at the boundary, then trust the
  type. Extra validation happens only where a value must be rejected
  (empty agent/session, empty body).
- YAGNI: no hooks, frameworks, or helpers for imagined work. The broker is
  in-memory and boring on purpose.
- Rule of three: first copy is fine, second is a note, third lifts into a
  shared helper.
- Delete beats abstract: a helper left with one caller gets inlined and
  removed.

## Shape of the code

- Small functions with one job: framing in `protocol.rs`, the domain types
  in `model.rs`, the broker loop in `server.rs`, the per-command connections
  in `client.rs`, argument parsing in `cli.rs`.
- Layer discipline: `model`/`protocol` never import tokio or open sockets.
  The client never imports server state, and vice versa.
- Prefer exhaustive handling: every command lists all of its behaviors
  (happy path, error, empty). A happy-path-only patch is unfinished.
- No dead code. No commented-out blocks. If a helper loses its last caller,
  delete it.

## Files and modules

- Module paths use `foo.rs` plus a `foo/` folder for children. Never use
  `foo/mod.rs`.
- One concern per file. A file name says what the file owns. Do not make
  `utils.rs`, `helpers.rs`, or `common.rs`: they collect unrelated code and
  only grow.
- Size budget. When a file passes about 800 lines without its tests, or
  about 1500 lines in total, split it before adding more. Cut along a path
  seam — a group of functions that call each other, plus their tests — not
  by item kind.
- Tests stay in the same file as the code they test, in
  `#[cfg(test)] mod tests`.
  Exception: a big module may keep its tests in `foo/tests/*.rs`, one file
  per subject, as `src/server/tests/` does.
- Item order in a file: `mod` declarations, then `use`, then re-exports.
  After that, types before `impl` blocks and functions; public items before
  private ones.
- Imports: `std`, external crates, and `crate::` in separate groups with a
  blank line between them. Prefer `use crate::foo::bar` to `use super::bar`;
  the exception is `use super::*` in a test module. No glob imports of enum
  variants.
- Visibility: use the narrowest level that compiles. `pub(super)` for helpers
  shared inside one module tree, `pub(crate)` for items other modules use.
  Add a `pub use` only at a facade or module root, and only for an item
  something outside the module actually uses.

## Naming

- RFC 430 casing: `snake_case` for modules, functions and locals;
  `UpperCamelCase` for types, traits and variants; `SCREAMING_SNAKE_CASE` for
  consts and statics. An acronym counts as one word: `Ts`, not `TS`.
- Getters have no `get_` prefix. `as_` borrows cheaply, `to_` is costly,
  `into_` consumes.
- Do not repeat the container in a name. Inside `Session`, a field is
  `agents`, not `session_agents`.
- Locals get plain, full names: name a local after its type in lowercase.
  Avoid single letters except in short closures and loop indices.
- American spelling. One word for one idea across the crate: if it is
  `session` in one place, do not call it `room` in another.

## Structs and state

- Group fields by owner. When a struct grows past about 20 fields, or holds
  two concerns that do not change together, move each concern into a
  sub-struct.
- Prefer `#[derive(Default)]` to a `new()` with no arguments. Write `new()`
  only when a field has no sensible default, such as an `mpsc` channel pair.
- Make a field public if any value is valid for it. Keep it private and give
  it a getter when it has an invariant. Do not write setters.
- Take borrowed views as parameters: `&str` over `&String`, `&[T]` over
  `&Vec<T>`.

## Functions

- Return early: handle the error or empty case first, then the main path
  without extra nesting.
- Seven parameters or fewer (clippy enforces this). Group related parameters
  into the domain type they come from.
- Do not add a `bool` or `Option` parameter that every caller passes as a
  literal. Split the function in two.
- Do not create a helper used in only one place unless it needs `return` or
  `?`. Use a block instead.

## Comments

- A comment says why, not what, in full sentences that start with a capital
  letter and end with a period.
- Put `///` on an item when its name and type do not explain it, directly
  above that item and above any attributes on it.
- Never write comments about the edit itself: no "moved to X", "renamed
  from Y", "previously", "new:", or "was:".
- No commented-out code. No `TODO` without an issue number.
- Error strings start in lowercase and do not end with a period. They are
  printed as `chattrs: <error>`.

## Networking and protocol

- The wire format is line-delimited JSON, one object per line, `\n`-ended.
  Framing lives only in `protocol.rs` (`encode`/`decode`); nothing else reads
  or writes raw bytes.
- Every frame carries a `type` field. `model.rs` uses a tagged serde enum
  (`#[serde(tag = "type", rename_all = "snake_case")]`) so adding a frame is
  one enum variant plus one match arm.
- The broker never blocks on a peer. Each connection runs its own task with a
  reader loop and a writer task. Slow or dead subscribers are dropped, not
  waited on.
- Client commands open a fresh connection per invocation. `watch` is the one
  exception: it holds the connection open and streams.
- State lives in memory behind a mutex. Cache reads tolerate absence: a
  missing session returns empty history, never a panic.

## Formatting

- rustfmt decides the layout (`rustfmt.toml`, 100 characters per line). Run
  `make fmt` before you commit. `make check` fails on unformatted code.
- Complexity limit: 40 per function (clippy `cognitive_complexity`, set in
  `clippy.toml`). `make check` fails above it. Split the function; do not add
  an `allow`.

## Tests

- Pure logic gets unit tests in its module (`protocol.rs`, `cli.rs`).
- Networking gets integration tests in `server.rs` against an ephemeral
  `127.0.0.1:0` listener, driving the real client — no mocks.
- Name tests after the behavior: `send_acks_and_lists_in_order`.
- No `#[should_panic]` tests: assert on the `Err` or `None` value instead.
  No `#[ignore]` tests.

## Refactors

- A refactor changes structure only, not behavior or wire format. Prove it:
  the tests pass without edits to their expected values.
- Make the smallest diff that does the job. Leave no scaffolding — no
  re-export shims, no `#[allow(...)]` hiding a leftover.
