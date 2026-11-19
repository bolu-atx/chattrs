//! Broker integration tests, split by subject. Helpers live in
//! `helpers`; every test drives the real broker or the built binary.

mod ask;
mod auth;
mod autostart;
mod helpers;
mod log;
mod mailbox;
mod retry;
mod topics;
mod watch;
