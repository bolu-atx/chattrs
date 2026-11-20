//! Broker integration tests, split by subject. Helpers live in
//! `fixture`; every test drives the real broker or the built binary.

mod ask;
mod auth;
mod autostart;
mod fixture;
mod log;
mod mailbox;
mod observer;
mod projects;
mod retry;
mod session;
mod state;
mod tags;
mod tokens;
mod topics;
mod watch;
