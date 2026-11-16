//! chattrs — the broker: hello, send, and SQLite persistence.
//!
//! One `Broker` owns a `Store`. Each connection runs its own task with a
//! reader loop: the first frame must be `hello`, then the peer may `send`.
//! An error before `hello` closes the connection; after `hello` it does not.
//! Every accepted send is committed to SQLite before the broker writes `sent`.

use crate::model::{self, Inbound, Outbound};
use crate::protocol;
use crate::store::Store;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};

pub struct Broker {
    store: Store,
}

impl Broker {
    pub fn new(store: Store) -> Broker {
        Broker { store }
    }

    async fn handle(self: Arc<Self>, stream: TcpStream) {
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let mut peer: Option<String> = None;
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            if line.len() > model::MAX_LINE_BYTES {
                let frame = Outbound::error("too_large", "inbound line is too large");
                write_error(&mut write_half, frame).await;
                if peer.is_none() {
                    break;
                }
                continue;
            }
            let frame: Inbound = match protocol::decode(&line) {
                Ok(frame) => frame,
                Err(e) => {
                    let frame = Outbound::error("bad_frame", format!("bad frame: {e}"));
                    write_error(&mut write_half, frame).await;
                    if peer.is_none() {
                        break;
                    }
                    continue;
                }
            };
            match peer.clone() {
                None => match frame {
                    Inbound::Hello { v, address, .. } => {
                        if v != model::PROTOCOL_VERSION {
                            write_error(
                                &mut write_half,
                                Outbound::error("bad_version", "unsupported version, expected 1"),
                            )
                            .await;
                            break;
                        }
                        if !model::is_valid_address(&address) {
                            write_error(
                                &mut write_half,
                                Outbound::error("bad_address", "address is invalid"),
                            )
                            .await;
                            break;
                        }
                        peer = Some(address.clone());
                        write_frame(
                            &mut write_half,
                            &Outbound::Welcome {
                                server: protocol::SERVER_NAME.to_string(),
                                v: model::PROTOCOL_VERSION,
                                address,
                            },
                        )
                        .await;
                    }
                    Inbound::Send { .. } | Inbound::Inbox | Inbound::Ack { .. } => {
                        write_error(
                            &mut write_half,
                            Outbound::error("hello_required", "hello must be the first frame"),
                        )
                        .await;
                        break;
                    }
                },
                Some(from) => match frame {
                    Inbound::Hello { v, address, .. } => {
                        if v != model::PROTOCOL_VERSION {
                            write_error(
                                &mut write_half,
                                Outbound::error("bad_version", "unsupported version, expected 1"),
                            )
                            .await;
                            continue;
                        }
                        if !model::is_valid_address(&address) {
                            write_error(
                                &mut write_half,
                                Outbound::error("bad_address", "address is invalid"),
                            )
                            .await;
                            continue;
                        }
                        peer = Some(address.clone());
                        write_frame(
                            &mut write_half,
                            &Outbound::Welcome {
                                server: protocol::SERVER_NAME.to_string(),
                                v: model::PROTOCOL_VERSION,
                                address,
                            },
                        )
                        .await;
                    }
                    Inbound::Send { to, body, reply_to } => {
                        if !model::is_valid_address(&to) {
                            write_error(
                                &mut write_half,
                                Outbound::error("bad_address", "address is invalid"),
                            )
                            .await;
                            continue;
                        }
                        if body.is_empty() {
                            write_error(
                                &mut write_half,
                                Outbound::error("empty_body", "body is empty"),
                            )
                            .await;
                            continue;
                        }
                        if body.len() > model::MAX_BODY_BYTES {
                            write_error(
                                &mut write_half,
                                Outbound::error("too_large", "body is too large"),
                            )
                            .await;
                            continue;
                        }
                        match self.store.insert(from.clone(), to, body, reply_to).await {
                            Ok(id) => write_frame(&mut write_half, &Outbound::Sent { id }).await,
                            Err(e) => {
                                write_error(&mut write_half, Outbound::error("internal", e)).await;
                                continue;
                            }
                        }
                    }
                    Inbound::Inbox => {
                        match self.store.inbox(from.clone()).await {
                            Ok(messages) => {
                                write_frame(&mut write_half, &Outbound::Inbox { messages }).await;
                            }
                            Err(e) => {
                                write_error(&mut write_half, Outbound::error("internal", e)).await;
                                continue;
                            }
                        }
                    }
                    Inbound::Ack { ids } => {
                        match self.store.ack(from.clone(), ids).await {
                            Ok(ids) => {
                                write_frame(&mut write_half, &Outbound::Acked { ids }).await;
                            }
                            Err(e) => {
                                write_error(&mut write_half, Outbound::error("internal", e)).await;
                                continue;
                            }
                        }
                    }
                },
            }
        }
        let _ = write_half.shutdown().await;
    }
}

async fn write_frame(writer: &mut OwnedWriteHalf, frame: &Outbound) {
    let line = match protocol::encode(frame) {
        Ok(line) => line,
        Err(_) => return,
    };
    let _ = writer.write_all(line.as_bytes()).await;
    let _ = writer.flush().await;
}

async fn write_error(writer: &mut OwnedWriteHalf, frame: Outbound) {
    write_frame(writer, &frame).await;
}

/// Bind and serve until the process exits.
pub async fn serve(listen: &str, db: &Path) -> Result<(), String> {
    let listener = TcpListener::bind(listen).await.map_err(|e| format!("bind {listen}: {e}"))?;
    let store = Store::open(db.to_path_buf()).await?;
    println!("chattrs: serving on {listen}");
    let broker = Arc::new(Broker::new(store));
    run_listener(listener, broker).await
}

async fn run_listener(listener: TcpListener, broker: Arc<Broker>) -> Result<(), String> {
    loop {
        let (stream, _peer) = listener.accept().await.map_err(|e| e.to_string())?;
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            broker.handle(stream).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client;
    use tempfile::TempDir;
    use tokio::io::BufReader as TokioBufReader;
    use tokio::net::TcpStream as TokioTcpStream;

    async fn spawn() -> (String, TempDir, tokio::task::JoinHandle<()>) {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path().join("chattrs.db")).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let broker = Arc::new(Broker::new(store));
        let handle = tokio::spawn(async move {
            let _ = run_listener(listener, broker).await;
        });
        (addr, dir, handle)
    }

    async fn spawn_at(db: &Path) -> (String, tokio::task::JoinHandle<()>) {
        let store = Store::open(db.to_path_buf()).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let broker = Arc::new(Broker::new(store));
        let handle = tokio::spawn(async move {
            let _ = run_listener(listener, broker).await;
        });
        (addr, handle)
    }

    struct Raw {
        reader: TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
        writer: tokio::net::tcp::OwnedWriteHalf,
    }

    async fn raw_connect(addr: &str) -> Raw {
        let stream = TokioTcpStream::connect(addr).await.unwrap();
        let (read_half, write_half) = stream.into_split();
        Raw { reader: TokioBufReader::new(read_half), writer: write_half }
    }

    async fn raw_send(raw: &mut Raw, line: &str) {
        raw.writer.write_all(line.as_bytes()).await.unwrap();
        raw.writer.flush().await.unwrap();
    }

    async fn raw_recv(raw: &mut Raw) -> Result<Outbound, String> {
        let mut line = String::new();
        let n = raw.reader.read_line(&mut line).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("server closed the connection".to_string());
        }
        protocol::decode(&line)
    }

    async fn raw_hello(raw: &mut Raw, hello: &str) -> Outbound {
        raw_send(raw, hello).await;
        raw_recv(raw).await.unwrap()
    }

    fn error_code(frame: &Outbound) -> &str {
        match frame {
            Outbound::Error { code, .. } => code,
            other => panic!("expected an error frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn message_survives_broker_restart() {
        let (addr, dir, handle) = spawn().await;
        let db = dir.path().join("chattrs.db");
        let id = client::send(&addr, "a@h", None, "b@h", "hi", None).await.unwrap();
        handle.abort();

        let (addr, handle) = spawn_at(&db).await;
        let (sender, recipient, body): (String, String, String) =
            rusqlite::Connection::open(&db)
                .unwrap()
                .query_row(
                    "SELECT sender, recipient, body FROM messages WHERE id = ?1",
                    [id as i64],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .expect("row survives the restart");
        assert_eq!(sender, "a@h");
        assert_eq!(recipient, "b@h");
        assert_eq!(body, "hi");

        let next = client::send(&addr, "a@h", None, "b@h", "again", None).await.unwrap();
        assert_eq!(next, id + 1);
        handle.abort();
    }

    #[tokio::test]
    async fn send_returns_increasing_ids() {
        let (addr, _dir, handle) = spawn().await;
        let mut last = 0;
        for i in 0..5 {
            let id =
                client::send(&addr, "a@h", None, "b@h", &format!("msg {i}"), None).await.unwrap();
            assert!(id > last, "ids increase with send order");
            last = id;
        }
        handle.abort();
    }

    #[tokio::test]
    async fn first_frame_must_be_hello() {
        let (addr, _dir, handle) = spawn().await;
        let mut raw = raw_connect(&addr).await;
        raw_send(&mut raw, "{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"hi\",\"reply_to\":null}\n")
            .await;
        let frame = raw_recv(&mut raw).await.unwrap();
        assert_eq!(error_code(&frame), "hello_required");
        assert!(raw_recv(&mut raw).await.is_err(), "connection closes after hello_required");
        handle.abort();
    }

    #[tokio::test]
    async fn hello_rejects_wrong_version() {
        let (addr, _dir, handle) = spawn().await;
        let mut raw = raw_connect(&addr).await;
        let frame =
            raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":2,\"address\":\"a@h\"}\n").await;
        assert_eq!(error_code(&frame), "bad_version");
        handle.abort();
    }

    #[tokio::test]
    async fn hello_rejects_bad_address() {
        let (addr, _dir, handle) = spawn().await;
        for address in ["not-an-address", "a@", "@h", "a@b@c"] {
            let mut raw = raw_connect(&addr).await;
            let hello = format!("{{\"type\":\"hello\",\"v\":1,\"address\":\"{address}\"}}\n");
            let frame = raw_hello(&mut raw, &hello).await;
            assert_eq!(error_code(&frame), "bad_address", "for {address}");
        }
        handle.abort();
    }

    #[tokio::test]
    async fn send_rejects_empty_body() {
        let (addr, _dir, handle) = spawn().await;
        let err = client::send(&addr, "a@h", None, "b@h", "", None).await.unwrap_err();
        assert!(err.contains("empty_body"), "got: {err}");
        handle.abort();
    }

    #[tokio::test]
    async fn send_rejects_oversize_body() {
        let (addr, _dir, handle) = spawn().await;
        let body = "x".repeat(model::MAX_BODY_BYTES + 1);
        let err = client::send(&addr, "a@h", None, "b@h", &body, None).await.unwrap_err();
        assert!(err.contains("too_large"), "got: {err}");
        handle.abort();
    }

    #[tokio::test]
    async fn inbox_returns_only_messages_for_own_address() {
        let (addr, _dir, handle) = spawn().await;
        let to_b = client::send(&addr, "a@h", None, "b@h", "for b", None).await.unwrap();
        let to_c = client::send(&addr, "a@h", None, "c@h", "for c", None).await.unwrap();

        let b_messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(b_messages.len(), 1);
        assert_eq!(b_messages[0].id, to_b);
        assert_eq!(b_messages[0].from, "a@h");
        assert_eq!(b_messages[0].to, "b@h");
        assert_eq!(b_messages[0].body, "for b");

        let c_messages = client::fetch_inbox(&addr, "c@h", None).await.unwrap();
        assert_eq!(c_messages.len(), 1);
        assert_eq!(c_messages[0].id, to_c);
        assert_eq!(c_messages[0].to, "c@h");
        assert_eq!(c_messages[0].body, "for c");
        handle.abort();
    }

    #[tokio::test]
    async fn inbox_returns_messages_in_id_order() {
        let (addr, _dir, handle) = spawn().await;
        let mut expected = Vec::new();
        for i in 0..3 {
            let id =
                client::send(&addr, "a@h", None, "b@h", &format!("msg {i}"), None).await.unwrap();
            expected.push(id);
        }
        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        let ids: Vec<u64> = messages.iter().map(|message| message.id).collect();
        assert_eq!(ids, expected);
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "ids grow in id order");
        handle.abort();
    }

    #[tokio::test]
    async fn unacked_message_is_redelivered_after_disconnect() {
        let (addr, _dir, handle) = spawn().await;
        let id = client::send(&addr, "a@h", None, "b@h", "hi", None).await.unwrap();

        let mut raw = raw_connect(&addr).await;
        raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"b@h\"}\n").await;
        raw_send(&mut raw, "{\"type\":\"inbox\"}\n").await;
        match raw_recv(&mut raw).await.unwrap() {
            Outbound::Inbox { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, id);
            }
            other => panic!("expected an inbox frame, got {other:?}"),
        }
        drop(raw);

        let again = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].id, id);
        assert_eq!(again[0].body, "hi");
        handle.abort();
    }

    #[tokio::test]
    async fn acked_message_is_not_redelivered() {
        let (addr, _dir, handle) = spawn().await;
        let id = client::send(&addr, "a@h", None, "b@h", "hi", None).await.unwrap();

        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        let acked = client::ack(&addr, "b@h", None, &[id]).await.unwrap();
        assert_eq!(acked, vec![id]);

        let again = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert!(again.is_empty(), "acked message stays gone: {again:?}");
        handle.abort();
    }

    #[tokio::test]
    async fn ack_ignores_ids_of_other_addresses() {
        let (addr, _dir, handle) = spawn().await;
        let to_b = client::send(&addr, "a@h", None, "b@h", "for b", None).await.unwrap();
        let to_c = client::send(&addr, "a@h", None, "c@h", "for c", None).await.unwrap();

        let acked = client::ack(&addr, "b@h", None, &[to_b, to_c]).await.unwrap();
        assert_eq!(acked, vec![to_b], "only the own id changes");

        let c_messages = client::fetch_inbox(&addr, "c@h", None).await.unwrap();
        assert_eq!(c_messages.len(), 1);
        assert_eq!(c_messages[0].id, to_c);
        handle.abort();
    }

    #[tokio::test]
    async fn message_to_offline_address_survives_restart_and_arrives_once() {
        let (addr, dir, handle) = spawn().await;
        let db = dir.path().join("chattrs.db");
        let id = client::send(&addr, "a@h", None, "b@h", "hi", None).await.unwrap();
        handle.abort();

        let (addr, handle) = spawn_at(&db).await;
        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].from, "a@h");
        assert_eq!(messages[0].to, "b@h");
        assert_eq!(messages[0].body, "hi");

        let acked = client::ack(&addr, "b@h", None, &[id]).await.unwrap();
        assert_eq!(acked, vec![id]);
        let again = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert!(again.is_empty(), "message arrives once: {again:?}");
        handle.abort();
    }
}
