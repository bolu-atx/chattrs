//! chattrs — client: one TCP connection per command.
//!
//! Every command opens a fresh connection, sends a `hello`, then the command
//! frame, and reads the response.

use crate::model::{self, Inbound, Outbound};
use crate::protocol;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

struct Conn {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

async fn connect(server: &str) -> Result<Conn, String> {
    let stream = TcpStream::connect(server).await.map_err(|e| format!("connect {server}: {e}"))?;
    let (read_half, write_half) = stream.into_split();
    Ok(Conn { reader: BufReader::new(read_half), writer: write_half })
}

async fn send_frame(conn: &mut Conn, frame: &Inbound) -> Result<(), String> {
    let line = protocol::encode(frame)?;
    conn.writer.write_all(line.as_bytes()).await.map_err(|e| e.to_string())?;
    conn.writer.flush().await.map_err(|e| e.to_string())
}

async fn recv_frame(conn: &mut Conn) -> Result<Outbound, String> {
    let mut line = String::new();
    let n = conn.reader.read_line(&mut line).await.map_err(|e| e.to_string())?;
    if n == 0 {
        return Err("server closed the connection".to_string());
    }
    protocol::decode(&line)
}

/// Publish one message and return its id.
pub async fn send(
    server: &str,
    address: &str,
    token: Option<&str>,
    to: &str,
    body: &str,
    reply_to: Option<u64>,
) -> Result<u64, String> {
    let mut conn = connect(server).await?;
    send_frame(
        &mut conn,
        &Inbound::Hello {
            v: model::PROTOCOL_VERSION,
            address: address.to_string(),
            token: token.map(|token| token.to_string()),
        },
    )
    .await?;
    match recv_frame(&mut conn).await? {
        Outbound::Welcome { .. } => {}
        Outbound::Error { code, message } => return Err(format!("{code}: {message}")),
        _ => return Err("server sent an unexpected frame instead of welcome".to_string()),
    }
    send_frame(&mut conn, &Inbound::Send { to: to.to_string(), body: body.to_string(), reply_to })
        .await?;
    match recv_frame(&mut conn).await? {
        Outbound::Sent { id } => Ok(id),
        Outbound::Error { code, message } => Err(format!("{code}: {message}")),
        _ => Err("server sent an unexpected frame instead of sent".to_string()),
    }
}
