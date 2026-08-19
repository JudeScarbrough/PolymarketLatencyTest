use crate::update::{parse_custom_payload, ClientMessage};
use crate::reconnect::Backoff;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::interval;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const DEFAULT_IP_FILE: &str = "ip.txt";
const CLIENT_PING_INTERVAL: Duration = Duration::from_secs(30);

pub fn load_custom_feed_addr() -> Result<String> {
    let path = ip_file_path();
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    parse_addr(&contents).with_context(|| format!("invalid address in {}", path.display()))
}

pub async fn run_custom_feed(broadcast_tx: broadcast::Sender<ClientMessage>) -> Result<()> {
    let mut backoff = Backoff::new();

    loop {
        let addr = match load_custom_feed_addr() {
            Ok(addr) => addr,
            Err(err) => {
                eprintln!("custom feed: {err:#}");
                eprintln!("custom feed: retrying in {:?}...", backoff.delay());
                backoff.wait().await;
                continue;
            }
        };

        let url = to_websocket_url(&addr);
        eprintln!("custom feed: connecting to {url}");

        match stream_once(&url, &broadcast_tx).await {
            Ok(()) => {
                eprintln!("custom feed: stream ended, reconnecting...");
                backoff.reset();
            }
            Err(err) => {
                eprintln!("custom feed: {err:#}");
                eprintln!("custom feed: reconnecting in {:?}...", backoff.delay());
                backoff.wait().await;
            }
        }
    }
}

async fn stream_once(url: &str, broadcast_tx: &broadcast::Sender<ClientMessage>) -> Result<()> {
    let (ws, _) = connect_async(url)
        .await
        .with_context(|| format!("failed to connect to custom websocket at {url}"))?;
    eprintln!("custom feed: connected to {url}");

    let (mut write, mut read) = ws.split();
    let mut ping = interval(CLIENT_PING_INTERVAL);
    ping.tick().await;

    loop {
        tokio::select! {
            _ = ping.tick() => {
                write.send(Message::Ping(Vec::new().into())).await?;
            }
            msg = read.next() => {
                let Some(msg) = msg else {
                    anyhow::bail!("stream closed");
                };

                match msg.context("custom feed read failed")? {
                    Message::Text(text) => {
                        let payload = parse_custom_payload(&text);
                        let _ = broadcast_tx.send(ClientMessage::custom(payload));
                    }
                    Message::Binary(data) => {
                        let text = String::from_utf8_lossy(&data);
                        let payload = parse_custom_payload(&text);
                        let _ = broadcast_tx.send(ClientMessage::custom(payload));
                    }
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(frame) => {
                        anyhow::bail!("closed by peer: {frame:?}");
                    }
                    _ => {}
                }
            }
        }
    }
}

fn ip_file_path() -> PathBuf {
    std::env::var("IP_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_IP_FILE))
}

fn to_websocket_url(addr: &str) -> String {
    if addr.starts_with("ws://") || addr.starts_with("wss://") {
        addr.to_string()
    } else {
        format!("ws://{addr}")
    }
}

fn parse_addr(raw: &str) -> Result<String> {
    let mut addr = raw.trim();
    anyhow::ensure!(!addr.is_empty(), "address is empty");

    for prefix in ["ws://", "wss://"] {
        if let Some(stripped) = addr.strip_prefix(prefix) {
            addr = stripped;
            break;
        }
    }

    let (host, port) = addr
        .rsplit_once(':')
        .context("address must be host:port")?;
    anyhow::ensure!(!host.is_empty(), "host is empty");
    anyhow::ensure!(!port.is_empty(), "port is empty");
    anyhow::ensure!(port.chars().all(|c| c.is_ascii_digit()), "invalid port");

    Ok(format!("{host}:{port}"))
}

#[cfg(test)]
mod tests {
    use super::{parse_addr, to_websocket_url};

    #[test]
    fn parses_host_port() {
        assert_eq!(parse_addr("34.0.0.8:9000").unwrap(), "34.0.0.8:9000");
        assert_eq!(
            parse_addr("ws://127.0.0.1:8765").unwrap(),
            "127.0.0.1:8765"
        );
    }

    #[test]
    fn builds_websocket_url() {
        assert_eq!(
            to_websocket_url("127.0.0.1:8765"),
            "ws://127.0.0.1:8765"
        );
    }
}
