use crate::update::{parse_custom_payload, ClientMessage, SharedQuotes};
use crate::reconnect::Backoff;
use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::interval;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const DEFAULT_IP_FILE: &str = "ip.txt";
const CLIENT_PING_INTERVAL: Duration = Duration::from_secs(30);
const LATENCY_PING_INTERVAL: Duration = Duration::from_millis(100);

pub fn load_custom_feed_addr() -> Result<String> {
    let path = ip_file_path();
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    parse_addr(&contents).with_context(|| format!("invalid address in {}", path.display()))
}

pub async fn run_custom_feed(
    broadcast_tx: broadcast::Sender<ClientMessage>,
    quotes: SharedQuotes,
) -> Result<()> {
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

        match stream_once(&url, &broadcast_tx, &quotes).await {
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

async fn stream_once(
    url: &str,
    broadcast_tx: &broadcast::Sender<ClientMessage>,
    quotes: &SharedQuotes,
) -> Result<()> {
    let (ws, _) = connect_async(url)
        .await
        .with_context(|| format!("failed to connect to custom websocket at {url}"))?;
    eprintln!("custom feed: connected to {url}");

    let (mut write, mut read) = ws.split();
    let mut ping = interval(CLIENT_PING_INTERVAL);
    let mut latency_ping = interval(LATENCY_PING_INTERVAL);
    ping.tick().await;
    latency_ping.tick().await;

    loop {
        tokio::select! {
            _ = ping.tick() => {
                write.send(Message::Ping(Vec::new().into())).await?;
            }
            _ = latency_ping.tick() => {
                let body = json!({
                    "type": "ping",
                    "t": Utc::now().timestamp_millis(),
                })
                .to_string();
                write.send(Message::Text(body.into())).await?;
            }
            msg = read.next() => {
                let Some(msg) = msg else {
                    anyhow::bail!("stream closed");
                };

                match msg.context("custom feed read failed")? {
                    Message::Text(text) => {
                        handle_feed_text(&text, quotes, broadcast_tx);
                    }
                    Message::Binary(data) => {
                        let text = String::from_utf8_lossy(&data);
                        handle_feed_text(&text, quotes, broadcast_tx);
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

fn handle_feed_text(
    text: &str,
    quotes: &SharedQuotes,
    broadcast_tx: &broadcast::Sender<ClientMessage>,
) {
    if let Some(sent_at_ms) = parse_pong(text) {
        let latency_ms = Utc::now().timestamp_millis() - sent_at_ms;
        let latest = read_quotes(quotes);
        let _ = broadcast_tx.send(ClientMessage::with_latency(latest, latency_ms));
        return;
    }
    forward_custom(text, quotes, broadcast_tx);
}

fn parse_pong(text: &str) -> Option<i64> {
    let value: Value = serde_json::from_str(text).ok()?;
    let object = value.as_object()?;
    if object.get("type")?.as_str()? != "pong" {
        return None;
    }
    match object.get("t")? {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|n| i64::try_from(n).ok()))
            .or_else(|| number.as_f64().map(|n| n as i64)),
        _ => None,
    }
}

fn read_quotes(quotes: &SharedQuotes) -> crate::update::LatestQuotes {
    match quotes.read() {
        Ok(guard) => *guard,
        Err(err) => *err.into_inner(),
    }
}

fn forward_custom(
    text: &str,
    quotes: &SharedQuotes,
    broadcast_tx: &broadcast::Sender<ClientMessage>,
) {
    let payload = parse_custom_payload(text);
    let latest = read_quotes(quotes);
    let _ = broadcast_tx.send(ClientMessage::custom(payload, latest));
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
    use super::{parse_addr, parse_pong, to_websocket_url};

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

    #[test]
    fn parses_pong_timestamp() {
        assert_eq!(
            parse_pong(r#"{"type":"pong","t":1710000000123}"#),
            Some(1710000000123)
        );
        assert_eq!(parse_pong(r#"{"type":"ping","t":1710000000123}"#), None);
        assert_eq!(parse_pong(r#"{"ask":1.0,"bid":1.0,"exch":"binance"}"#), None);
    }
}
