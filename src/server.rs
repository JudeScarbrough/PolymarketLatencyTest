use crate::reconnect::Backoff;
use crate::update::ClientMessage;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::interval;
use tokio_tungstenite::{accept_async, tungstenite::Message};

const DEFAULT_ADDR: &str = "0.0.0.0:8080";
const CLIENT_PING_INTERVAL: Duration = Duration::from_secs(30);

pub async fn run_server(broadcast_tx: broadcast::Sender<ClientMessage>) -> Result<()> {
    let mut backoff = Backoff::new();

    loop {
        let addr: SocketAddr = std::env::var("WS_ADDR")
            .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
            .parse()
            .context("invalid WS_ADDR")?;

        match run_listener(addr, broadcast_tx.clone()).await {
            Ok(()) => eprintln!("client server: listener exited, restarting..."),
            Err(err) => eprintln!("client server: {err:#}"),
        }

        eprintln!(
            "client server: restarting in {:?}...",
            backoff.delay()
        );
        backoff.wait().await;
    }
}

async fn run_listener(
    addr: SocketAddr,
    broadcast_tx: broadcast::Sender<ClientMessage>,
) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind websocket server to {addr}"))?;

    eprintln!("client websocket server listening on ws://{addr}");
    if addr.ip().is_unspecified() {
        eprintln!("client websocket server: accepting connections on all interfaces (use your instance public IP in clients)");
    }

    loop {
        let (stream, peer) = listener.accept().await.context("accept failed")?;
        if let Err(err) = stream.set_nodelay(true) {
            eprintln!("client {peer}: failed to set TCP_NODELAY: {err}");
        }

        let rx = broadcast_tx.subscribe();
        tokio::spawn(async move {
            if let Err(err) = handle_client(stream, rx).await {
                eprintln!("client {peer} disconnected: {err:#}");
            } else {
                eprintln!("client {peer} disconnected");
            }
        });
    }
}

async fn handle_client(
    stream: TcpStream,
    mut rx: broadcast::Receiver<ClientMessage>,
) -> Result<()> {
    let ws = accept_async(stream)
        .await
        .context("websocket handshake failed")?;
    let (mut write, mut read) = ws.split();
    let mut ping = interval(CLIENT_PING_INTERVAL);
    ping.tick().await;

    loop {
        tokio::select! {
            _ = ping.tick() => {
                write.send(Message::Ping(Vec::new().into())).await?;
            }
            update = rx.recv() => {
                match update {
                    Ok(update) => {
                        let payload = serde_json::to_string(&update)?;
                        if write.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        eprintln!("client lagged, skipped {skipped} updates");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(payload))) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err.into()),
                }
            }
        }
    }

    let _ = write.send(Message::Close(None)).await;
    Ok(())
}
