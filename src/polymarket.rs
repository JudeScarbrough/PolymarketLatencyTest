use crate::gamma::UpMarket;
use crate::orderbook::{OrderBook, TopOfBook};
use crate::reconnect::Backoff;
use crate::update::{ClientMessage, MarketUpdate};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio::time::{interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const PING_INTERVAL: Duration = Duration::from_secs(10);

pub async fn run_polymarket_feed(
    mut market_rx: watch::Receiver<Vec<UpMarket>>,
    broadcast_tx: broadcast::Sender<ClientMessage>,
) -> Result<()> {
    let mut backoff = Backoff::new();

    loop {
        if market_rx.changed().await.is_err() {
            sleep(backoff.delay()).await;
            continue;
        }

        let markets = market_rx.borrow_and_update().clone();
        if markets.is_empty() {
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        eprintln!(
            "polymarket: subscribing to {} market(s)",
            markets.len()
        );
        for market in &markets {
            eprintln!(
                "  - [{}] {} ({})",
                market.timeframe.label(),
                market.title,
                market.slug
            );
        }

        loop {
            tokio::select! {
                changed = market_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    break;
                }
                result = stream_once(&markets, &broadcast_tx) => {
                    match result {
                        Ok(()) => {
                            backoff.reset();
                            break;
                        }
                        Err(err) => {
                            eprintln!("polymarket: {err:#}");
                            eprintln!(
                                "polymarket: reconnecting in {:?}...",
                                backoff.delay()
                            );
                            backoff.wait().await;
                        }
                    }
                }
            }
        }
    }
}

async fn stream_once(
    markets: &[UpMarket],
    broadcast_tx: &broadcast::Sender<ClientMessage>,
) -> Result<()> {
    let (ws, _) = connect_async(WS_URL)
        .await
        .context("failed to connect to polymarket market websocket")?;

    eprintln!("polymarket: connected");

    let (mut write, mut read) = ws.split();

    let asset_ids: Vec<String> = markets.iter().map(|m| m.up_token_id.clone()).collect();
    let subscribe = json!({
        "assets_ids": asset_ids,
        "type": "market",
        "initial_dump": true,
        "level": 2
    });
    write
        .send(Message::Text(subscribe.to_string().into()))
        .await
        .context("failed to send subscription message")?;

    let market_by_asset: HashMap<String, UpMarket> = markets
        .iter()
        .map(|m| (m.up_token_id.clone(), m.clone()))
        .collect();

    let mut books: HashMap<String, OrderBook> = HashMap::new();
    let mut tops: HashMap<String, TopOfBook> = HashMap::new();
    let mut ping = interval(PING_INTERVAL);
    ping.tick().await;

    loop {
        tokio::select! {
            _ = ping.tick() => {
                write.send(Message::Text("PING".into())).await?;
            }
            msg = read.next() => {
                let Some(msg) = msg else {
                    anyhow::bail!("stream closed");
                };

                match msg.context("websocket read failed")? {
                    Message::Text(text) => {
                        if text == "PONG" {
                            continue;
                        }
                        handle_text(&text, &market_by_asset, &mut books, &mut tops, broadcast_tx);
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

fn handle_text(
    text: &str,
    market_by_asset: &HashMap<String, UpMarket>,
    books: &mut HashMap<String, OrderBook>,
    tops: &mut HashMap<String, TopOfBook>,
    broadcast_tx: &broadcast::Sender<ClientMessage>,
) {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return;
    };

    match value {
        Value::Array(items) => {
            for item in items {
                handle_event(&item, market_by_asset, books, tops, broadcast_tx);
            }
        }
        Value::Object(_) => handle_event(&value, market_by_asset, books, tops, broadcast_tx),
        _ => {}
    }
}

fn handle_event(
    value: &Value,
    market_by_asset: &HashMap<String, UpMarket>,
    books: &mut HashMap<String, OrderBook>,
    tops: &mut HashMap<String, TopOfBook>,
    broadcast_tx: &broadcast::Sender<ClientMessage>,
) {
    let Some(event_type) = value.get("event_type").and_then(Value::as_str) else {
        return;
    };

    match event_type {
        "book" => {
            if let Ok(event) = serde_json::from_value::<BookEvent>(value.clone()) {
                if let Some(market) = market_by_asset.get(&event.asset_id) {
                    let book = books.entry(event.asset_id.clone()).or_default();
                    book.apply_snapshot(
                        &event
                            .bids
                            .iter()
                            .map(|l| (l.price.clone(), l.size.clone()))
                            .collect::<Vec<_>>(),
                        &event
                            .asks
                            .iter()
                            .map(|l| (l.price.clone(), l.size.clone()))
                            .collect::<Vec<_>>(),
                    );
                    maybe_broadcast(market, book, tops, broadcast_tx);
                }
            }
        }
        "price_change" => {
            if let Ok(event) = serde_json::from_value::<PriceChangeEvent>(value.clone()) {
                for change in event.price_changes {
                    let Some(market) = market_by_asset.get(&change.asset_id) else {
                        continue;
                    };

                    let book = books.entry(change.asset_id.clone()).or_default();
                    book.apply_delta(&change.side, &change.price, &change.size);
                    book.prune_to_authoritative(change.best_bid.as_deref(), change.best_ask.as_deref());
                    maybe_broadcast(market, book, tops, broadcast_tx);
                }
            }
        }
        _ => {}
    }
}

fn maybe_broadcast(
    market: &UpMarket,
    book: &OrderBook,
    tops: &mut HashMap<String, TopOfBook>,
    broadcast_tx: &broadcast::Sender<ClientMessage>,
) {
    let top = book.top_of_book();
    let previous = tops.insert(market.up_token_id.clone(), top);

    if previous != Some(top) {
        if let Some(update) = MarketUpdate::from_top(market, top) {
            let _ = broadcast_tx.send(update.into());
        }
    }
}

#[derive(Debug, Deserialize)]
struct BookEvent {
    asset_id: String,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

#[derive(Debug, Deserialize)]
struct Level {
    price: String,
    size: String,
}

#[derive(Debug, Deserialize)]
struct PriceChangeEvent {
    price_changes: Vec<PriceChange>,
}

#[derive(Debug, Deserialize)]
struct PriceChange {
    asset_id: String,
    price: String,
    size: String,
    side: String,
    best_bid: Option<String>,
    best_ask: Option<String>,
}
