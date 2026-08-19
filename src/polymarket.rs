use crate::gamma::UpMarket;
use crate::orderbook::{OrderBook, TopOfBook};
use crate::reconnect::Backoff;
use crate::update::{ClientMessage, MarketUpdate};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch, RwLock};
use tokio::time::{interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const PING_INTERVAL: Duration = Duration::from_secs(10);
const MESSAGE_CHANNEL_CAPACITY: usize = 65_536;
const OUTGOING_CHANNEL_CAPACITY: usize = 256;

enum Outgoing {
    Text(String),
    Pong(Vec<u8>),
}

struct FeedState {
    market_by_asset: HashMap<String, UpMarket>,
    books: HashMap<String, OrderBook>,
    tops: HashMap<String, TopOfBook>,
}

impl FeedState {
    fn reset(&mut self, markets: &[UpMarket]) {
        self.market_by_asset = markets
            .iter()
            .map(|m| (m.up_token_id.clone(), m.clone()))
            .collect();
        self.books.clear();
        self.tops.clear();
    }

    fn apply_market_change(&mut self, markets: &[UpMarket]) {
        let next_ids: HashSet<_> = markets.iter().map(|m| m.up_token_id.as_str()).collect();

        self.books.retain(|id, _| next_ids.contains(id.as_str()));
        self.tops.retain(|id, _| next_ids.contains(id.as_str()));
        self.market_by_asset = markets
            .iter()
            .map(|m| (m.up_token_id.clone(), m.clone()))
            .collect();
    }
}

pub async fn run_polymarket_feed(
    mut market_rx: watch::Receiver<Vec<UpMarket>>,
    broadcast_tx: broadcast::Sender<ClientMessage>,
) -> Result<()> {
    let mut backoff = Backoff::new();

    loop {
        while market_rx.borrow().is_empty() {
            eprintln!("polymarket: waiting for subscribable markets...");
            if market_rx.changed().await.is_err() {
                sleep(backoff.delay()).await;
                continue;
            }
        }

        let markets = market_rx.borrow_and_update().clone();
        log_subscription("connecting with", &markets);

        match run_connection(&mut market_rx, &markets, &broadcast_tx).await {
            Ok(()) => {
                backoff.reset();
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

async fn run_connection(
    market_rx: &mut watch::Receiver<Vec<UpMarket>>,
    initial_markets: &[UpMarket],
    broadcast_tx: &broadcast::Sender<ClientMessage>,
) -> Result<()> {
    let (ws, _) = connect_async(WS_URL)
        .await
        .context("failed to connect to polymarket market websocket")?;

    eprintln!("polymarket: connected");

    let (mut write, mut read) = ws.split();
    let (out_tx, mut out_rx) = mpsc::channel(OUTGOING_CHANNEL_CAPACITY);
    let (text_tx, text_rx) = mpsc::channel(MESSAGE_CHANNEL_CAPACITY);

    let state = Arc::new(RwLock::new(FeedState {
        market_by_asset: HashMap::new(),
        books: HashMap::new(),
        tops: HashMap::new(),
    }));

    {
        let mut guard = state.write().await;
        guard.reset(initial_markets);
    }

    subscribe_assets_initial(&out_tx, initial_markets).await?;

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            let result = match msg {
                Outgoing::Text(text) => write.send(Message::Text(text.into())).await,
                Outgoing::Pong(payload) => write.send(Message::Pong(payload.into())).await,
            };
            if result.is_err() {
                break;
            }
        }
    });

    let out_for_reader = out_tx.clone();
    let (dead_tx, mut dead_rx) = mpsc::channel::<&'static str>(4);

    let dead_for_reader = dead_tx.clone();
    let reader = tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) if text == "PONG" => {}
                Ok(Message::Text(text)) => {
                    if text_tx.send(text.to_string()).await.is_err() {
                        break;
                    }
                }
                Ok(Message::Ping(payload)) => {
                    let _ = out_for_reader
                        .send(Outgoing::Pong(payload.to_vec()))
                        .await;
                }
                Ok(Message::Pong(_)) => {}
                Ok(Message::Close(_)) => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }
        let _ = dead_for_reader.send("reader").await;
    });

    let state_for_processor = state.clone();
    let broadcast_for_processor = broadcast_tx.clone();
    let dead_for_processor = dead_tx.clone();
    let processor = tokio::spawn(async move {
        process_messages(text_rx, state_for_processor, broadcast_for_processor).await;
        let _ = dead_for_processor.send("processor").await;
    });

    drop(dead_tx);

    let mut ping = interval(PING_INTERVAL);
    ping.tick().await;
    let mut subscribed: HashSet<String> = initial_markets
        .iter()
        .map(|m| m.up_token_id.clone())
        .collect();

    loop {
        tokio::select! {
            _ = ping.tick() => {
                out_tx
                    .send(Outgoing::Text("PING".into()))
                    .await
                    .context("failed to queue ping")?;
            }
            changed = market_rx.changed() => {
                if changed.is_err() {
                    break;
                }

                let markets = market_rx.borrow_and_update().clone();
                if markets.is_empty() {
                    eprintln!("polymarket: all markets closed, disconnecting");
                    break;
                }

                let next_ids: HashSet<_> = markets.iter().map(|m| m.up_token_id.clone()).collect();
                let removed: Vec<_> = subscribed.difference(&next_ids).cloned().collect();
                let added: Vec<_> = next_ids.difference(&subscribed).cloned().collect();

                if removed.is_empty() && added.is_empty() {
                    continue;
                }

                log_subscription("updating subscriptions", &markets);

                if !removed.is_empty() {
                    unsubscribe_assets(&out_tx, &removed).await?;
                }
                if !added.is_empty() {
                    let added_set: HashSet<String> = added.iter().cloned().collect();
                    subscribe_assets(&out_tx, &markets_for_assets(&markets, &added_set)).await?;
                }

                {
                    let mut guard = state.write().await;
                    guard.apply_market_change(&markets);
                }

                subscribed = next_ids;
            }
            dead = dead_rx.recv() => {
                match dead {
                    Some(who) => anyhow::bail!("polymarket {who} task stopped"),
                    None => anyhow::bail!("feed task status channel closed"),
                }
            }
        }
    }

    drop(out_tx);
    let _ = writer.await;
    let _ = reader.await;
    let _ = processor.await;
    Ok(())
}

async fn process_messages(
    mut text_rx: mpsc::Receiver<String>,
    state: Arc<RwLock<FeedState>>,
    broadcast_tx: broadcast::Sender<ClientMessage>,
) {
    while let Some(text) = text_rx.recv().await {
        handle_text(&text, &state, &broadcast_tx).await;
    }
}

async fn handle_text(
    text: &str,
    state: &Arc<RwLock<FeedState>>,
    broadcast_tx: &broadcast::Sender<ClientMessage>,
) {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return;
    };

    match value {
        Value::Array(items) => {
            for item in items {
                handle_event(&item, state, broadcast_tx).await;
            }
        }
        Value::Object(_) => handle_event(&value, state, broadcast_tx).await,
        _ => {}
    }
}

async fn handle_event(
    value: &Value,
    state: &Arc<RwLock<FeedState>>,
    broadcast_tx: &broadcast::Sender<ClientMessage>,
) {
    let Some(event_type) = value.get("event_type").and_then(Value::as_str) else {
        return;
    };

    match event_type {
        "book" => {
            if let Ok(event) = serde_json::from_value::<BookEvent>(value.clone()) {
                let mut guard = state.write().await;
                let Some(market) = guard.market_by_asset.get(&event.asset_id).cloned() else {
                    return;
                };
                let book = guard.books.entry(event.asset_id.clone()).or_default();
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
                let top = book.top_of_book();
                maybe_broadcast(&market, top, &mut guard.tops, broadcast_tx);
            }
        }
        "price_change" => {
            if let Ok(event) = serde_json::from_value::<PriceChangeEvent>(value.clone()) {
                let mut guard = state.write().await;
                for change in event.price_changes {
                    let Some(market) = guard.market_by_asset.get(&change.asset_id).cloned() else {
                        continue;
                    };

                    let book = guard.books.entry(change.asset_id.clone()).or_default();
                    book.apply_delta(&change.side, &change.price, &change.size);
                    book.prune_to_authoritative(
                        change.best_bid.as_deref(),
                        change.best_ask.as_deref(),
                    );
                    let top = book.top_of_book();
                    maybe_broadcast(&market, top, &mut guard.tops, broadcast_tx);
                }
            }
        }
        _ => {}
    }
}

fn maybe_broadcast(
    market: &UpMarket,
    top: TopOfBook,
    tops: &mut HashMap<String, TopOfBook>,
    broadcast_tx: &broadcast::Sender<ClientMessage>,
) {
    let previous = tops.insert(market.up_token_id.clone(), top);

    if previous != Some(top) {
        if let Some(update) = MarketUpdate::from_top(market, top) {
            let _ = broadcast_tx.send(update.into());
        }
    }
}

async fn subscribe_assets_initial(
    out_tx: &mpsc::Sender<Outgoing>,
    markets: &[UpMarket],
) -> Result<()> {
    if markets.is_empty() {
        return Ok(());
    }

    let asset_ids: Vec<String> = markets.iter().map(|m| m.up_token_id.clone()).collect();
    let payload = json!({
        "assets_ids": asset_ids,
        "type": "market",
        "initial_dump": true,
        "level": 2
    });

    out_tx
        .send(Outgoing::Text(payload.to_string()))
        .await
        .context("failed to queue initial subscribe message")?;
    Ok(())
}

async fn subscribe_assets(out_tx: &mpsc::Sender<Outgoing>, markets: &[UpMarket]) -> Result<()> {
    if markets.is_empty() {
        return Ok(());
    }

    let asset_ids: Vec<String> = markets.iter().map(|m| m.up_token_id.clone()).collect();
    let payload = json!({
        "assets_ids": asset_ids,
        "operation": "subscribe",
        "type": "market",
        "initial_dump": true,
        "level": 2
    });

    out_tx
        .send(Outgoing::Text(payload.to_string()))
        .await
        .context("failed to queue subscribe message")?;
    Ok(())
}

async fn unsubscribe_assets(out_tx: &mpsc::Sender<Outgoing>, asset_ids: &[String]) -> Result<()> {
    if asset_ids.is_empty() {
        return Ok(());
    }

    let payload = json!({
        "assets_ids": asset_ids,
        "operation": "unsubscribe",
    });

    out_tx
        .send(Outgoing::Text(payload.to_string()))
        .await
        .context("failed to queue unsubscribe message")?;
    Ok(())
}

fn markets_for_assets(markets: &[UpMarket], asset_ids: &HashSet<String>) -> Vec<UpMarket> {
    markets
        .iter()
        .filter(|m| asset_ids.contains(&m.up_token_id))
        .cloned()
        .collect()
}

fn log_subscription(label: &str, markets: &[UpMarket]) {
    eprintln!("polymarket: {label} {} market(s)", markets.len());
    for market in markets {
        eprintln!(
            "  - [{}] {} ({})",
            market.timeframe.label(),
            market.title,
            market.slug
        );
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
