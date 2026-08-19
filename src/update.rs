use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::gamma::{Timeframe, UpMarket};
use crate::orderbook::TopOfBook;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientMessage {
    Market {
        market_start_time: i64,
        market_type: String,
        bid: Option<f64>,
        ask: Option<f64>,
        timestamp: i64,
    },
    Custom {
        timestamp: i64,
        payload: Value,
    },
}

#[derive(Debug, Clone)]
pub struct MarketUpdate {
    pub market_start_time: i64,
    pub market_type: String,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub timestamp: i64,
}

impl From<MarketUpdate> for ClientMessage {
    fn from(update: MarketUpdate) -> Self {
        Self::Market {
            market_start_time: update.market_start_time,
            market_type: update.market_type,
            bid: update.bid,
            ask: update.ask,
            timestamp: update.timestamp,
        }
    }
}

impl MarketUpdate {
    pub fn from_top(market: &UpMarket, top: TopOfBook) -> Option<Self> {
        let start = market.start_time?;
        let now = Utc::now();

        if now < start {
            return None;
        }
        if market.end_time.is_some_and(|end| now >= end) {
            return None;
        }

        Some(Self {
            market_start_time: to_unix_micros(start),
            market_type: market.timeframe.label().to_string(),
            bid: top.best_bid,
            ask: top.best_ask,
            timestamp: to_unix_micros(now),
        })
    }
}

impl ClientMessage {
    pub fn custom(payload: Value) -> Self {
        Self::Custom {
            timestamp: to_unix_micros(Utc::now()),
            payload,
        }
    }
}

pub fn parse_custom_payload(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string()))
}

fn to_unix_micros(dt: DateTime<Utc>) -> i64 {
    dt.timestamp_micros()
}

pub fn markets_changed(current: &[UpMarket], next: &[UpMarket]) -> bool {
    if current.len() != next.len() {
        return true;
    }

    let current_ids: Vec<_> = current.iter().map(|m| m.up_token_id.as_str()).collect();
    let next_ids: Vec<_> = next.iter().map(|m| m.up_token_id.as_str()).collect();
    current_ids != next_ids
}

pub fn next_rotation_delay(markets: &[UpMarket]) -> std::time::Duration {
    use chrono::Duration as ChronoDuration;
    use std::time::Duration;

    let now = Utc::now();
    let mut next = now + ChronoDuration::seconds(1);

    for market in markets {
        if let Some(end) = market.end_time {
            if end > now && end < next {
                next = end;
            }
        }
    }

    for timeframe in [Timeframe::FiveMin, Timeframe::FifteenMin, Timeframe::OneHour] {
        let start = timeframe.next_window_start(now);
        if start > now && start < next {
            next = start;
        }
    }

    next.signed_duration_since(now)
        .to_std()
        .unwrap_or(Duration::from_secs(1))
        .max(Duration::from_millis(100))
}
