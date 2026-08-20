use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use std::sync::{Arc, RwLock};

use crate::gamma::{Timeframe, UpMarket, SUBSCRIBE_LEAD_SECS};
use crate::orderbook::TopOfBook;

pub type SharedQuotes = Arc<RwLock<LatestQuotes>>;

#[derive(Debug, Clone, Copy, Default)]
pub struct BookQuote {
    pub bid: Option<f64>,
    pub ask: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LatestQuotes {
    pub five_min: BookQuote,
    pub fifteen_min: BookQuote,
    pub five_min_start: Option<i64>,
    pub fifteen_min_start: Option<i64>,
}

impl LatestQuotes {
    pub fn set(&mut self, market: &UpMarket, top: TopOfBook) {
        let quote = BookQuote {
            bid: top.best_bid,
            ask: top.best_ask,
        };
        let start = market.start_time.map(to_unix_micros);
        match market.timeframe {
            Timeframe::FiveMin => {
                self.five_min = quote;
                self.five_min_start = start;
            }
            Timeframe::FifteenMin => {
                self.fifteen_min = quote;
                self.fifteen_min_start = start;
            }
            Timeframe::OneHour => {}
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientMessage {
    pub timestamp: i64,
    pub payload: Value,
    pub bid_5m: Option<f64>,
    pub ask_5m: Option<f64>,
    pub bid_15m: Option<f64>,
    pub ask_15m: Option<f64>,
    #[serde(rename = "5m_starttime")]
    pub five_min_starttime: Option<i64>,
    #[serde(rename = "15m_starttime")]
    pub fifteen_min_starttime: Option<i64>,
}

impl ClientMessage {
    pub fn from_quotes(quotes: LatestQuotes, payload: Value) -> Self {
        Self {
            timestamp: to_unix_micros(Utc::now()),
            payload,
            bid_5m: quotes.five_min.bid,
            ask_5m: quotes.five_min.ask,
            bid_15m: quotes.fifteen_min.bid,
            ask_15m: quotes.fifteen_min.ask,
            five_min_starttime: quotes.five_min_start,
            fifteen_min_starttime: quotes.fifteen_min_start,
        }
    }

    pub fn custom(payload: Value, quotes: LatestQuotes) -> Self {
        Self::from_quotes(quotes, payload)
    }

    pub fn quotes_only(quotes: LatestQuotes) -> Self {
        Self::from_quotes(quotes, Value::Object(serde_json::Map::new()))
    }
}

pub fn market_is_live(market: &UpMarket) -> bool {
    let Some(start) = market.start_time else {
        return false;
    };
    let now = Utc::now();
    if now < start {
        return false;
    }
    if market.end_time.is_some_and(|end| now >= end) {
        return false;
    }
    true
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
    let mut next = now + ChronoDuration::hours(24);

    for market in markets {
        if let Some(end) = market.end_time {
            if end > now && end < next {
                next = end;
            }
        }
        if let Some(start) = market.start_time {
            let subscribe_at = start - ChronoDuration::seconds(SUBSCRIBE_LEAD_SECS);
            if subscribe_at > now && subscribe_at < next {
                next = subscribe_at;
            }
        }
    }

    for timeframe in [Timeframe::FiveMin, Timeframe::FifteenMin] {
        let window_start = timeframe.next_window_start(now);
        let subscribe_at = window_start - ChronoDuration::seconds(SUBSCRIBE_LEAD_SECS);
        if subscribe_at > now && subscribe_at < next {
            next = subscribe_at;
        }
        if window_start > now && window_start < next {
            next = window_start;
        }
    }

    next.signed_duration_since(now)
        .to_std()
        .unwrap_or(Duration::from_secs(1))
        .max(Duration::from_millis(100))
}
