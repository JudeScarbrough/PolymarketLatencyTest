use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Timelike, Utc};
use chrono_tz::America::New_York;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};

const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
pub const SUBSCRIBE_LEAD_SECS: i64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timeframe {
    FiveMin,
    FifteenMin,
    #[allow(dead_code)]
    OneHour,
}

impl Timeframe {
    pub fn label(self) -> &'static str {
        match self {
            Self::FiveMin => "5m",
            Self::FifteenMin => "15m",
            Self::OneHour => "1h",
        }
    }

    pub fn next_window_start(self, now: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            Self::FiveMin => next_aligned(now, 300),
            Self::FifteenMin => next_aligned(now, 900),
            Self::OneHour => {
                let et = now.with_timezone(&New_York);
                let next_hour = et
                    .with_minute(0)
                    .and_then(|dt| dt.with_second(0))
                    .and_then(|dt| dt.with_nanosecond(0))
                    .expect("valid hour boundary")
                    + chrono::Duration::hours(1);
                next_hour.with_timezone(&Utc)
            }
        }
    }

    fn series_slug(self) -> &'static str {
        match self {
            Self::FiveMin => "btc-up-or-down-5m",
            Self::FifteenMin => "btc-up-or-down-15m",
            Self::OneHour => "btc-up-or-down-hourly",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpMarket {
    pub timeframe: Timeframe,
    pub slug: String,
    pub title: String,
    pub up_token_id: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    closed: Option<bool>,
    markets: Vec<GammaMarket>,
}

#[derive(Debug, Deserialize)]
struct GammaMarket {
    slug: String,
    question: String,
    closed: Option<bool>,
    #[serde(rename = "acceptingOrders", default)]
    accepting_orders: Option<bool>,
    outcomes: String,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: String,
    #[serde(rename = "eventStartTime", default)]
    event_start_time: Option<String>,
    #[serde(rename = "endDate", default)]
    end_date: Option<String>,
}

pub async fn fetch_subscribable_up_markets(client: &reqwest::Client) -> Result<Vec<UpMarket>> {
    let mut markets = Vec::new();

    for timeframe in [Timeframe::FiveMin, Timeframe::FifteenMin] {
        if let Some(market) = fetch_subscribable_for_timeframe(client, timeframe).await? {
            markets.push(market);
        }
    }

    Ok(markets)
}

async fn fetch_subscribable_for_timeframe(
    client: &reqwest::Client,
    timeframe: Timeframe,
) -> Result<Option<UpMarket>> {
    let now = Utc::now();

    if let Some(market) = fetch_from_series(client, timeframe, now).await? {
        return Ok(Some(market));
    }

    for slug in slug_candidates(timeframe) {
        if let Some(market) = fetch_by_slug(client, timeframe, &slug, now).await? {
            return Ok(Some(market));
        }
    }

    Ok(None)
}

async fn fetch_from_series(
    client: &reqwest::Client,
    timeframe: Timeframe,
    now: DateTime<Utc>,
) -> Result<Option<UpMarket>> {
    let url = format!(
        "{GAMMA_BASE}/events?series_slug={}&active=true&closed=false&limit=100",
        timeframe.series_slug()
    );
    let events: Vec<GammaEvent> = client
        .get(&url)
        .send()
        .await
        .context("gamma events request failed")?
        .error_for_status()
        .context("gamma events returned error status")?
        .json()
        .await
        .context("gamma events response was not valid json")?;

    Ok(events
        .into_iter()
        .flat_map(|event| parse_event(event, timeframe))
        .filter(|market| is_subscribable_market(market, now))
        .min_by_key(|market| market.start_time))
}

async fn fetch_by_slug(
    client: &reqwest::Client,
    timeframe: Timeframe,
    slug: &str,
    now: DateTime<Utc>,
) -> Result<Option<UpMarket>> {
    let url = format!("{GAMMA_BASE}/events?slug={slug}");
    let events: Vec<GammaEvent> = client
        .get(&url)
        .send()
        .await
        .context("gamma slug request failed")?
        .error_for_status()
        .context("gamma slug returned error status")?
        .json()
        .await
        .unwrap_or_default();

    Ok(events
        .into_iter()
        .flat_map(|event| parse_event(event, timeframe))
        .find(|market| is_subscribable_market(market, now)))
}

fn is_subscribable_market(market: &UpMarket, now: DateTime<Utc>) -> bool {
    match (market.start_time, market.end_time) {
        (Some(start), Some(end)) => {
            let subscribe_from =
                start - chrono::Duration::seconds(SUBSCRIBE_LEAD_SECS);
            now >= subscribe_from && now < end
        }
        (Some(start), None) => {
            let subscribe_from =
                start - chrono::Duration::seconds(SUBSCRIBE_LEAD_SECS);
            now >= subscribe_from
        }
        _ => true,
    }
}

fn parse_event(event: GammaEvent, timeframe: Timeframe) -> Option<UpMarket> {
    if event.closed.unwrap_or(false) {
        return None;
    }

    let market = event
        .markets
        .into_iter()
        .find(|m| !m.closed.unwrap_or(false) && m.accepting_orders.unwrap_or(true))?;

    let outcomes: Vec<String> = serde_json::from_str(&market.outcomes).ok()?;
    let token_ids: Vec<String> = serde_json::from_str(&market.clob_token_ids).ok()?;

    let up_index = outcomes.iter().position(|o| o.eq_ignore_ascii_case("up"))?;
    let up_token_id = token_ids.get(up_index)?.clone();

    Some(UpMarket {
        timeframe,
        slug: market.slug,
        title: market.question,
        up_token_id,
        start_time: parse_time(market.event_start_time.as_deref()),
        end_time: parse_time(market.end_date.as_deref()),
    })
}

fn parse_time(raw: Option<&str>) -> Option<DateTime<Utc>> {
    raw.and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn slug_candidates(timeframe: Timeframe) -> Vec<String> {
    match timeframe {
        Timeframe::FiveMin => vec![
            timestamp_slug("btc-updown-5m", 300, 0),
            timestamp_slug("btc-updown-5m", 300, 1),
        ],
        Timeframe::FifteenMin => vec![
            timestamp_slug("btc-updown-15m", 900, 0),
            timestamp_slug("btc-updown-15m", 900, 1),
        ],
        Timeframe::OneHour => hourly_slug_candidates(),
    }
}

fn timestamp_slug(prefix: &str, window_secs: i64, windows_ahead: i64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_secs() as i64;
    let start = ((now / window_secs) + windows_ahead) * window_secs;
    format!("{prefix}-{start}")
}

fn hourly_slug_candidates() -> Vec<String> {
    let now = Utc::now().with_timezone(&New_York);
    let next = now + chrono::Duration::hours(1);
    vec![
        format_hourly_slug(&now, true),
        format_hourly_slug(&now, false),
        format_hourly_slug(&next, true),
        format_hourly_slug(&next, false),
    ]
}

fn format_hourly_slug(dt: &chrono::DateTime<chrono_tz::Tz>, with_year: bool) -> String {
    let month = dt.format("%B").to_string().to_lowercase();
    let day = dt.day();
    let hour = dt.hour();

    let (hour_12, am_pm) = match hour {
        0 => (12, "am"),
        1..=11 => (hour, "am"),
        12 => (12, "pm"),
        _ => (hour - 12, "pm"),
    };

    if with_year {
        format!(
            "bitcoin-up-or-down-{month}-{day}-{}-{hour_12}{am_pm}-et",
            dt.year()
        )
    } else {
        format!("bitcoin-up-or-down-{month}-{day}-{hour_12}{am_pm}-et")
    }
}

fn next_aligned(now: DateTime<Utc>, window_secs: i64) -> DateTime<Utc> {
    let ts = now.timestamp();
    let next = ((ts / window_secs) + 1) * window_secs;
    DateTime::from_timestamp(next, 0).expect("valid unix timestamp")
}
