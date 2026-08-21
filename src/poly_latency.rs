use crate::reconnect::Backoff;
use crate::update::ClientMessage;
use anyhow::{Context, Result};
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE},
    Engine,
};
use chrono::Utc;
use hmac::{Hmac, Mac};
use rand::RngCore;
use reqwest::{Client, Method};
use sha2::Sha256;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::time::sleep;

const CLOB_CANCEL_URL: &str = "https://clob.polymarket.com/order";
const CLOB_CANCEL_PATH: &str = "/order";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

type HmacSha256 = Hmac<Sha256>;

pub async fn run_poly_cancel_probe(broadcast_tx: broadcast::Sender<ClientMessage>) -> Result<()> {
    let mut backoff = Backoff::new();

    loop {
        let client = match Client::builder()
            .user_agent("latency/0.1")
            .timeout(REQUEST_TIMEOUT)
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                eprintln!("polymarket cancel probe: failed to build http client: {err:#}");
                backoff.wait().await;
                continue;
            }
        };

        backoff.reset();
        eprintln!("polymarket cancel probe: sending fake cancel every wall-clock second");

        loop {
            sleep(duration_until_next_second()).await;
            let http = client.clone();
            let tx = broadcast_tx.clone();
            tokio::spawn(async move {
                probe_once(&http, &tx).await;
            });
        }
    }
}

async fn probe_once(client: &Client, broadcast_tx: &broadcast::Sender<ClientMessage>) {
    let sent_at_ms = Utc::now().timestamp_millis();
    let started = Instant::now();
    let result = send_fake_cancel(client).await;
    let latency_ms = started.elapsed().as_millis().min(i64::MAX as u128) as i64;

    match result {
        Ok(_) => {}
        Err(err) => {
            eprintln!("polymarket cancel probe: {err:#} in {latency_ms}ms");
        }
    }

    let _ = broadcast_tx.send(ClientMessage::latency(
        sent_at_ms,
        latency_ms,
        "polymarket",
    ));
}

async fn send_fake_cancel(client: &Client) -> Result<reqwest::StatusCode> {
    let address = format!("0x{}", random_hex(20));
    let api_key = random_uuid();
    let passphrase = random_hex(16);
    let secret = STANDARD.encode(random_bytes(32));
    let order_id = format!("0x{}", random_hex(32));
    let timestamp = Utc::now().timestamp().to_string();
    let body = format!(r#"{{"orderID":"{order_id}"}}"#);
    let signature = l2_signature(&secret, &timestamp, "DELETE", CLOB_CANCEL_PATH, &body)?;

    let response = client
        .request(Method::DELETE, CLOB_CANCEL_URL)
        .header("Content-Type", "application/json")
        .header("POLY_ADDRESS", address)
        .header("POLY_API_KEY", api_key)
        .header("POLY_PASSPHRASE", passphrase)
        .header("POLY_SIGNATURE", signature)
        .header("POLY_TIMESTAMP", timestamp)
        .body(body)
        .send()
        .await
        .context("cancel request failed")?;

    Ok(response.status())
}

fn l2_signature(
    secret_b64: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    body: &str,
) -> Result<String> {
    let secret = STANDARD
        .decode(secret_b64)
        .context("fake api secret was not valid base64")?;
    let mut mac =
        HmacSha256::new_from_slice(&secret).context("invalid hmac key")?;
    mac.update(timestamp.as_bytes());
    mac.update(method.as_bytes());
    mac.update(path.as_bytes());
    mac.update(body.as_bytes());
    Ok(URL_SAFE.encode(mac.finalize().into_bytes()))
}

fn duration_until_next_second() -> Duration {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let nanos = now.subsec_nanos();
    if nanos == 0 {
        Duration::from_secs(1)
    } else {
        Duration::from_nanos(1_000_000_000 - u64::from(nanos))
    }
}

fn random_bytes(len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    rand::rng().fill_bytes(&mut bytes);
    bytes
}

fn random_hex(len: usize) -> String {
    random_bytes(len)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn random_uuid() -> String {
    let bytes = random_bytes(16);
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

#[cfg(test)]
mod tests {
    use super::l2_signature;

    #[test]
    fn signs_cancel_body() {
        let secret = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"0123456789abcdef0123456789abcdef",
        );
        let signature = l2_signature(
            &secret,
            "1710000000",
            "DELETE",
            "/order",
            r#"{"orderID":"0xabc"}"#,
        )
        .unwrap();
        assert!(!signature.is_empty());
    }
}
