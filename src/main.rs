mod custom_feed;
mod gamma;
mod orderbook;
mod polymarket;
mod reconnect;
mod server;
mod update;

use anyhow::{Context, Result};
use gamma::UpMarket;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio::time::sleep;
use update::{markets_changed, next_rotation_delay};

const BROADCAST_CAPACITY: usize = 16_384;

#[tokio::main]
async fn main() -> Result<()> {
    let client = reqwest::Client::builder()
        .user_agent("latency/0.1")
        .build()
        .context("failed to build http client")?;

    match custom_feed::load_custom_feed_addr() {
        Ok(addr) => eprintln!("custom feed address: {addr}"),
        Err(err) => eprintln!("custom feed address not loaded yet: {err:#}"),
    }

    let (market_tx, market_rx) = watch::channel(Vec::<UpMarket>::new());
    let (broadcast_tx, _) = broadcast::channel(BROADCAST_CAPACITY);

    tokio::spawn(server::run_server(broadcast_tx.clone()));
    tokio::spawn(polymarket::run_polymarket_feed(
        market_rx,
        broadcast_tx.clone(),
    ));
    tokio::spawn(custom_feed::run_custom_feed(broadcast_tx));

    run_market_rotation(client, market_tx).await
}

async fn run_market_rotation(
    client: reqwest::Client,
    market_tx: watch::Sender<Vec<UpMarket>>,
) -> Result<()> {
    let mut current = Vec::new();

    loop {
        match gamma::fetch_subscribable_up_markets(&client).await {
            Ok(markets) => {
                if markets_changed(&current, &markets) {
                    current = markets;
                    eprintln!(
                        "markets rotated: {} subscribable up market(s)",
                        current.len()
                    );
                    for market in &current {
                        eprintln!(
                            "  - [{}] {} ({})",
                            market.timeframe.label(),
                            market.title,
                            market.slug
                        );
                    }
                    let _ = market_tx.send(current.clone());
                }
            }
            Err(err) => {
                eprintln!("market refresh failed: {err:#}");
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        }

        let delay = if current.is_empty() {
            Duration::from_secs(5)
        } else {
            next_rotation_delay(&current)
        };
        sleep(delay).await;
    }
}
