use ordered_float::NotNan;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct OrderBook {
    bids: BTreeMap<NotNan<f64>, f64>,
    asks: BTreeMap<NotNan<f64>, f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopOfBook {
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
}

impl OrderBook {
    pub fn apply_snapshot(&mut self, bids: &[(String, String)], asks: &[(String, String)]) {
        self.apply_side(true, bids);
        self.apply_side(false, asks);
    }

    pub fn apply_delta(&mut self, side: &str, price: &str, size: &str) {
        let Some(price) = parse_price(price) else {
            return;
        };
        let size = size.parse::<f64>().unwrap_or(0.0);

        let book = if side.eq_ignore_ascii_case("BUY") {
            &mut self.bids
        } else {
            &mut self.asks
        };

        if size <= 0.0 {
            book.remove(&price);
        } else {
            book.insert(price, size);
        }
    }

    pub fn prune_to_authoritative(&mut self, best_bid: Option<&str>, best_ask: Option<&str>) {
        if let Some(raw) = best_bid {
            match parse_top_price(raw) {
                Some(bb) => {
                    self.bids.retain(|price, _| price.into_inner() <= bb);
                }
                None => self.bids.clear(),
            }
        }

        if let Some(raw) = best_ask {
            match parse_top_price(raw) {
                Some(ba) => {
                    self.asks.retain(|price, _| price.into_inner() >= ba);
                }
                None => self.asks.clear(),
            }
        }
    }

    pub fn top_of_book(&self) -> TopOfBook {
        TopOfBook {
            best_bid: self.bids.keys().next_back().map(|p| p.into_inner()),
            best_ask: self.asks.keys().next().map(|p| p.into_inner()),
        }
    }

    fn apply_side(&mut self, is_bid: bool, levels: &[(String, String)]) {
        let book = if is_bid {
            &mut self.bids
        } else {
            &mut self.asks
        };

        for (price, size) in levels {
            let Some(price) = parse_price(price) else {
                continue;
            };
            let size = size.parse::<f64>().unwrap_or(0.0);
            if size <= 0.0 {
                book.remove(&price);
            } else {
                book.insert(price, size);
            }
        }
    }
}

fn parse_price(raw: &str) -> Option<NotNan<f64>> {
    NotNan::new(raw.parse().ok()?).ok()
}

fn parse_top_price(raw: &str) -> Option<f64> {
    let price = raw.parse::<f64>().ok()?;
    if price <= 0.0 {
        None
    } else {
        Some(price)
    }
}
