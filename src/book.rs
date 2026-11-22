use anyhow::{Context, Result, bail, ensure};
use rust_decimal::Decimal;

use crate::event::{Event, Message, Record};
use crate::hyperliquid::{self, Incoming, Snapshot};

pub struct Book {
    coin: String,
    connection_id: Option<u64>,
    snapshot: Option<Snapshot>,
}

pub struct Midpoint {
    pub received_at_unix_ms: u64,
    pub price: Decimal,
}

impl Book {
    pub fn new(coin: String) -> Self {
        Self {
            coin,
            connection_id: None,
            snapshot: None,
        }
    }

    pub fn update(&mut self, record: Record) -> Result<Option<Midpoint>> {
        let connection_id = record.connection_id.context("missing connection ID")?;
        match record.event {
            Event::TcpOpen { .. } => {
                self.connection_id = Some(connection_id);
                self.snapshot = None;
            }
            Event::TcpClose { .. } if Some(connection_id) == self.connection_id => {
                self.connection_id = None;
                self.snapshot = None;
            }
            Event::WsIn {
                message: Message::Text(text),
            } if Some(connection_id) == self.connection_id => match hyperliquid::decode(&text)? {
                Incoming::Book(snapshot) if snapshot.coin == self.coin => {
                    let snapshot = self.snapshot.insert(snapshot);
                    return Ok(midpoint(snapshot)?.map(|price| Midpoint {
                        received_at_unix_ms: record.local_time_unix_ms,
                        price,
                    }));
                }
                Incoming::SubscriptionResponse(response) => {
                    ensure!(
                        response.confirms_book(&self.coin),
                        "unexpected Hyperliquid subscription acknowledgement for {}",
                        self.coin
                    );
                }
                Incoming::Error(data) => {
                    let detail = data.map_or("no details", |value| value.get());
                    bail!("Hyperliquid returned an error: {detail}");
                }
                _ => {}
            },
            _ => {}
        }
        Ok(None)
    }
}

pub fn best_prices(snapshot: &Snapshot) -> Option<(Decimal, Decimal)> {
    let [bids, asks] = &snapshot.levels;
    let level_price = |level: &hyperliquid::Level| {
        (level.sz > Decimal::ZERO && level.px > Decimal::ZERO).then_some(level.px)
    };
    let best_bid = bids.iter().filter_map(level_price).max();
    let best_ask = asks.iter().filter_map(level_price).min();
    best_bid.zip(best_ask).filter(|(bid, ask)| bid < ask)
}

fn midpoint(snapshot: &Snapshot) -> Result<Option<Decimal>> {
    let Some((bid, ask)) = best_prices(snapshot) else {
        return Ok(None);
    };
    let price = bid
        .checked_add(ask)
        .and_then(|sum| sum.checked_div(Decimal::TWO))
        .context("midpoint exceeds decimal range")?;
    Ok(Some(price.normalize()))
}
