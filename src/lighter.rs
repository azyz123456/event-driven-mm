#[cfg(unix)]
pub mod signer;

#[cfg(not(unix))]
#[path = "lighter/signer_unavailable.rs"]
pub mod signer;

use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use crate::intent::Side;
use crate::maker::{Fill, Input, Instrument, Level, Status, Venue};
use crate::network::maker::{decimal, number};

pub const HOST: &str = "api.testnet.lighter.xyz";
pub const PATH: &str = "/stream";
// The exchange requires at least five minutes of order lifetime.
pub const ORDER_LIFETIME_MS: u64 = 310_000;
pub const BASE: &str = "https://api.testnet.lighter.xyz";
const VENUE: Venue = Venue::Lighter;

pub fn subscribe(channel: String, auth: Option<&str>) -> String {
    let mut result = json!({"type": "subscribe", "channel": channel});
    if let Some(auth) = auth {
        result["auth"] = json!(auth);
    }
    result.to_string()
}

pub fn instrument(body: &Value, market: u16, coin: &str) -> Result<Instrument> {
    let entry = body["order_book_details"]
        .as_array()
        .context("missing Lighter perpetuals")?
        .iter()
        .find(|entry| entry["market_id"] == market)
        .context("Lighter market ID is missing from testnet metadata")?;
    ensure!(
        entry["symbol"] == coin
            && entry["market_type"] == "perp"
            && entry["status"] == "active"
            && entry["is_frozen"].as_bool() != Some(true)
            && entry["quote_multiplier"] == 1
            && entry["quote_asset_id"] == 0
            && decimal(entry, "multiplier")? == Decimal::ONE,
        "Lighter market must be an active default linear perpetual for the configured coin"
    );
    let size_decimals = number(entry, "supported_size_decimals")?.try_into()?;
    let price_decimals = number(entry, "supported_price_decimals")?.try_into()?;
    ensure!(
        size_decimals == number(entry, "size_decimals")? as u32
            && price_decimals == number(entry, "price_decimals")? as u32,
        "Lighter signing and displayed precision differ"
    );
    Ok(Instrument {
        asset: market.into(),
        size_decimals,
        price_decimals,
        min_size: decimal(entry, "min_base_amount")?,
        min_notional: decimal(entry, "min_quote_amount")?,
        // Metadata reports percentages. Use the full market rate for pricing.
        maker_fee: decimal(entry, "maker_fee")? / Decimal::ONE_HUNDRED,
        taker_fee: decimal(entry, "taker_fee")? / Decimal::ONE_HUNDRED,
    })
}

pub fn observations(
    message: &Value,
    market: u16,
    account: Option<u64>,
    connection: u64,
) -> Result<Vec<Input>> {
    let market_key = market.to_string();
    let channel = message["channel"].as_str().unwrap_or_default();
    let kind = message["type"].as_str().unwrap_or_default();
    match kind {
        "subscribed/order_book" | "update/order_book"
            if channel == format!("order_book:{market}") =>
        {
            let book = &message["order_book"];
            let levels = |key: &str| -> Result<Vec<Level>> {
                book[key]
                    .as_array()
                    .context("missing Lighter book levels")?
                    .iter()
                    .map(|level| {
                        Ok(Level {
                            price: decimal(level, "price")?,
                            size: decimal(level, "size")?,
                        })
                    })
                    .collect()
            };
            Ok(vec![Input::Book {
                venue: VENUE,
                sequence: number(book, "nonce")?,
                previous: (kind == "update/order_book")
                    .then(|| number(book, "begin_nonce"))
                    .transpose()?,
                bids: levels("bids")?,
                asks: levels("asks")?,
            }])
        }
        "update/account_all_trades" | "subscribed/account_all_trades"
            if account.is_some_and(|id| channel == format!("account_all_trades:{id}")) =>
        {
            let trades = &message["trades"];
            let market_trades = trades.as_array().or_else(|| trades[&market_key].as_array());
            market_trades
                .into_iter()
                .flatten()
                .filter(|trade| trade["market_id"] == market)
                .map(|trade| fill(trade, account.expect("matched account")))
                .collect()
        }
        "update/account_orders" | "subscribed/account_orders"
            if account.is_some_and(|id| message["account"] == id) =>
        {
            order_updates(&message["orders"][&market_key], market)
        }
        "update/trade" | "subscribed/trade" if channel == format!("trade:{market}") => {
            message["trades"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|trade| {
                    Ok(Input::PublicTrade {
                        venue: VENUE,
                        id: trade_id(trade)?,
                        connection,
                        venue_ms: number(trade, "timestamp")?,
                    })
                })
                .collect()
        }
        _ => Ok(vec![]),
    }
}

pub fn order_updates(orders: &Value, market: u16) -> Result<Vec<Input>> {
    orders
        .as_array()
        .into_iter()
        .flatten()
        .filter(|order| order["market_index"] == market)
        .filter_map(|order| match order["status"].as_str()? {
            "open" => Some((order, Status::Open)),
            "filled" | "expired" => Some((order, Status::Terminal)),
            status if status.starts_with("canceled") => Some((order, Status::Terminal)),
            _ => None,
        })
        .map(|(order, status)| {
            Ok(Input::Order {
                venue: VENUE,
                client: Some(number(order, "client_order_index")?),
                oid: Some(number(order, "order_index")?),
                status,
                filled: decimal(order, "filled_base_amount")?,
            })
        })
        .collect()
}

fn trade_id(value: &Value) -> Result<String> {
    value["trade_id_str"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| value["trade_id"].as_u64().map(|id| id.to_string()))
        .context("missing Lighter trade ID")
}

pub fn fill(trade: &Value, account: u64) -> Result<Input> {
    let is_buy = trade["bid_account_id"] == account;
    let is_sell = trade["ask_account_id"] == account;
    ensure!(is_buy != is_sell, "unexpected Lighter fill account");
    let maker = trade["is_maker_ask"]
        .as_bool()
        .context("missing Lighter maker side")?
        == is_sell;
    let leg = if is_sell { "ask" } else { "bid" };
    let price = decimal(trade, "price")?;
    let size = decimal(trade, "size")?;
    let role = if maker { "maker" } else { "taker" };
    let millionths = |key: &str| -> Result<Decimal> {
        if trade[key].is_null() {
            return Ok(Decimal::ZERO);
        }
        Ok(Decimal::from(
            trade[key].as_i64().context("invalid Lighter trade fee")?,
        ))
    };
    let fee_rate = (millionths(&format!("{role}_fee"))?
        + millionths(&format!("integrator_{role}_fee"))?)
        / Decimal::from(1_000_000);
    Ok(Input::Fill {
        venue: VENUE,
        fill: Fill {
            id: trade_id(trade)?,
            client: Some(number(trade, &format!("{leg}_client_id"))?),
            oid: number(trade, &format!("{leg}_id"))?,
            side: if is_buy { Side::Buy } else { Side::Sell },
            maker,
            price,
            size,
            fee: size * price * fee_rate,
            venue_ms: number(trade, "timestamp")?,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_lighter_account_trade_before_an_order_ack() -> Result<()> {
        let trade = json!({
            "trade_id_str":"9987", "market_id":4096, "ask_account_id":42, "bid_account_id":22,
            "ask_client_id":711, "ask_id":281474976710801u64, "is_maker_ask":true,
            "size":"0.10", "price":"250", "maker_fee":28, "timestamp":1700000000000u64
        });
        let decoded = observations(
            &json!({
                "type":"update/account_all_trades", "channel":"account_all_trades:42",
                "trades":{"4096":[trade]}
            }),
            4096,
            Some(42),
            1,
        )?;
        assert!(
            matches!(&decoded[..], [Input::Fill { venue: Venue::Lighter, fill }]
            if fill.id == "9987" && fill.client == Some(711) && fill.side == Side::Sell
                && fill.maker && fill.fee == Decimal::new(7, 4))
        );
        Ok(())
    }
}
