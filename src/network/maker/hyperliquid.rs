use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use crate::hyperliquid::Snapshot;
use crate::intent::{self, Side, TimeInForce};
use crate::maker::{Action, Fill, Input, Instrument, Intent, Kind, Level, Status, Venue};
use crate::signing::Signer;

use super::{decimal, number};

const VENUE: Venue = Venue::Hyperliquid;

pub fn cloid(seed: u64, client: u64) -> String {
    format!("0x{seed:016x}{client:016x}")
}

pub fn client(value: &Value, seed: u64) -> Option<u64> {
    let hex = value.as_str()?.strip_prefix("0x")?;
    if hex.len() != 32 || !hex.is_ascii() || u64::from_str_radix(&hex[..16], 16).ok()? != seed {
        return None;
    }
    u64::from_str_radix(&hex[16..], 16).ok()
}

pub fn instrument(meta: &Value, fees: Option<&Value>, coin: &str) -> Result<Instrument> {
    let (asset, entry) = meta["universe"]
        .as_array()
        .context("missing Hyperliquid universe")?
        .iter()
        .enumerate()
        .find(|(_, entry)| entry["name"] == coin)
        .context("coin absent from Hyperliquid testnet perpetuals")?;
    ensure!(
        entry["isDelisted"] != true,
        "Hyperliquid perpetual is delisted"
    );
    let size_decimals: u32 = number(entry, "szDecimals")?.try_into()?;
    ensure!(size_decimals <= 6, "invalid Hyperliquid size precision");
    Ok(Instrument {
        asset: asset.try_into()?,
        size_decimals,
        price_decimals: 6 - size_decimals,
        min_size: Decimal::new(1, size_decimals),
        min_notional: Decimal::TEN,
        maker_fee: fees
            .map(|fees| decimal(fees, "userAddRate"))
            .transpose()?
            .unwrap_or_default(),
        taker_fee: fees
            .map(|fees| decimal(fees, "userCrossRate"))
            .transpose()?
            .unwrap_or_default(),
    })
}

pub fn subscribe(kind: &str, key: &str, value: &str) -> String {
    let mut subscription = json!({"type": kind});
    subscription[key] = json!(value);
    json!({"method": "subscribe", "subscription": subscription}).to_string()
}

pub fn observations(
    message: &Value,
    coin: &str,
    account: Option<&str>,
    seed: u64,
    connection: u64,
) -> Result<Vec<Input>> {
    let data = &message["data"];
    match message["channel"].as_str() {
        Some("l2Book") if data["coin"] == coin => {
            let Snapshot {
                levels: [bids, asks],
                ..
            } = serde_json::from_value(data.clone())?;
            let normalize = |levels: Vec<crate::hyperliquid::Level>| {
                levels
                    .into_iter()
                    .map(|level| Level {
                        price: level.px,
                        size: level.sz,
                    })
                    .collect()
            };
            Ok(vec![Input::Book {
                venue: VENUE,
                sequence: number(data, "time")?,
                previous: None,
                bids: normalize(bids),
                asks: normalize(asks),
            }])
        }
        Some("orderUpdates") if account.is_some() => data
            .as_array()
            .context("missing Hyperliquid order updates")?
            .iter()
            .filter(|entry| entry["order"]["coin"] == coin)
            .map(|entry| order(entry, seed))
            .collect::<Result<Vec<_>>>()
            .map(|orders| orders.into_iter().flatten().collect()),
        Some("userFills")
            if account
                .zip(data["user"].as_str())
                .is_some_and(|(a, b)| a.eq_ignore_ascii_case(b)) =>
        {
            data["fills"]
                .as_array()
                .context("missing Hyperliquid fills")?
                .iter()
                .filter(|trade| trade["coin"] == coin)
                .map(|trade| fill(trade, seed))
                .collect()
        }
        Some("trades") => data
            .as_array()
            .context("missing Hyperliquid public trades")?
            .iter()
            .filter(|trade| trade["coin"] == coin)
            .map(|trade| {
                Ok(Input::PublicTrade {
                    venue: VENUE,
                    id: number(trade, "tid")?.to_string(),
                    connection,
                    venue_ms: number(trade, "time")?,
                })
            })
            .collect(),
        _ => Ok(vec![]),
    }
}

pub fn fill(trade: &Value, seed: u64) -> Result<Input> {
    ensure!(
        trade["feeToken"] == "USDC",
        "unexpected Hyperliquid fee currency"
    );
    let side = match trade["side"].as_str() {
        Some("B") => Side::Buy,
        Some("A") => Side::Sell,
        _ => anyhow::bail!("unknown Hyperliquid fill side"),
    };
    Ok(Input::Fill {
        venue: VENUE,
        fill: Fill {
            id: number(trade, "tid")?.to_string(),
            client: client(&trade["cloid"], seed),
            oid: number(trade, "oid")?,
            side,
            maker: !trade["crossed"]
                .as_bool()
                .context("missing Hyperliquid liquidity role")?,
            price: decimal(trade, "px")?,
            size: decimal(trade, "sz")?,
            // Hyperliquid reports the total fee, including any builder fee.
            fee: decimal(trade, "fee")?,
            venue_ms: number(trade, "time")?,
        },
    })
}

pub fn order(wrapper: &Value, seed: u64) -> Result<Option<Input>> {
    let order = &wrapper["order"];
    let status = match wrapper["status"].as_str() {
        Some("open") => Status::Open,
        Some(value)
            if matches!(
                value,
                "filled" | "canceled" | "rejected" | "scheduledCancel"
            ) || value.ends_with("Canceled")
                || value.ends_with("Rejected") =>
        {
            Status::Terminal
        }
        _ => return Ok(None),
    };
    let filled = decimal(order, "origSz")? - decimal(order, "sz")?;
    Ok(Some(Input::Order {
        venue: VENUE,
        client: client(&order["cloid"], seed),
        oid: Some(number(order, "oid")?),
        status,
        filled,
    }))
}

pub fn order_reply(message: &Value, intent: &Intent) -> Result<Option<Input>> {
    let response = &message["data"]["response"];
    if response["type"] != "action" || response["payload"]["status"] != "ok" {
        return Ok(None);
    }
    let Some([result]) = response["payload"]["response"]["data"]["statuses"]
        .as_array()
        .map(Vec::as_slice)
    else {
        return Ok(None);
    };
    let positive = |oid: Option<u64>, status, filled| {
        Some(Input::Order {
            venue: VENUE,
            client: Some(intent.client),
            oid,
            status,
            filled,
        })
    };
    Ok(match intent.action {
        Action::Place { .. } if result["error"].is_string() => {
            positive(None, Status::Terminal, Decimal::ZERO)
        }
        Action::Place { .. } if result["resting"].is_object() => positive(
            Some(number(&result["resting"], "oid")?),
            Status::Open,
            Decimal::ZERO,
        ),
        Action::Place { .. } if result["filled"].is_object() => positive(
            Some(number(&result["filled"], "oid")?),
            Status::Terminal,
            decimal(&result["filled"], "totalSz")?,
        ),
        // Cancel success confirms the request. The order update carries the total filled.
        _ => None,
    })
}

pub fn sign(signer: &mut Signer, intent: &Intent, asset: u32, seed: u64) -> Result<(String, u64)> {
    let action = match intent.action {
        Action::Place {
            side,
            kind,
            price,
            size,
        } => intent::Action::Place {
            side,
            price,
            size,
            book_at_ms: 0,
            tif: match kind {
                Kind::PostOnly => TimeInForce::Alo,
                Kind::Ioc => TimeInForce::Ioc,
            },
        },
        Action::Cancel { oid } => intent::Action::Cancel {
            oid: oid.unwrap_or_default(),
        },
    };
    let signed = signer.sign(
        &intent::Intent {
            id: intent.id,
            caused_by_seq: intent.caused_by_seq,
            asset,
            cloid: cloid(seed, intent.client),
            action,
        },
        5_000,
    )?;
    Ok((
        crate::hyperliquid::trading::signed_post(intent.id, &signed.wire_payload),
        signed.nonce,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinguishes_partial_ioc_receipt_from_user_fill() -> Result<()> {
        let intent = Intent {
            id: 8,
            caused_by_seq: 3,
            ordinal: 0,
            venue: VENUE,
            client: 12,
            action: Action::Place {
                side: Side::Sell,
                kind: Kind::Ioc,
                price: Decimal::new(230, 0),
                size: Decimal::new(30, 2),
            },
        };
        let receipt = json!({"data":{"response":{"type":"action","payload":{
            "status":"ok","response":{"type":"order","data":{
                "statuses":[{"filled":{"totalSz":"0.20","avgPx":"239","oid":567}}]
            }}
        }}}});
        let order = order_reply(&receipt, &intent)?.unwrap();
        assert!(
            matches!(order, Input::Order { status: Status::Terminal, filled, .. }
            if filled == Decimal::new(20, 2))
        );
        let fill = fill(
            &json!({"tid":77,"oid":567,"side":"A","crossed":true,
            "px":"239","sz":"0.20","fee":"0.02","builderFee":"0.01","feeToken":"USDC","time":123}),
            1,
        )?;
        assert!(matches!(fill, Input::Fill { fill, .. }
            if fill.id == "77" && fill.client.is_none() && !fill.maker && fill.fee == Decimal::new(2, 2)));
        Ok(())
    }
}
