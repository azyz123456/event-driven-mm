use anyhow::{Context, Result, ensure};
use reqwest::blocking::Client;
use rust_decimal::Decimal;
use serde_json::{Value, json};

use crate::lighter;
use crate::maker::{Config, Input, Venue};

use super::hyperliquid as hyper;
use super::{decimal, number};

#[derive(Clone)]
pub struct Account {
    pub hyperliquid: Option<String>,
    pub lighter: Option<u64>,
    pub key_index: Option<u8>,
}

pub struct Request {
    pub generation: u64,
    pub requested_ms: u64,
    pub cutoff_ms: u64,
    pub start_ms: u64,
    pub first: bool,
    pub auth: Option<String>,
    pub targets: Vec<Target>,
}

pub struct Target {
    pub client: u64,
    pub oid: Option<u64>,
    pub abandonable: bool,
    pub reported: Decimal,
    pub check: bool,
}

pub struct Snapshot {
    pub nonce: Option<u64>,
    pub inputs: Vec<Input>,
}

pub fn fetch(
    http: &Client,
    config: &Config,
    accounts: &Account,
    venue: Venue,
    seed: u64,
    agent: Option<&str>,
    request: &Request,
) -> Result<Snapshot> {
    match venue {
        Venue::Hyperliquid => hyper_snapshot(http, config, accounts, seed, agent, request),
        Venue::Lighter => lighter_snapshot(http, config, accounts, request),
    }
}

fn hyper_snapshot(
    http: &Client,
    config: &Config,
    accounts: &Account,
    seed: u64,
    agent: Option<&str>,
    request: &Request,
) -> Result<Snapshot> {
    let account = accounts.hyperliquid.as_deref();
    let info = |query: Value| -> Result<Value> {
        let url = format!("https://{}/info", crate::hyperliquid::TESTNET_HOST);
        Ok(http
            .post(url)
            .json(&query)
            .send()?
            .error_for_status()?
            .json()?)
    };

    let mut inputs = Vec::new();
    if request.first {
        if let Some(agent) = agent {
            let role = info(json!({"type":"userRole","user":agent}))?;
            ensure!(
                role["role"] == "agent"
                    && account
                        .zip(role["data"]["user"].as_str())
                        .is_some_and(|(a, b)| a.eq_ignore_ascii_case(b)),
                "Hyperliquid agent is not approved for this account"
            );
        }
        let meta = info(json!({"type":"meta"}))?;
        let fees = account
            .map(|account| info(json!({"type":"userFees","user":account})))
            .transpose()?;
        inputs.push(Input::Instrument {
            venue: Venue::Hyperliquid,
            instrument: hyper::instrument(&meta, fees.as_ref(), &config.coin)?,
        });
    }
    let Some(account) = account else {
        return Ok(Snapshot {
            nonce: None,
            inputs,
        });
    };

    let fills = info(json!({"type":"userFillsByTime","user":account,
        "startTime":request.cutoff_ms, "aggregateByTime":false}))?;
    let fills = fills
        .as_array()
        .context("missing Hyperliquid fill history")?;
    ensure!(
        fills.len() < 2_000,
        "Hyperliquid fill history window overflowed"
    );
    for trade in fills.iter().filter(|trade| trade["coin"] == config.coin) {
        if number(trade, "time")? >= request.start_ms {
            inputs.push(hyper::fill(trade, seed)?);
        }
    }
    let orders = info(json!({"type":"openOrders","user":account}))?;
    if config.mode == crate::maker::Mode::Hyperliquid && agent.is_some() {
        ensure!(
            orders
                .as_array()
                .context("missing Hyperliquid open orders")?
                .iter()
                .all(|order| order["coin"] == config.coin),
            "scheduled cancellation requires an isolated account with no other working orders"
        );
    }
    let mut open = Vec::new();
    for order in orders
        .as_array()
        .context("missing Hyperliquid open orders")?
        .iter()
        .filter(|order| order["coin"] == config.coin)
    {
        open.push((hyper::client(&order["cloid"], seed), number(order, "oid")?));
    }
    for order in orders
        .as_array()
        .into_iter()
        .flatten()
        .filter(|order| order["coin"] == config.coin)
    {
        if let Some(client) = hyper::client(&order["cloid"], seed) {
            inputs.push(Input::Order {
                venue: Venue::Hyperliquid,
                client: Some(client),
                oid: Some(number(order, "oid")?),
                status: crate::maker::Status::Open,
                filled: decimal(order, "origSz")? - decimal(order, "sz")?,
            });
        }
    }
    for target in request.targets.iter().filter(|target| {
        target.check
            && !open
                .iter()
                .any(|&(client, oid)| client == Some(target.client) || Some(oid) == target.oid)
    }) {
        let status = info(
            json!({"type":"orderStatus","user":account,"oid":hyper::cloid(seed, target.client)}),
        )?;
        if status["status"] == "order" && status["order"]["order"]["coin"] == config.coin {
            if let Some(input) = hyper::order(&status["order"], seed)? {
                inputs.push(input);
            }
        } else if status["status"] == "unknownOid"
            && target.abandonable
            && !open
                .iter()
                .any(|&(client, oid)| client == Some(target.client) || Some(oid) == target.oid)
        {
            inputs.push(Input::Order {
                venue: Venue::Hyperliquid,
                client: Some(target.client),
                oid: target.oid,
                status: crate::maker::Status::Terminal,
                filled: target.reported,
            });
        }
    }

    let balance = info(json!({"type":"clearinghouseState","user":account}))?;
    let mut position = Decimal::ZERO;
    for entry in balance["assetPositions"]
        .as_array()
        .context("missing Hyperliquid positions")?
    {
        if entry["position"]["coin"] == config.coin {
            position += decimal(&entry["position"], "szi")?;
        }
    }
    inputs.push(Input::Account {
        venue: Venue::Hyperliquid,
        position,
        open,
        available: decimal(&balance, "withdrawable")?,
        requested_ms: request.requested_ms,
    });
    Ok(Snapshot {
        nonce: None,
        inputs,
    })
}

fn lighter_snapshot(
    http: &Client,
    config: &Config,
    accounts: &Account,
    request: &Request,
) -> Result<Snapshot> {
    let get = |path: &str, params: &[(&str, String)]| -> Result<Value> {
        let mut builder = http
            .get(format!("{}/api/v1/{path}", lighter::BASE))
            .query(params);
        if let Some(auth) = &request.auth {
            builder = builder.header("Authorization", auth);
        }
        // Avoid including response bodies or credential-bearing URLs in diagnostics.
        let value: Value = builder
            .send()
            .and_then(|response| response.error_for_status())
            .and_then(|response| response.json())
            .map_err(|error| {
                anyhow::anyhow!(
                    "Lighter {path} HTTP request failed: {}",
                    error.without_url()
                )
            })?;
        ensure!(value["code"] == 200, "Lighter {path} returned an API error");
        Ok(value)
    };
    let market = config.lighter_market.context("missing Lighter market")?;
    let mut inputs = Vec::new();
    let mut nonce = None;
    if request.first {
        let meta = get("orderBookDetails", &[("filter", "perp".into())])?;
        inputs.push(Input::Instrument {
            venue: Venue::Lighter,
            instrument: lighter::instrument(&meta, market, &config.coin)?,
        });
        if let Some((account, key)) = accounts.lighter.zip(accounts.key_index) {
            let response = get(
                "nextNonce",
                &[
                    ("account_index", account.to_string()),
                    ("api_key_index", key.to_string()),
                ],
            )?;
            nonce = Some(number(&response, "nonce")?);
        }
    }
    let Some((account, _)) = accounts.lighter.zip(request.auth.as_ref()) else {
        return Ok(Snapshot { nonce, inputs });
    };

    let params = [
        ("account_index", account.to_string()),
        ("market_id", market.to_string()),
    ];
    let mut query = params.to_vec();
    query.extend([
        ("limit", "100".into()),
        ("sort_by", "timestamp".into()),
        ("sort_dir", "desc".into()),
    ]);
    let trades = get("trades", &query)?;
    let trades = trades["trades"]
        .as_array()
        .context("missing Lighter trade history")?;
    let mut oldest = u64::MAX;
    for trade in trades {
        let time = number(trade, "timestamp")?;
        oldest = oldest.min(time);
        if time >= request.start_ms {
            inputs.push(lighter::fill(trade, account)?);
        }
    }
    ensure!(
        trades.len() < 100 || oldest < request.cutoff_ms,
        "Lighter fill history window overflowed"
    );

    let orders = get("accountActiveOrders", &params)?;
    let mut open = Vec::new();
    for order in orders["orders"]
        .as_array()
        .context("missing Lighter open orders")?
    {
        open.push((
            Some(number(order, "client_order_index")?),
            number(order, "order_index")?,
        ));
    }
    inputs.extend(lighter::order_updates(&orders["orders"], market)?);
    if request.targets.iter().any(|target| {
        target.check
            && !open
                .iter()
                .any(|&(client, oid)| client == Some(target.client) || Some(oid) == target.oid)
    }) {
        let mut query = params.to_vec();
        query.push(("limit", "100".into()));
        let orders = get("accountInactiveOrders", &query)?;
        let inactive = orders["orders"]
            .as_array()
            .context("missing Lighter inactive orders")?;
        inputs.extend(lighter::order_updates(&orders["orders"], market)?);
        if inactive.len() < 100 {
            for target in request.targets.iter().filter(|target| target.abandonable) {
                if !open
                    .iter()
                    .any(|&(client, oid)| client == Some(target.client) || Some(oid) == target.oid)
                    && !inactive
                        .iter()
                        .any(|order| order["client_order_index"] == target.client)
                {
                    inputs.push(Input::Order {
                        venue: Venue::Lighter,
                        client: Some(target.client),
                        oid: target.oid,
                        status: crate::maker::Status::Terminal,
                        filled: target.reported,
                    });
                }
            }
        }
    }

    let balance = get(
        "account",
        &[("by", "index".into()), ("value", account.to_string())],
    )?;
    let account = balance["accounts"]
        .as_array()
        .context("missing Lighter accounts")?
        .iter()
        .find(|entry| entry["index"] == account || entry["account_index"] == account)
        .context("configured Lighter testnet account is absent")?;
    let mut position = Decimal::ZERO;
    for entry in account["positions"]
        .as_array()
        .context("missing Lighter positions")?
    {
        if entry["market_id"] == market {
            let sign = entry["sign"]
                .as_i64()
                .context("missing Lighter position sign")?;
            ensure!(matches!(sign, -1 | 1), "invalid Lighter position sign");
            position += decimal(entry, "position")? * Decimal::from(sign);
        }
    }
    inputs.push(Input::Account {
        venue: Venue::Lighter,
        position,
        open,
        available: decimal(account, "available_balance")?,
        requested_ms: request.requested_ms,
    });
    Ok(Snapshot { nonce, inputs })
}
