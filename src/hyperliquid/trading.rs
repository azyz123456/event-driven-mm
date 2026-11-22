use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};

use crate::book::best_prices;
use crate::hyperliquid::Snapshot;
use crate::order_test::{Config, Evidence, Input, Source, Status, Stream};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Request {
    Meta,
    AgentRole,
    OpenOrders { verification: bool },
    Position { verification: bool },
    Status,
    Place,
    Cancel,
}

impl Request {
    pub fn info(self, config: &Config, agent: &str, id: u64) -> String {
        let payload = match self {
            Self::Meta => json!({"type": "meta"}),
            Self::AgentRole => json!({"type": "userRole", "user": agent}),
            Self::OpenOrders { .. } => json!({"type": "openOrders", "user": config.account}),
            Self::Position { .. } => json!({"type": "clearinghouseState", "user": config.account}),
            Self::Status => {
                json!({"type": "orderStatus", "user": config.account, "oid": config.cloid})
            }
            _ => unreachable!("signed requests are created by the signer"),
        };
        json!({"method": "post", "id": id, "request": {"type": "info", "payload": payload}})
            .to_string()
    }

    pub fn startup() -> [Self; 3] {
        [
            Self::Meta,
            Self::OpenOrders {
                verification: false,
            },
            Self::Position {
                verification: false,
            },
        ]
    }

    pub fn verification() -> [Self; 2] {
        [
            Self::OpenOrders { verification: true },
            Self::Position { verification: true },
        ]
    }
}

pub fn subscribe(stream: Stream, account: &str) -> String {
    let channel = match stream {
        Stream::Orders => "orderUpdates",
        Stream::Fills => "userFills",
    };
    json!({"method": "subscribe", "subscription": {"type": channel, "user": account}}).to_string()
}

pub fn signed_post(id: u64, payload: &RawValue) -> String {
    format!(r#"{{"method":"post","id":{id},"request":{{"type":"action","payload":{payload}}}}}"#)
}

pub fn response_id(message: &Value) -> Result<Option<u64>> {
    if message["channel"] != "post" {
        return Ok(None);
    }
    message["data"]["id"]
        .as_u64()
        .context("post response is missing a numeric request ID")
        .map(Some)
}

pub fn observations(
    message: &Value,
    request: Option<Request>,
    config: &Config,
) -> Result<Vec<Input>> {
    let data = &message["data"];
    match message["channel"]
        .as_str()
        .context("message is missing a channel")?
    {
        "post" => request.map_or(Ok(vec![]), |request| {
            post_response(&data["response"], request, config).map(|input| vec![input])
        }),
        "l2Book" => {
            let snapshot = Snapshot::deserialize(data).context("decode testnet book")?;
            if snapshot.coin != config.coin {
                return Ok(vec![]);
            }
            Ok(vec![Input::Book {
                best_bid: best_prices(&snapshot).map(|(bid, _)| bid),
            }])
        }
        "subscriptionResponse" => {
            let sub = &data["subscription"];
            if data["method"] != "subscribe"
                || !sub["user"]
                    .as_str()
                    .is_some_and(|user| user.eq_ignore_ascii_case(&config.account))
            {
                return Ok(vec![]);
            }
            let stream = match sub["type"].as_str() {
                Some("orderUpdates") => Stream::Orders,
                Some("userFills") => Stream::Fills,
                _ => return Ok(vec![]),
            };
            Ok(vec![Input::Subscribed { stream }])
        }
        "orderUpdates" => {
            let updates = data
                .as_array()
                .context("orderUpdates is missing its array")?;
            updates
                .iter()
                .filter(|item| item["order"]["coin"] == config.coin)
                .map(|item| {
                    evidence(item).map(|evidence| Input::Order {
                        evidence,
                        source: Source::Update,
                    })
                })
                .collect()
        }
        "userFills" => {
            if !data["user"]
                .as_str()
                .is_some_and(|user| user.eq_ignore_ascii_case(&config.account))
            {
                return Ok(vec![]);
            }
            let fills = data["fills"]
                .as_array()
                .context("userFills is missing its array")?;
            let mut inputs = vec![Input::Subscribed {
                stream: Stream::Fills,
            }];
            for fill in fills.iter().filter(|fill| fill["coin"] == config.coin) {
                inputs.push(Input::Fill {
                    oid: fill["oid"]
                        .as_u64()
                        .context("fill is missing its order ID")?,
                });
            }
            Ok(inputs)
        }
        "error" => Ok(vec![Input::PreflightFailed {
            reason: "venue reported a WebSocket error".into(),
        }]),
        _ => Ok(vec![]),
    }
}

fn post_response(response: &Value, request: Request, config: &Config) -> Result<Input> {
    let payload = &response["payload"];
    if matches!(request, Request::Place | Request::Cancel) {
        if response["type"] != "action" || payload["status"] != "ok" {
            return Ok(Input::ActionUncertain);
        }
        let result = &payload["response"];
        let statuses = result["data"]["statuses"].as_array();
        let Some([status]) = statuses.map(Vec::as_slice) else {
            return Ok(Input::ActionUncertain);
        };
        return Ok(match request {
            Request::Place if result["type"] == "order" => place_result(status, config),
            Request::Cancel
                if matches!(result["type"].as_str(), Some("cancel" | "cancelByCloid")) =>
            {
                Input::CancelReply
            }
            _ => Input::ActionUncertain,
        });
    }

    if response["type"] != "info" {
        return Ok(match request {
            Request::Status => Input::StatusUnknown,
            Request::OpenOrders { verification: true }
            | Request::Position { verification: true } => Input::ActionUncertain,
            _ => Input::PreflightFailed {
                reason: format!("venue rejected {request:?} preflight query"),
            },
        });
    }
    // Testnet wraps info responses as { type, data }. Direct payloads are also accepted.
    let body = payload
        .get("data")
        .filter(|_| payload.get("type").is_some())
        .unwrap_or(payload);
    match request {
        Request::AgentRole => {
            if body["role"] == "agent"
                && body["data"]["user"]
                    .as_str()
                    .is_some_and(|user| user.eq_ignore_ascii_case(&config.account))
            {
                Ok(Input::AgentVerified)
            } else {
                Ok(Input::PreflightFailed {
                    reason: "testnet API wallet does not belong to the configured account".into(),
                })
            }
        }
        Request::Meta => {
            let universe = body["universe"]
                .as_array()
                .context("metadata is missing its universe")?;
            let Some((index, entry)) = universe
                .iter()
                .enumerate()
                .find(|(_, entry)| entry["name"] == config.coin)
            else {
                return Ok(Input::PreflightFailed {
                    reason: "instrument is absent from testnet perpetual metadata".into(),
                });
            };
            if entry["isDelisted"] == true {
                return Ok(Input::PreflightFailed {
                    reason: "instrument is delisted".into(),
                });
            }
            let size_decimals: u32 = entry["szDecimals"]
                .as_u64()
                .context("metadata is missing size precision")?
                .try_into()?;
            ensure!(size_decimals <= 6, "invalid perpetual lot precision");
            Ok(Input::Asset {
                asset: index.try_into()?,
                size_decimals,
            })
        }
        Request::OpenOrders { verification } => {
            let orders = body.as_array().context("openOrders is missing its array")?;
            let mut count = 0;
            for order in orders {
                count += usize::from(
                    order["coin"]
                        .as_str()
                        .context("open order is missing its instrument")?
                        == config.coin,
                );
            }
            Ok(Input::OpenOrders {
                verification,
                count,
            })
        }
        Request::Position { verification } => {
            let positions = body["assetPositions"]
                .as_array()
                .context("account summary is missing assetPositions")?;
            let mut flat = true;
            for entry in positions {
                let position = &entry["position"];
                if position["coin"]
                    .as_str()
                    .context("position is missing its instrument")?
                    == config.coin
                {
                    flat &= decimal(&position["szi"], "position size")?.is_zero();
                }
            }
            Ok(Input::Position { verification, flat })
        }
        Request::Status if body["status"] == "order" => {
            let order = &body["order"];
            ensure!(
                order["order"]["coin"] == config.coin,
                "status response references a different instrument"
            );
            Ok(Input::Order {
                evidence: evidence(order)?,
                source: Source::Status,
            })
        }
        Request::Status => Ok(Input::StatusUnknown),
        Request::Place | Request::Cancel => unreachable!(),
    }
}

fn place_result(status: &Value, config: &Config) -> Input {
    if status["error"].is_string() {
        return Input::PlaceRejected {
            reason: "venue rejected the placement".into(),
        };
    }
    let (key, kind) = if status.get("resting").is_some() {
        ("resting", Status::Open)
    } else {
        ("filled", Status::Filled)
    };
    let Some(oid) = status[key]["oid"].as_u64() else {
        return Input::ActionUncertain;
    };
    Input::Order {
        evidence: Evidence {
            cloid: Some(config.cloid.clone()),
            oid,
            status: kind,
            partial: false,
        },
        source: Source::PlaceReply,
    }
}

fn evidence(wrapper: &Value) -> Result<Evidence> {
    let order = &wrapper["order"];
    let venue_status = wrapper["status"]
        .as_str()
        .context("order is missing its status")?;
    let status = match venue_status {
        "open" => Status::Open,
        "canceled" => Status::Canceled,
        "filled" => Status::Filled,
        "scheduledCancel" => Status::OtherTerminal(venue_status.into()),
        value
            if value.ends_with("Canceled")
                || value.ends_with("Rejected")
                || value == "rejected" =>
        {
            Status::OtherTerminal(value.into())
        }
        _ => Status::Unknown,
    };
    let partial = status == Status::Open
        && decimal(&order["sz"], "remaining order size")?
            < decimal(&order["origSz"], "original order size")?;
    Ok(Evidence {
        cloid: order["cloid"].as_str().map(ToOwned::to_owned),
        oid: order["oid"]
            .as_u64()
            .context("order is missing its exchange ID")?,
        status,
        partial,
    })
}

fn decimal(value: &Value, field: &str) -> Result<Decimal> {
    value
        .as_str()
        .with_context(|| format!("missing {field}"))?
        .parse()
        .with_context(|| format!("invalid {field}"))
}

pub fn redact(message: &mut Value) {
    // Some venue error replies can echo a request. Stop free-form errors and signed envelopes at the journal boundary.
    if message["channel"] == "error" {
        message["data"] = Value::Null;
    }
    redact_nested(message);
}

fn redact_nested(value: &mut Value) {
    match value {
        Value::Object(object) => {
            if object.get("type").and_then(Value::as_str) == Some("error") {
                object.remove("payload");
            }
            if object.get("status").and_then(Value::as_str) == Some("err") {
                object.remove("response");
            }
            object.remove("signature");
            for (key, value) in object {
                if key == "error" && value.is_string() {
                    *value = Value::String("venue error".into());
                } else {
                    redact_nested(value);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact_nested),
        _ => {}
    }
}
