use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, value::RawValue};

pub const HOST: &str = "api.hyperliquid.xyz";
pub const ENDPOINT: &str = "wss://api.hyperliquid.xyz/ws";
pub const PATH: &str = "/ws";
const BOOK_CHANNEL: &str = "l2Book";

#[derive(Debug, Deserialize)]
pub struct Snapshot {
    pub coin: String,
    pub levels: [Vec<Level>; 2],
}

#[derive(Debug, Deserialize)]
pub struct Level {
    #[serde(with = "rust_decimal::serde::str")]
    pub px: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub sz: Decimal,
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionResponse {
    pub method: String,
    pub subscription: Subscription,
}

#[derive(Debug, Deserialize)]
pub struct Subscription {
    #[serde(rename = "type")]
    pub channel: String,
    pub coin: String,
}

pub enum Incoming<'a> {
    Book(Snapshot),
    SubscriptionResponse(SubscriptionResponse),
    Error(Option<&'a RawValue>),
    Other,
}

pub fn subscribe(coin: &str) -> String {
    json!({"method": "subscribe", "subscription": {"type": BOOK_CHANNEL, "coin": coin}}).to_string()
}

pub fn decode(text: &str) -> Result<Incoming<'_>> {
    #[derive(Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow)]
        channel: Cow<'a, str>,
        #[serde(borrow)]
        data: Option<&'a RawValue>,
    }

    let envelope: Envelope<'_> =
        serde_json::from_str(text).context("decode Hyperliquid message")?;
    let data = || envelope.data.context("Hyperliquid message is missing data");
    match envelope.channel.as_ref() {
        BOOK_CHANNEL => Ok(Incoming::Book(
            serde_json::from_str(data()?.get()).context("decode Hyperliquid book snapshot")?,
        )),
        "subscriptionResponse" => Ok(Incoming::SubscriptionResponse(
            serde_json::from_str(data()?.get()).context("decode Hyperliquid subscription reply")?,
        )),
        "error" => Ok(Incoming::Error(envelope.data)),
        _ => Ok(Incoming::Other),
    }
}

impl SubscriptionResponse {
    pub fn confirms_book(&self, coin: &str) -> bool {
        self.method == "subscribe"
            && self.subscription.channel == BOOK_CHANNEL
            && self.subscription.coin == coin
    }
}
use std::borrow::Cow;
