use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Intent {
    pub id: u64,
    pub caused_by_seq: u64,
    pub asset: u32,
    pub cloid: String,
    #[serde(flatten)]
    pub action: Action,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    Place {
        side: Side,
        tif: TimeInForce,
        #[serde(with = "rust_decimal::serde::str")]
        price: Decimal,
        #[serde(with = "rust_decimal::serde::str")]
        size: Decimal,
        book_at_ms: u64,
    },
    Cancel {
        oid: u64,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TimeInForce {
    Alo,
}
