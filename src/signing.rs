use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use chrono::DateTime;
use hypersdk::hypercore::{
    BatchCancelCloid, BatchOrder, CancelByCloid, Chain, Cloid, OrderGrouping, OrderRequest,
    OrderTypePlacement, PrivateKeySigner, TimeInForce as ExchangeTif,
    api::Action as ExchangeAction,
};
use serde_json::{Value, value::RawValue};

use crate::intent::{Action, Intent, Side, TimeInForce};

pub const KEY_ENV: &str = "HYPERLIQUID_TESTNET_AGENT_KEY";

pub struct Signer {
    key: PrivateKeySigner,
    last_nonce: u64,
}

pub struct SignedAction {
    pub nonce: u64,
    pub expires_after: u64,
    pub action: Value,
    pub wire_payload: Box<RawValue>,
}

impl Signer {
    pub fn from_env(account: &str) -> Result<Self> {
        let key = env::var(KEY_ENV)
            .with_context(|| format!("set {KEY_ENV} to the approved testnet API wallet key"))?;
        let signer = Self::new(&key)?;
        ensure!(
            !signer.address().eq_ignore_ascii_case(account),
            "use a dedicated API wallet key distinct from the account address"
        );
        Ok(signer)
    }

    pub fn new(key: &str) -> Result<Self> {
        let key = key
            .parse::<PrivateKeySigner>()
            .ok()
            .context("invalid testnet API wallet key")?;
        Ok(Self { key, last_nonce: 0 })
    }

    pub fn address(&self) -> String {
        format!("{:#x}", self.key.address())
    }

    pub fn sign(&mut self, intent: &Intent, ttl_ms: u64) -> Result<SignedAction> {
        let now: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .try_into()?;
        let nonce = now.max(self.last_nonce.checked_add(1).context("nonce overflow")?);
        let expires_after = nonce
            .checked_add(ttl_ms)
            .context("action expiry overflow")?;
        let expiry = DateTime::from_timestamp_millis(expires_after.try_into()?)
            .context("invalid action expiry")?;
        let cloid = intent
            .cloid
            .parse::<Cloid>()
            .context("invalid client order ID")?;
        let action = match intent.action {
            Action::Place {
                side: Side::Buy,
                tif: TimeInForce::Alo,
                price,
                size,
                ..
            } => ExchangeAction::from(BatchOrder {
                orders: vec![OrderRequest {
                    asset: intent.asset.try_into()?,
                    is_buy: true,
                    limit_px: price,
                    sz: size,
                    reduce_only: false,
                    order_type: OrderTypePlacement::Limit {
                        tif: ExchangeTif::Alo,
                    },
                    cloid,
                }],
                grouping: OrderGrouping::Na,
                builder: None,
            }),
            Action::Cancel { .. } => ExchangeAction::from(BatchCancelCloid {
                cancels: vec![CancelByCloid {
                    asset: intent.asset,
                    cloid,
                }],
                fast: false,
            }),
        };
        let action_json = serde_json::to_value(&action)?;
        let signed = action.sign_sync(&self.key, nonce, None, Some(expiry), Chain::Testnet)?;
        self.last_nonce = nonce;
        Ok(SignedAction {
            nonce,
            expires_after,
            action: action_json,
            // Preserve the SDK's field order in the signed wire representation.
            wire_payload: serde_json::value::to_raw_value(&signed)?,
        })
    }
}
