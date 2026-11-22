use std::collections::HashSet;

use anyhow::{Result, anyhow, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::intent::{Action, Intent, Side, TimeInForce};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub account: String,
    pub coin: String,
    pub cloid: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_notional: Decimal,
    pub timings: Timings,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Timings {
    pub ready_ms: u64,
    pub max_book_age_ms: u64,
    pub ack_ms: u64,
    pub action_expiry_ms: u64,
    pub reconciliation_ms: u64,
    pub probe_interval_ms: u64,
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            ready_ms: 15_000,
            max_book_age_ms: 2_000,
            ack_ms: 3_000,
            action_expiry_ms: 8_000,
            reconciliation_ms: 20_000,
            probe_interval_ms: 750,
        }
    }
}

impl Config {
    pub fn new(
        account: String,
        coin: String,
        price: Decimal,
        size: Decimal,
        max_notional: Decimal,
    ) -> Result<Self> {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes)
            .map_err(|error| anyhow!("generate client order ID: {error}"))?;
        let cloid = hypersdk::hypercore::Cloid::from(bytes).to_string();
        let config = Self {
            account: account.to_ascii_lowercase(),
            coin,
            cloid,
            price: price.normalize(),
            size: size.normalize(),
            max_notional: max_notional.normalize(),
            timings: Timings::default(),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            hex_digits(&self.account, 40),
            "expected a 0x-prefixed testnet account address"
        );
        ensure!(
            hex_digits(&self.cloid, 32) && self.cloid[2..].bytes().any(|b| b != b'0'),
            "invalid client order ID"
        );
        ensure!(
            !self.coin.is_empty()
                && self
                    .coin
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-'),
            "expected a default-dex perpetual symbol"
        );
        ensure!(
            self.price > Decimal::ZERO && self.size > Decimal::ZERO,
            "price and size must be positive"
        );
        let notional = self
            .price
            .checked_mul(self.size)
            .ok_or_else(|| anyhow!("notional exceeds decimal range"))?;
        ensure!(
            notional >= Decimal::TEN,
            "order notional is below the perpetual minimum"
        );
        ensure!(
            notional <= self.max_notional,
            "order exceeds the configured maximum notional"
        );
        let t = &self.timings;
        ensure!(
            t.ready_ms > 0
                && t.max_book_age_ms > 0
                && t.ack_ms > 0
                && t.action_expiry_ms > t.ack_ms
                && t.reconciliation_ms > t.action_expiry_ms
                && t.probe_interval_ms > 0,
            "invalid order-test deadlines"
        );
        Ok(())
    }

    fn check_precision(&self, size_decimals: u32) -> Result<()> {
        let price = self.price.normalize();
        ensure!(
            self.size.normalize().scale() <= size_decimals,
            "size exceeds the instrument's lot precision"
        );
        ensure!(
            price.is_integer()
                || (price.scale() <= 6_u32.saturating_sub(size_decimals)
                    && price.mantissa().unsigned_abs().ilog10() < 5),
            "price exceeds the instrument's tick precision"
        );
        Ok(())
    }
}

fn hex_digits(value: &str, count: usize) -> bool {
    value
        .strip_prefix("0x")
        .is_some_and(|s| s.len() == count && s.bytes().all(|b| b.is_ascii_hexdigit()))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Input {
    Tick,
    Disconnected {
        reason: String,
    },
    RecoveryUnavailable {
        reason: String,
    },
    Book {
        #[serde(with = "rust_decimal::serde::str_option")]
        best_bid: Option<Decimal>,
    },
    Subscribed {
        stream: Stream,
    },
    Asset {
        asset: u32,
        size_decimals: u32,
    },
    AgentVerified,
    PreflightFailed {
        reason: String,
    },
    OpenOrders {
        verification: bool,
        count: usize,
    },
    Position {
        verification: bool,
        flat: bool,
    },
    Order {
        evidence: Evidence,
        source: Source,
    },
    Fill {
        oid: u64,
    },
    PlaceRejected {
        reason: String,
    },
    CancelReply,
    ActionUncertain,
    PlaceNotSent {
        reason: String,
    },
    StatusUnknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stream {
    Orders,
    Fills,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    PlaceReply,
    Update,
    Status,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Evidence {
    pub cloid: Option<String>,
    pub oid: u64,
    pub status: Status,
    pub partial: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Open,
    Canceled,
    Filled,
    OtherTerminal(String),
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    VerifiedCanceled {
        oid: u64,
    },
    CanceledAfterRetry {
        oid: u64,
    },
    Rejected {
        reason: String,
    },
    Filled {
        oid: u64,
        position_flat: bool,
    },
    Unresolved {
        oid: Option<u64>,
        fill_observed: bool,
        reason: String,
    },
}

impl Outcome {
    pub fn passed(&self) -> bool {
        matches!(self, Self::VerifiedCanceled { .. })
    }
}

#[derive(Debug)]
pub enum Command {
    Trade(Intent),
    QueryStatus,
    VerifyAccount,
    Reconnect,
    Finish(Outcome),
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Waiting,
    Placing,
    Canceling,
    Checking,
    Verifying,
    Finished,
}

#[derive(Default)]
struct AccountCheck {
    open_orders: Option<usize>,
    position_flat: Option<bool>,
}

pub struct Lifecycle {
    config: Config,
    phase: Phase,
    asset: Option<u32>,
    book: Option<(Decimal, u64)>,
    orders_ready: bool,
    fills_ready: bool,
    agent_verified: bool,
    account: AccountCheck,
    elapsed_ms: u64,
    ack_at: u64,
    next_probe: u64,
    expires_at: Option<u64>,
    oid: Option<u64>,
    resting: bool,
    fill_observed: bool,
    early_fills: HashSet<u64>,
    terminal: Option<Status>,
    cancels: u8,
    next_intent: u64,
}

impl Lifecycle {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            phase: Phase::Waiting,
            asset: None,
            book: None,
            orders_ready: false,
            fills_ready: false,
            agent_verified: false,
            account: AccountCheck::default(),
            elapsed_ms: 0,
            ack_at: 0,
            next_probe: 0,
            expires_at: None,
            oid: None,
            resting: false,
            fill_observed: false,
            early_fills: HashSet::new(),
            terminal: None,
            cancels: 0,
            next_intent: 1,
        }
    }

    pub fn update(&mut self, seq: u64, elapsed_ms: u64, input: Input) -> Result<Option<Command>> {
        ensure!(
            elapsed_ms >= self.elapsed_ms,
            "order input elapsed time went backward"
        );
        self.elapsed_ms = elapsed_ms;
        if self.phase == Phase::Finished {
            return Ok(None);
        }

        let command = match input {
            Input::Disconnected { reason } if self.phase == Phase::Waiting => self.reject(reason),
            Input::Disconnected { .. } => {
                self.agent_verified = false;
                self.check_status();
                Some(Command::Reconnect)
            }
            Input::RecoveryUnavailable { reason } => self.unresolved(reason),
            Input::PreflightFailed { reason } if self.phase == Phase::Waiting => {
                self.reject(reason)
            }
            Input::PlaceNotSent { reason } => self.reject(reason),
            Input::PlaceRejected { reason }
                if self.oid.is_none() && matches!(self.phase, Phase::Placing | Phase::Checking) =>
            {
                self.reject(reason)
            }
            Input::CancelReply if self.phase == Phase::Canceling => self.check_status(),
            Input::ActionUncertain if matches!(self.phase, Phase::Placing | Phase::Canceling) => {
                self.check_status()
            }
            Input::Book { best_bid } if self.phase == Phase::Waiting => {
                self.book = best_bid.map(|bid| (bid, elapsed_ms));
                None
            }
            Input::Subscribed { stream } if self.phase == Phase::Waiting => {
                match stream {
                    Stream::Orders => self.orders_ready = true,
                    Stream::Fills => self.fills_ready = true,
                }
                None
            }
            Input::AgentVerified => {
                self.agent_verified = true;
                None
            }
            Input::Asset {
                asset,
                size_decimals,
            } if self.phase == Phase::Waiting => {
                if let Err(error) = self.config.check_precision(size_decimals) {
                    return Ok(self.reject(error.to_string()));
                }
                self.asset = Some(asset);
                None
            }
            Input::OpenOrders {
                verification,
                count,
            } if self.check_account(verification) => {
                self.account.open_orders = Some(count);
                None
            }
            Input::Position { verification, flat } if self.check_account(verification) => {
                self.account.position_flat = Some(flat);
                None
            }
            Input::Fill { oid } if self.phase != Phase::Waiting => {
                if self.oid == Some(oid) {
                    self.fill_observed = true;
                }
                if self.oid.is_none() {
                    self.early_fills.insert(oid);
                }
                None
            }
            Input::Order { evidence, source } if self.phase != Phase::Waiting => {
                self.on_order(seq, evidence, source)
            }
            _ => None,
        };
        if command.is_some() {
            return Ok(command);
        }
        let command = match self.phase {
            Phase::Waiting => self.maybe_place(seq),
            Phase::Placing | Phase::Canceling if elapsed_ms >= self.ack_at => self.check_status(),
            Phase::Checking | Phase::Verifying
                if self.expires_at.is_some_and(|end| elapsed_ms >= end) =>
            {
                self.unresolved("reconciliation deadline expired".into())
            }
            Phase::Checking if elapsed_ms >= self.next_probe => self.probe(),
            Phase::Verifying => self
                .maybe_finish()
                .or_else(|| (elapsed_ms >= self.next_probe).then(|| self.verify_account())),
            _ => None,
        };
        Ok(command)
    }

    fn check_account(&self, verification: bool) -> bool {
        if verification {
            self.phase == Phase::Verifying
        } else {
            self.phase == Phase::Waiting
        }
    }

    fn maybe_place(&mut self, seq: u64) -> Option<Command> {
        if self.elapsed_ms >= self.config.timings.ready_ms {
            return self.reject("preflight or usable book deadline expired".into());
        }
        if self.account.open_orders.is_some_and(|count| count > 0)
            || self.account.position_flat == Some(false)
        {
            return self.reject("test instrument already has an open order or position".into());
        }
        let asset = self.asset?;
        let (bid, book_at_ms) = self.book?;
        if self.agent_verified
            && self.orders_ready
            && self.fills_ready
            && self.account.open_orders == Some(0)
            && self.account.position_flat == Some(true)
            && self.config.price < bid
            && self.elapsed_ms - book_at_ms <= self.config.timings.max_book_age_ms
        {
            self.phase = Phase::Placing;
            self.ack_at = self.elapsed_ms.saturating_add(self.config.timings.ack_ms);
            return self.trade(
                seq,
                asset,
                Action::Place {
                    side: Side::Buy,
                    tif: TimeInForce::Alo,
                    price: self.config.price,
                    size: self.config.size,
                    book_at_ms,
                },
            );
        }
        None
    }

    fn on_order(&mut self, seq: u64, evidence: Evidence, source: Source) -> Option<Command> {
        let Evidence {
            cloid,
            oid,
            status,
            partial,
        } = evidence;
        let addressed = cloid
            .as_deref()
            .is_some_and(|id| id.eq_ignore_ascii_case(&self.config.cloid));
        if (cloid.is_some() && !addressed)
            || (!addressed && self.oid != Some(oid) && source == Source::Update)
        {
            return None;
        }
        if self.oid.is_some_and(|known| known != oid) {
            return self.unresolved("conflicting exchange order IDs".into());
        }
        self.oid = Some(oid);
        self.fill_observed |=
            partial || status == Status::Filled || self.early_fills.contains(&oid);
        self.early_fills.clear();

        let first_cancel =
            self.cancels == 0 && matches!(self.phase, Phase::Placing | Phase::Checking);
        let cleanup_cancel = self.cancels == 1
            && self.phase == Phase::Checking
            && source == Source::Status
            && self.elapsed_ms >= self.ack_at;
        match status {
            Status::Open
                if self.agent_verified
                    && self.terminal.is_none()
                    && (first_cancel || cleanup_cancel) =>
            {
                let asset = self.asset?;
                self.resting = true;
                self.cancels += 1;
                self.phase = Phase::Canceling;
                self.ack_at = self.elapsed_ms.saturating_add(self.config.timings.ack_ms);
                self.trade(seq, asset, Action::Cancel { oid })
            }
            Status::Open | Status::Unknown => None,
            status => {
                if source == Source::Status && self.phase == Phase::Checking {
                    self.terminal = Some(status);
                    self.account = AccountCheck::default();
                    self.phase = Phase::Verifying;
                    return Some(self.verify_account());
                }
                // Stream evidence prevents a stale open update from triggering a cancel.
                self.terminal.get_or_insert(status);
                if matches!(self.phase, Phase::Placing | Phase::Canceling) {
                    return self.check_status();
                }
                None
            }
        }
    }

    fn check_status(&mut self) -> Option<Command> {
        self.phase = Phase::Checking;
        self.expires_at.get_or_insert(
            self.elapsed_ms
                .saturating_add(self.config.timings.reconciliation_ms),
        );
        self.probe()
    }

    fn probe(&mut self) -> Option<Command> {
        self.next_probe = self
            .elapsed_ms
            .saturating_add(self.config.timings.probe_interval_ms);
        Some(Command::QueryStatus)
    }

    fn verify_account(&mut self) -> Command {
        self.next_probe = self.elapsed_ms.saturating_add(self.config.timings.ack_ms);
        Command::VerifyAccount
    }

    fn maybe_finish(&mut self) -> Option<Command> {
        let (count, flat, oid) = (
            self.account.open_orders?,
            self.account.position_flat?,
            self.oid?,
        );
        if count > 0 {
            return None;
        }
        let outcome = if self.fill_observed || !flat {
            Outcome::Filled {
                oid,
                position_flat: flat,
            }
        } else if self.terminal == Some(Status::Canceled) && self.resting {
            match self.cancels {
                1 => Outcome::VerifiedCanceled { oid },
                2 => Outcome::CanceledAfterRetry { oid },
                _ => Outcome::Rejected {
                    reason: "order canceled before the test sent a cancel".into(),
                },
            }
        } else {
            Outcome::Rejected {
                reason: format!("terminal order status: {:?}", self.terminal),
            }
        };
        self.finish(outcome)
    }

    fn trade(&mut self, seq: u64, asset: u32, action: Action) -> Option<Command> {
        let id = self.next_intent;
        self.next_intent += 1;
        Some(Command::Trade(Intent {
            id,
            caused_by_seq: seq,
            asset,
            cloid: self.config.cloid.clone(),
            action,
        }))
    }

    fn reject(&mut self, reason: String) -> Option<Command> {
        self.finish(Outcome::Rejected { reason })
    }

    fn unresolved(&mut self, reason: String) -> Option<Command> {
        self.finish(Outcome::Unresolved {
            oid: self.oid,
            fill_observed: self.fill_observed,
            reason,
        })
    }

    fn finish(&mut self, outcome: Outcome) -> Option<Command> {
        self.phase = Phase::Finished;
        Some(Command::Finish(outcome))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Driver {
        state: Lifecycle,
        seq: u64,
        elapsed: u64,
    }

    impl Driver {
        fn new() -> Result<Self> {
            let config = Config::new(
                "0x0101010101010101010101010101010101010101".into(),
                "BTC".into(),
                "99".parse()?,
                "0.2".parse()?,
                "20".parse()?,
            )?;
            Ok(Self {
                state: Lifecycle::new(config),
                seq: 1,
                elapsed: 0,
            })
        }

        fn send(&mut self, input: Input) -> Result<Option<Command>> {
            self.seq += 1;
            self.elapsed += 1;
            self.state.update(self.seq, self.elapsed, input)
        }

        fn preflight(&mut self) -> Result<()> {
            for input in [
                Input::Asset {
                    asset: 1,
                    size_decimals: 5,
                },
                Input::AgentVerified,
                Input::Subscribed {
                    stream: Stream::Orders,
                },
                Input::Subscribed {
                    stream: Stream::Fills,
                },
                Input::OpenOrders {
                    verification: false,
                    count: 0,
                },
                Input::Position {
                    verification: false,
                    flat: true,
                },
            ] {
                assert!(self.send(input)?.is_none());
            }
            Ok(())
        }

        fn place(&mut self) -> Result<()> {
            self.preflight()?;
            assert!(matches!(
                self.send(Input::Book {
                    best_bid: Some(Decimal::new(100, 0))
                })?,
                Some(Command::Trade(Intent {
                    id: 1,
                    action: Action::Place { .. },
                    ..
                }))
            ));
            Ok(())
        }

        fn order(
            &mut self,
            source: Source,
            status: Status,
            partial: bool,
        ) -> Result<Option<Command>> {
            self.send(Input::Order {
                evidence: Evidence {
                    cloid: Some(self.state.config.cloid.clone()),
                    oid: 22,
                    status,
                    partial,
                },
                source,
            })
        }

        fn verify(&mut self, flat: bool) -> Result<Option<Command>> {
            assert!(
                self.send(Input::OpenOrders {
                    verification: true,
                    count: 0
                })?
                .is_none()
            );
            self.send(Input::Position {
                verification: true,
                flat,
            })
        }
    }

    #[test]
    fn stale_book_and_unconfirmed_account_cannot_trigger_placement() -> Result<()> {
        let mut order = Driver::new()?;
        assert!(
            order
                .send(Input::Book {
                    best_bid: Some(Decimal::new(100, 0))
                })?
                .is_none()
        );
        order.elapsed += order.state.config.timings.max_book_age_ms;
        order.preflight()?;
        assert!(order.send(Input::Tick)?.is_none());
        assert!(
            order
                .send(Input::Book {
                    best_bid: Some(Decimal::new(99, 0))
                })?
                .is_none()
        );
        assert!(matches!(
            order.send(Input::Book {
                best_bid: Some(Decimal::new(100, 0))
            })?,
            Some(Command::Trade(_))
        ));
        Ok(())
    }

    #[test]
    fn rejected_placement_ends_without_retrying_or_canceling() -> Result<()> {
        let mut order = Driver::new()?;
        order.place()?;
        let reason = "venue rejected the placement".to_owned();
        assert!(
            matches!(order.send(Input::PlaceRejected { reason: reason.clone() })?, Some(Command::Finish(Outcome::Rejected { reason: actual })) if actual == reason)
        );
        assert!(order.order(Source::Update, Status::Open, false)?.is_none());
        Ok(())
    }

    #[test]
    fn full_fill_before_resting_verifies_exposure_without_a_cancel() -> Result<()> {
        let mut order = Driver::new()?;
        order.place()?;
        assert!(matches!(
            order.order(Source::PlaceReply, Status::Filled, false)?,
            Some(Command::QueryStatus)
        ));
        assert!(order.order(Source::Update, Status::Open, false)?.is_none());
        assert!(matches!(
            order.order(Source::Status, Status::Filled, false)?,
            Some(Command::VerifyAccount)
        ));
        assert!(matches!(
            order.verify(false)?,
            Some(Command::Finish(Outcome::Filled {
                oid: 22,
                position_flat: false
            }))
        ));
        Ok(())
    }

    #[test]
    fn early_fill_and_partial_open_cancel_only_the_remainder_and_fail_the_smoke_test() -> Result<()>
    {
        let mut order = Driver::new()?;
        order.place()?;
        assert!(order.send(Input::Fill { oid: 22 })?.is_none());
        assert!(order.send(Input::Fill { oid: 22 })?.is_none());
        assert!(matches!(
            order.order(Source::Update, Status::Open, true)?,
            Some(Command::Trade(Intent {
                id: 2,
                action: Action::Cancel { oid: 22 },
                ..
            }))
        ));
        assert!(matches!(
            order.send(Input::CancelReply)?,
            Some(Command::QueryStatus)
        ));
        assert!(matches!(
            order.order(Source::Status, Status::Canceled, false)?,
            Some(Command::VerifyAccount)
        ));
        assert!(order.order(Source::Update, Status::Open, false)?.is_none());
        assert!(matches!(
            order.verify(true)?,
            Some(Command::Finish(Outcome::Filled {
                oid: 22,
                position_flat: true
            }))
        ));
        Ok(())
    }

    #[test]
    fn unknown_order_status_never_causes_a_second_placement() -> Result<()> {
        let mut order = Driver::new()?;
        order.place()?;
        order.elapsed += order.state.config.timings.ack_ms;
        assert!(matches!(
            order.send(Input::Tick)?,
            Some(Command::QueryStatus)
        ));
        assert!(order.send(Input::StatusUnknown)?.is_none());
        order.elapsed += order.state.config.timings.reconciliation_ms;
        assert!(matches!(
            order.send(Input::Tick)?,
            Some(Command::Finish(Outcome::Unresolved { oid: None, .. }))
        ));
        Ok(())
    }

    #[test]
    fn rest_confirmation_after_the_place_deadline_still_triggers_one_cancel() -> Result<()> {
        let mut order = Driver::new()?;
        order.place()?;
        order.elapsed += order.state.config.timings.ack_ms;
        assert!(matches!(
            order.send(Input::Tick)?,
            Some(Command::QueryStatus)
        ));
        assert!(matches!(
            order.order(Source::Update, Status::Open, false)?,
            Some(Command::Trade(Intent {
                id: 2,
                action: Action::Cancel { .. },
                ..
            }))
        ));
        assert!(
            order
                .order(Source::PlaceReply, Status::Open, false)?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn verifies_the_authoritative_terminal_status_after_a_stream_update() -> Result<()> {
        let mut order = Driver::new()?;
        order.place()?;
        assert!(matches!(
            order.order(Source::PlaceReply, Status::Open, false)?,
            Some(Command::Trade(_))
        ));
        assert!(matches!(
            order.order(Source::Update, Status::Canceled, false)?,
            Some(Command::QueryStatus)
        ));
        assert!(matches!(
            order.order(
                Source::Status,
                Status::OtherTerminal("scheduledCancel".into()),
                false
            )?,
            Some(Command::VerifyAccount)
        ));
        assert!(matches!(
            order.verify(true)?,
            Some(Command::Finish(Outcome::Rejected { .. }))
        ));
        Ok(())
    }
}
