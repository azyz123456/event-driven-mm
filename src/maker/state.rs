use std::collections::{BTreeMap, BTreeSet, VecDeque};

use anyhow::{Result, ensure};
use rust_decimal::Decimal;
use serde::Serialize;

use super::{
    Action, Config, Fill, Input, Instrument, Intent, Kind, Level, OrderRole, Status, Venue,
};
use crate::intent::Side;

#[derive(Default, Serialize)]
pub struct State {
    pub now_ms: u64,
    pub stop_ms: Option<u64>,
    #[serde(skip)]
    lighter_actions: VecDeque<u64>,
    pub orders: BTreeMap<u64, Order>,
    #[serde(skip)]
    active: BTreeSet<u64>,
    #[serde(skip)]
    order_ids: BTreeMap<(Venue, u64), u64>,
    #[serde(skip)]
    last_quotes: [Option<u64>; 2],
    #[serde(skip)]
    pub applied_fills: Vec<AppliedFill>,
    venues: [VenueState; 2],
    #[serde(skip)]
    seen: BTreeSet<(Venue, String)>,
    #[serde(skip)]
    unmatched: BTreeMap<(Venue, String), Fill>,
    pub protection_ms: Option<u64>,
    pub residual_since_ms: Option<u64>,
    pub last_hedge_ms: Option<u64>,
    pub passive_attempted_for: Option<u64>,
}

#[derive(Serialize)]
pub struct AppliedFill {
    pub venue: Venue,
    pub fill: Fill,
}

#[derive(Default, Serialize)]
pub struct VenueState {
    pub generation: Option<u64>,
    pub instrument: Option<Instrument>,
    pub book: Book,
    pub saw_book: bool,
    pub position: Decimal,
    pub fees: Decimal,
    pub cash: Decimal,
    pub fills: u64,
    pub account_ms: Option<u64>,
    pub available: Decimal,
    pub account_requested_ms: Option<u64>,
    pub fault: Option<String>,
    changed_ms: Option<u64>,
}

#[derive(Default, Serialize)]
pub struct Book {
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
    pub updated_ms: Option<u64>,
    sequence: Option<u64>,
}

#[derive(Serialize)]
pub struct Order {
    pub venue: Venue,
    pub generation: u64,
    pub oid: Option<u64>,
    pub side: Side,
    pub kind: Kind,
    pub role: OrderRole,
    pub price: Decimal,
    pub size: Decimal,
    pub filled: Decimal,
    pub reported: Decimal,
    pub sent_ms: u64,
    pub cancel_ms: Option<u64>,
    pub status: Option<Status>,
}

impl Order {
    pub fn done(&self) -> bool {
        self.status == Some(Status::Terminal) && self.reported == self.filled
    }

    pub fn outstanding(&self) -> Decimal {
        let remaining = if self.status == Some(Status::Terminal) {
            self.reported
        } else {
            self.size
        };
        remaining - self.filled
    }
}

impl Venue {
    fn index(self) -> usize {
        match self {
            Self::Hyperliquid => 0,
            Self::Lighter => 1,
        }
    }
}

impl State {
    pub fn venue(&self, venue: Venue) -> &VenueState {
        &self.venues[venue.index()]
    }

    fn venue_mut(&mut self, venue: Venue) -> &mut VenueState {
        &mut self.venues[venue.index()]
    }

    pub fn residual(&self) -> Decimal {
        self.venues.iter().map(|venue| venue.position).sum()
    }

    pub fn account_verified(&self, venue: Venue, config: &Config) -> bool {
        let state = self.venue(venue);
        state.generation.is_some()
            && state.fault.is_none()
            && state.available >= config.min_available_collateral
            && state
                .account_ms
                .is_some_and(|at| self.now_ms - at <= config.account_stale_ms)
            && !self.unmatched.keys().any(|(source, _)| *source == venue)
    }

    pub fn book_fresh(&self, venue: Venue, config: &Config) -> bool {
        let state = self.venue(venue);
        state.generation.is_some()
            && state.book.touch().is_some()
            && state
                .book
                .updated_ms
                .is_some_and(|at| self.now_ms - at <= config.book_stale_ms)
    }

    pub fn apply(
        &mut self,
        config: &Config,
        at_ms: u64,
        input: Input,
        fixed_lighter_fills: bool,
    ) -> Result<()> {
        ensure!(at_ms >= self.now_ms, "maker event time moved backwards");
        self.now_ms = at_ms;
        self.applied_fills.clear();
        while self
            .lighter_actions
            .front()
            .is_some_and(|&at| at_ms - at >= 60_000)
        {
            self.lighter_actions.pop_front();
        }
        let before = self.residual();
        match input {
            Input::Connected { venue, generation } => {
                if venue == Venue::Hyperliquid {
                    self.protection_ms = None;
                }
                let state = self.venue_mut(venue);
                state.generation = Some(generation);
                state.account_ms = None;
                state.book = Book::default();
            }
            Input::Disconnected { venue, .. } => {
                let state = self.venue_mut(venue);
                state.generation = None;
                state.account_ms = None;
                state.book = Book::default();
            }
            Input::Instrument { venue, instrument } => {
                ensure!(instrument.valid(venue), "invalid instrument metadata");
                let state = self.venue_mut(venue);
                ensure!(
                    state.instrument.as_ref().is_none_or(|old| {
                        old.asset == instrument.asset
                            && old.size_decimals == instrument.size_decimals
                            && old.price_decimals == instrument.price_decimals
                    }),
                    "instrument mapping changed during this session"
                );
                state.instrument = Some(instrument);
            }
            Input::Book {
                venue,
                sequence,
                previous,
                bids,
                asks,
            } => {
                let state = self.venue_mut(venue);
                state.book.update(at_ms, sequence, previous, bids, asks)?;
                state.saw_book |= state.book.touch().is_some();
            }
            Input::Account {
                venue,
                position,
                available,
                open,
                requested_ms,
            } => {
                ensure!(requested_ms <= at_ms, "account query is from the future");
                let state = self.venue(venue);
                // Ignore a snapshot racing a known action or a fill and try again next poll.
                if state
                    .changed_ms
                    .is_none_or(|changed| changed < requested_ms)
                {
                    let known = open.iter().all(|&(client, oid)| {
                        self.find_order(venue, client, Some(oid))
                            .is_some_and(|client| {
                                self.orders[&client].status != Some(Status::Terminal)
                            })
                    });
                    let state = self.venue_mut(venue);
                    state.available = available;
                    state.account_ms = (known && state.position == position).then_some(at_ms);
                    state.account_requested_ms = state.account_ms.map(|_| requested_ms);
                }
            }
            Input::Order {
                venue,
                client,
                oid,
                status,
                filled,
            } => {
                self.order(venue, client, oid, status, filled)?;
            }
            Input::Fill { venue, fill } => self.fill(config, venue, fill, fixed_lighter_fills)?,
            Input::Protection { expires_ms } => self.protection_ms = expires_ms,
            Input::Stop => self.stop_ms = Some(at_ms),
            Input::Tick | Input::PublicTrade { .. } | Input::PublicBook { .. } => {}
        }
        let after = self.residual();
        if after.is_zero() {
            self.residual_since_ms = None;
        } else if before.is_zero() || before.is_sign_negative() != after.is_sign_negative() {
            self.residual_since_ms = Some(at_ms);
        }
        Ok(())
    }

    fn order(
        &mut self,
        venue: Venue,
        client: Option<u64>,
        oid: Option<u64>,
        status: Status,
        filled: Decimal,
    ) -> Result<()> {
        let Some(client) = self.find_order(venue, client, oid) else {
            return Ok(());
        };
        let order = self.orders.get_mut(&client).expect("known order");
        ensure!(
            oid.is_none() || order.oid.is_none() || order.oid == oid,
            "order ID changed"
        );
        ensure!(
            filled >= Decimal::ZERO && filled <= order.size,
            "invalid reported fill total"
        );
        order.oid = order.oid.or(oid);
        if order.status != Some(Status::Terminal) {
            order.status = Some(status);
        }
        order.reported = order.reported.max(filled).max(order.filled);
        if order.done() {
            self.active.remove(&client);
        } else {
            self.active.insert(client);
        }
        if let Some(oid) = order.oid {
            self.order_ids.insert((venue, oid), client);
            let ids = self
                .unmatched
                .iter()
                .filter_map(|(key, fill)| {
                    (key.0 == venue && fill.oid == oid).then_some(key.clone())
                })
                .collect::<Vec<_>>();
            for key in ids {
                let fill = self.unmatched.remove(&key).expect("buffered fill");
                self.apply_fill(venue, client, &fill)?;
            }
        }
        Ok(())
    }

    fn fill(
        &mut self,
        config: &Config,
        venue: Venue,
        fill: Fill,
        fixed_lighter_fills: bool,
    ) -> Result<()> {
        ensure!(
            !fill.id.is_empty()
                && fill.price > Decimal::ZERO
                && fill.price <= Decimal::from(1_000_000_000_000_u64)
                && fill.size > Decimal::ZERO
                && fill.size <= Decimal::from(1_000_000_000_000_u64),
            "invalid account fill"
        );
        let key = (venue, fill.id.clone());
        if !self.seen.insert(key.clone()) {
            return Ok(());
        }
        if venue == Venue::Lighter && fixed_lighter_fills {
            ensure!(fill.maker, "fixed Lighter flow must contain maker fills");
            self.book_fill(venue, &fill);
        } else if let Some(client) = self.find_order(venue, fill.client, Some(fill.oid)) {
            self.apply_fill(venue, client, &fill)?;
        } else if config.mode.uses(venue) && self.unmatched.len() < 1024 {
            self.unmatched.insert(key, fill);
        } else {
            self.venue_mut(venue).fault = Some("unexpected account fill".into());
        }
        Ok(())
    }

    fn apply_fill(&mut self, venue: Venue, client: u64, fill: &Fill) -> Result<()> {
        let order = self.orders.get_mut(&client).expect("known order");
        if order.side != fill.side
            || (order.kind == Kind::PostOnly) != fill.maker
            || order.filled + fill.size > order.size
            || order.oid.is_some_and(|oid| oid != fill.oid)
        {
            self.venue_mut(venue).fault = Some("account fill conflicts with tracked order".into());
            return Ok(());
        }
        order.oid = Some(fill.oid);
        order.filled += fill.size;
        order.reported = order.reported.max(order.filled);
        if order.done() {
            self.active.remove(&client);
        } else {
            self.active.insert(client);
        }
        self.order_ids.insert((venue, fill.oid), client);
        self.book_fill(venue, fill);
        Ok(())
    }

    fn book_fill(&mut self, venue: Venue, fill: &Fill) {
        self.applied_fills.push(AppliedFill {
            venue,
            fill: fill.clone(),
        });
        let now = self.now_ms;
        let state = self.venue_mut(venue);
        let signed = fill.side.signed(fill.size);
        state.position += signed;
        state.cash -= signed * fill.price;
        state.fees += fill.fee;
        state.fills += 1;
        state.changed_ms = Some(now);
    }

    fn find_order(&self, venue: Venue, client: Option<u64>, oid: Option<u64>) -> Option<u64> {
        client
            .filter(|client| {
                self.orders
                    .get(client)
                    .is_some_and(|order| order.venue == venue)
            })
            .or_else(|| oid.and_then(|oid| self.order_ids.get(&(venue, oid)).copied()))
    }

    pub fn client_for_oid(&self, venue: Venue, oid: u64) -> Option<u64> {
        self.order_ids.get(&(venue, oid)).copied()
    }

    pub fn active_orders(&self) -> impl Iterator<Item = (u64, &Order)> {
        self.active
            .iter()
            .map(|&client| (client, &self.orders[&client]))
    }

    pub fn last_quote(&self, side: Side) -> Option<u64> {
        self.last_quotes[usize::from(side == Side::Sell)]
    }

    pub fn actions_in_last_minute(&self) -> usize {
        self.lighter_actions.len()
    }

    pub fn sent(&mut self, intent: &Intent, role: OrderRole) {
        let now = self.now_ms;
        if intent.venue == Venue::Lighter {
            self.lighter_actions.push_back(now);
        }
        self.venue_mut(intent.venue).changed_ms = Some(now);
        match intent.action {
            Action::Place {
                side,
                kind,
                price,
                size,
            } => {
                self.active.insert(intent.client);
                if role == OrderRole::Quote {
                    self.last_quotes[usize::from(side == Side::Sell)] = Some(now);
                }
                self.orders.insert(
                    intent.client,
                    Order {
                        venue: intent.venue,
                        generation: self
                            .venue(intent.venue)
                            .generation
                            .expect("connected order venue"),
                        oid: None,
                        side,
                        kind,
                        role,
                        price,
                        size,
                        filled: Decimal::ZERO,
                        reported: Decimal::ZERO,
                        sent_ms: now,
                        cancel_ms: None,
                        status: None,
                    },
                );
                if role == OrderRole::Hedge {
                    match kind {
                        Kind::Ioc => self.last_hedge_ms = Some(now),
                        Kind::PostOnly => self.passive_attempted_for = self.residual_since_ms,
                    }
                }
            }
            Action::Cancel { .. } => {
                self.orders
                    .get_mut(&intent.client)
                    .expect("known cancel")
                    .cancel_ms = Some(now);
            }
        }
    }
}

impl Instrument {
    fn valid(&self, venue: Venue) -> bool {
        self.size_decimals <= 8
            && self.price_decimals <= 8
            && (venue != Venue::Hyperliquid || self.size_decimals + self.price_decimals == 6)
            && self.min_size > Decimal::ZERO
            && self.min_notional > Decimal::ZERO
            && self.maker_fee.abs() < Decimal::ONE
            && self.taker_fee.abs() < Decimal::ONE
    }

    pub fn size_down(&self, size: Decimal) -> Decimal {
        size.round_dp_with_strategy(self.size_decimals, rust_decimal::RoundingStrategy::ToZero)
    }

    pub fn price_tick(&self, venue: Venue, price: Decimal) -> Decimal {
        let decimals = if venue == Venue::Hyperliquid {
            // Non-integer perpetual prices are limited to 5 significant figures.
            let whole = price.trunc().normalize().mantissa().unsigned_abs();
            if whole > 0 {
                self.price_decimals
                    .min(5_u32.saturating_sub(whole.ilog10() + 1))
            } else {
                let mut scaled = price;
                let mut leading = 0;
                while scaled < Decimal::ONE && !scaled.is_zero() && leading < self.price_decimals {
                    scaled *= Decimal::TEN;
                    leading += 1;
                }
                self.price_decimals.min(4 + leading)
            }
        } else {
            self.price_decimals
        };
        Decimal::new(1, decimals)
    }

    pub fn valid_size(&self, price: Decimal, size: Decimal) -> bool {
        size >= self.min_size && price * size >= self.min_notional
    }
}

impl Book {
    pub(crate) fn sequence(&self) -> Option<u64> {
        self.sequence
    }

    pub fn has_snapshot(&self) -> bool {
        self.sequence.is_some()
    }

    pub fn touch(&self) -> Option<(Decimal, Decimal)> {
        let (&bid, _) = self.bids.last_key_value()?;
        let (&ask, _) = self.asks.first_key_value()?;
        (bid < ask).then_some((bid, ask))
    }

    pub(crate) fn update(
        &mut self,
        now: u64,
        sequence: u64,
        previous: Option<u64>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    ) -> Result<()> {
        if previous.is_none() && self.sequence.is_some_and(|last| sequence < last) {
            return Ok(());
        }
        if let Some(previous) = previous {
            if self.sequence.is_some_and(|last| sequence <= last) {
                return Ok(());
            }
            if self.sequence != Some(previous) || sequence <= previous {
                *self = Self::default();
                return Ok(());
            }
        } else {
            *self = Self::default();
        }
        for (side, levels) in [(&mut self.bids, bids), (&mut self.asks, asks)] {
            for Level { price, size } in levels {
                ensure!(
                    price > Decimal::ZERO
                        && size >= Decimal::ZERO
                        && price <= Decimal::from(1_000_000_000_000_u64)
                        && size <= Decimal::from(1_000_000_000_000_u64),
                    "invalid book level"
                );
                if size.is_zero() {
                    side.remove(&price);
                } else {
                    side.insert(price, size);
                }
            }
        }
        self.sequence = Some(sequence);
        self.updated_ms = Some(now);
        Ok(())
    }

    pub fn sweep(
        &self,
        side: Side,
        size: Decimal,
        limit: Option<Decimal>,
    ) -> (Decimal, Decimal, Decimal) {
        let mut left = size;
        let mut quote = Decimal::ZERO;
        let mut worst = Decimal::ZERO;
        let mut take = |(&price, &available): (&Decimal, &Decimal)| {
            if left.is_zero()
                || limit.is_some_and(|limit| match side {
                    Side::Buy => price > limit,
                    Side::Sell => price < limit,
                })
            {
                return false;
            }
            let quantity = left.min(available);
            left -= quantity;
            quote += quantity * price;
            worst = price;
            true
        };
        match side {
            Side::Buy => {
                self.asks.iter().all(&mut take);
            }
            Side::Sell => {
                self.bids.iter().rev().all(&mut take);
            }
        }
        (size - left, quote, worst)
    }
}
