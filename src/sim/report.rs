use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::Serialize;

use crate::maker::state::State;
use crate::maker::{Engine, Kind, OrderRole, Venue};

#[derive(Default, Serialize)]
pub struct LatencySummary {
    samples: usize,
    p50_us: Option<u64>,
    p95_us: Option<u64>,
    p99_us: Option<u64>,
}

impl LatencySummary {
    pub fn of(mut samples: Vec<u64>) -> Self {
        samples.sort_unstable();
        let percentile = |n: usize| {
            samples
                .get((samples.len() * n).div_ceil(100).saturating_sub(1))
                .copied()
        };
        Self {
            samples: samples.len(),
            p50_us: percentile(50),
            p95_us: percentile(95),
            p99_us: percentile(99),
        }
    }
}

#[derive(Clone, Copy)]
pub enum MarketEvent {
    Book,
    Trade,
}

#[derive(Default)]
pub struct ProcessingTimes {
    book_ns: Vec<u64>,
    trade_ns: Vec<u64>,
}

#[derive(Serialize)]
pub struct ProcessingSummary {
    // Exact frequency tables allow pooling events without averaging per-day medians.
    books: BTreeMap<u64, u64>,
    trades: BTreeMap<u64, u64>,
}

impl ProcessingTimes {
    pub fn record(&mut self, kind: MarketEvent, ns: u64) {
        match kind {
            MarketEvent::Book => self.book_ns.push(ns),
            MarketEvent::Trade => self.trade_ns.push(ns),
        }
    }

    pub fn summarize(self) -> ProcessingSummary {
        fn histogram(values: Vec<u64>) -> BTreeMap<u64, u64> {
            let mut counts = BTreeMap::new();
            for value in values {
                *counts.entry(value).or_insert(0) += 1;
            }
            counts
        }
        ProcessingSummary {
            books: histogram(self.book_ns),
            trades: histogram(self.trade_ns),
        }
    }
}

#[derive(Default, Serialize)]
pub struct Outcomes {
    placements: usize,
    empty: usize,
    partial: usize,
    full: usize,
    unresolved: usize,
    filled_orders: usize,
    requested_base: Decimal,
    filled_base: Decimal,
    fill_fraction: Option<Decimal>,
}

impl Outcomes {
    fn from_orders(state: &State, role: OrderRole, kind: Kind) -> Self {
        let mut counts = Self::default();
        for order in state
            .orders
            .values()
            .filter(|order| order.role == role && order.kind == kind)
        {
            counts.placements += 1;
            counts.filled_orders += usize::from(!order.filled.is_zero());
            counts.requested_base += order.size;
            counts.filled_base += order.filled;
            if !order.done() {
                counts.unresolved += 1;
            } else if order.filled.is_zero() {
                counts.empty += 1;
            } else if order.filled < order.size {
                counts.partial += 1;
            } else {
                counts.full += 1;
            }
        }
        counts.fill_fraction =
            (!counts.requested_base.is_zero()).then(|| counts.filled_base / counts.requested_base);
        counts
    }
}

#[derive(Default)]
struct AbsoluteInventory {
    since: u64,
    quantity: Decimal,
    area: Decimal,
}

impl AbsoluteInventory {
    fn observe(&mut self, at: u64, quantity: Decimal) {
        if self.quantity != quantity {
            self.area = self.area_at(at);
            self.since = at;
            self.quantity = quantity;
        }
    }

    fn area_at(&self, at: u64) -> Decimal {
        self.area + self.quantity * Decimal::from(at - self.since)
    }
}

#[derive(Default)]
pub struct Exposure {
    residual: Decimal,
    since: Option<u64>,
    first_hedge: bool,
    at: u64,
    peak: Decimal,
    max_age_us: u64,
    exposed_us: u64,
    hedge_samples: Vec<u64>,
    maker_inventory: AbsoluteInventory,
    net_inventory: AbsoluteInventory,
    filled_notional: Decimal,
}

impl Exposure {
    pub fn observe(&mut self, now: u64, engine: &Engine, current_input: bool) {
        let positions = engine.state.residual();
        self.maker_inventory.observe(
            now,
            engine
                .state
                .venue(engine.config.mode.maker())
                .position
                .abs(),
        );
        self.net_inventory.observe(now, positions.abs());
        if current_input {
            self.filled_notional += engine
                .state
                .applied_fills
                .iter()
                .map(|fill| fill.fill.size * fill.fill.price)
                .sum::<Decimal>();
        }
        if let Some(since) = self.since {
            self.exposed_us += now - self.at;
            self.max_age_us = self.max_age_us.max(now - since);
            if current_input
                && !self.first_hedge
                && engine
                    .state
                    .applied_fills
                    .iter()
                    .any(|fill| engine.config.role(fill.venue) == OrderRole::Hedge)
            {
                self.hedge_samples.push(now - since);
                self.first_hedge = true;
            }
        }
        if positions.is_zero() {
            self.since = None;
        } else if self.residual.is_zero()
            || positions.is_sign_negative() != self.residual.is_sign_negative()
        {
            self.since = Some(now);
            self.first_hedge = false;
        }
        self.residual = positions;
        self.peak = self.peak.max(positions.abs());
        self.at = now;
    }
}

#[derive(Serialize)]
pub struct Metrics {
    pub venues: Vec<VenueLedger>,
    pub net_pnl: Option<Decimal>,
    pub net_pnl_per_hour: Option<Decimal>,
    pub open_orders: usize,
    pub hedge_ioc: Outcomes,
    pub hedge_passive: Outcomes,
    pub quotes: Outcomes,
    pub order_cancels: usize,
    pub max_residual: Decimal,
    pub end_residual: Decimal,
    pub max_residual_age_us: u64,
    pub exposed_us: u64,
    pub exposure_to_first_hedge_fill: LatencySummary,
    pub abs_maker_inventory_base_us: Decimal,
    pub abs_net_inventory_base_us: Decimal,
    pub filled_notional: Decimal,
}

#[derive(Serialize)]
pub struct VenueLedger {
    venue: Venue,
    fills: u64,
    position: Decimal,
    cash: Decimal,
    fees: Decimal,
    mark: Option<Decimal>,
    mark_age_us: Option<u64>,
    gross_pnl: Option<Decimal>,
    net_pnl: Option<Decimal>,
}

pub type Marks = BTreeMap<Venue, (u64, Decimal)>;

pub fn metrics(
    engine: &Engine,
    marks: &Marks,
    now: u64,
    mut exposure: Exposure,
    cancels: usize,
) -> Metrics {
    exposure.observe(now, engine, false);
    let venues = [Venue::Lighter, Venue::Hyperliquid]
        .into_iter()
        .map(|venue| {
            let state = engine.state.venue(venue);
            let mark = marks.get(&venue).copied();
            let gross_pnl = if state.position.is_zero() {
                Some(state.cash)
            } else {
                mark.map(|(_, price)| state.cash + state.position * price)
            };
            VenueLedger {
                venue,
                fills: state.fills,
                position: state.position,
                cash: state.cash,
                fees: state.fees,
                mark: mark.map(|(_, price)| price),
                mark_age_us: mark.map(|(at, _)| now.saturating_sub(at)),
                gross_pnl,
                net_pnl: gross_pnl.map(|pnl| pnl - state.fees),
            }
        })
        .collect::<Vec<_>>();
    let net_pnl = venues
        .iter()
        .map(|ledger| ledger.net_pnl)
        .sum::<Option<Decimal>>();
    Metrics {
        venues,
        net_pnl,
        net_pnl_per_hour: net_pnl
            .filter(|_| now > 0)
            .map(|pnl| pnl * Decimal::from(3_600_000_000_u64) / Decimal::from(now)),
        open_orders: engine.state.active_orders().count(),
        hedge_ioc: Outcomes::from_orders(&engine.state, OrderRole::Hedge, Kind::Ioc),
        hedge_passive: Outcomes::from_orders(&engine.state, OrderRole::Hedge, Kind::PostOnly),
        quotes: Outcomes::from_orders(&engine.state, OrderRole::Quote, Kind::PostOnly),
        order_cancels: cancels,
        max_residual: exposure.peak,
        end_residual: engine.state.residual(),
        max_residual_age_us: exposure.max_age_us,
        exposed_us: exposure.exposed_us,
        exposure_to_first_hedge_fill: LatencySummary::of(exposure.hedge_samples),
        abs_maker_inventory_base_us: exposure.maker_inventory.area_at(now),
        abs_net_inventory_base_us: exposure.net_inventory.area_at(now),
        filled_notional: exposure.filled_notional,
    }
}

#[cfg(test)]
mod tests {
    use super::AbsoluteInventory;
    use rust_decimal::Decimal;

    #[test]
    fn inventory_area_uses_the_previous_position_and_includes_the_final_interval() {
        let mut inventory = AbsoluteInventory::default();
        inventory.observe(100, Decimal::new(5, 4));
        inventory.observe(110, Decimal::new(5, 4));
        inventory.observe(120, Decimal::new(2, 4));
        inventory.observe(120, Decimal::new(1, 4));
        assert_eq!(inventory.area_at(180), Decimal::new(16, 3));
        inventory.observe(180, Decimal::ZERO);
        assert_eq!(inventory.area_at(200), Decimal::new(16, 3));
    }
}
