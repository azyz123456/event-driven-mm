use rust_decimal::{Decimal, RoundingStrategy};

use super::state::State;
use super::{Config, Instrument, Mode, Venue};
use crate::intent::Side;

pub const SIDES: [Side; 2] = [Side::Buy, Side::Sell];

#[derive(Clone, Copy)]
pub struct Quote {
    pub price: Decimal,
    pub size: Decimal,
    pub guard_price: Decimal,
}

pub fn desired(config: &Config, state: &State, inventory_skew: bool) -> [Option<Quote>; 2] {
    let venue = config.mode.maker();
    if state.stop_ms.is_some()
        || (config.mode == Mode::Hyperliquid
            && state
                .protection_ms
                .is_none_or(|expires| expires <= state.now_ms + 5_000))
        || !state.account_verified(venue, config)
        || !state.book_fresh(venue, config)
        || (config.mode == Mode::Hedged && !hedge_ready(config, state))
        || state.active_orders().any(|(_, order)| {
            order.venue == venue
                && !order.done()
                && Some(order.generation) != state.venue(venue).generation
        })
    {
        return [None; 2];
    }

    let maker = state.venue(venue);
    let Some(instrument) = maker.instrument.as_ref() else {
        return [None; 2];
    };
    let Some((bid, ask)) = maker.book.touch() else {
        return [None; 2];
    };
    let mid = (bid + ask) / Decimal::TWO;
    let quote_inventory = if inventory_skew {
        maker.position
    } else {
        Decimal::ZERO
    };

    let hedge = state.venue(Venue::Hyperliquid);
    let lot_decimals = hedge
        .instrument
        .as_ref()
        .filter(|_| config.mode == Mode::Hedged)
        .map_or(instrument.size_decimals, |hedge| {
            instrument.size_decimals.min(hedge.size_decimals)
        });
    let lot_down =
        |size: Decimal| size.round_dp_with_strategy(lot_decimals, RoundingStrategy::ToZero);
    let pending_hedge = |side| {
        state
            .active_orders()
            .filter(|(_, order)| order.role == super::OrderRole::Hedge && order.side == side)
            .map(|(_, order)| order.outstanding())
            .sum::<Decimal>()
    };

    let dust = (config.mode == Mode::Hedged)
        .then(|| dust_mode(config, state))
        .flatten();
    SIDES.map(|side| {
        let room = config.max_inventory - side.signed(maker.position);
        let mut size = config.size.min(room).min(config.max_order_notional / ask);
        if dust == Some(true) {
            let residual = state.residual();
            if side.signed(residual) >= Decimal::ZERO {
                return None;
            }
            size = size.min(residual.abs() * Decimal::TWO);
        }
        size = lot_down(size);
        if size < instrument.min_size {
            return None;
        }
        let post_only_bound = match side {
            Side::Buy => ask - instrument.price_tick(venue, ask),
            Side::Sell => bid + instrument.price_tick(venue, bid),
        };
        let mut guard = post_only_bound;
        let base_price = if config.mode == Mode::Hedged {
            let hedge_instrument = hedge.instrument.as_ref()?;
            let hedge_side = opposite(side);
            let (best_bid, best_ask) = hedge.book.touch()?;
            let hedge_room = config.max_inventory
                - hedge_side.signed(hedge.position)
                - pending_hedge(hedge_side);
            let net_room =
                config.max_residual - side.signed(state.residual()) - pending_hedge(side);
            size = lot_down(size.min(hedge_room).min(net_room));
            if size < instrument.min_size {
                return None;
            }
            let (available, _, _) = hedge.book.sweep(hedge_side, size, None);
            size = lot_down(size.min(available));
            if size < instrument.min_size {
                return None;
            }
            if !hedge_instrument.valid_size(best_bid.min(best_ask), size) {
                return None;
            }
            let (_, total, worst) = hedge.book.sweep(hedge_side, size, None);
            let vwap = total / size;
            let fee = hedge_instrument.taker_fee.max(Decimal::ZERO);
            let maker_fee = instrument.maker_fee.max(Decimal::ZERO);
            let edge = vwap * bps(config.min_edge_bps);
            let slippage = vwap * bps(config.slippage_bps);
            let hedge_limit = match side {
                Side::Buy => {
                    (vwap * (Decimal::ONE - fee) - edge - slippage) / (Decimal::ONE + maker_fee)
                }
                Side::Sell => {
                    (vwap * (Decimal::ONE + fee) + edge + slippage) / (Decimal::ONE - maker_fee)
                }
            };
            let max_slippage = bps(config.slippage_bps);
            let executable = match hedge_side {
                Side::Buy => worst <= best_ask * (Decimal::ONE + max_slippage),
                Side::Sell => worst >= best_bid * (Decimal::ONE - max_slippage),
            };
            if !executable || !hedge_instrument.valid_size(worst, size) {
                return None;
            }
            guard = select_bound(side, guard, hedge_limit);
            let flat = skewed(mid, side, Decimal::ZERO, config)?;
            let skew = skewed(mid, side, quote_inventory, config)? - flat;
            select_bound(side, hedge_limit, flat) + skew
        } else {
            skewed(mid, side, quote_inventory, config)?
        };
        let price = round_price(
            instrument,
            venue,
            side,
            select_bound(side, base_price, guard),
        );
        if price <= Decimal::ZERO {
            return None;
        }
        size = lot_down(size.min(config.max_order_notional / price));
        instrument.valid_size(price, size).then_some(Quote {
            price,
            size,
            guard_price: guard,
        })
    })
}

fn hedge_ready(config: &Config, state: &State) -> bool {
    state.account_verified(Venue::Hyperliquid, config)
        && state.book_fresh(Venue::Hyperliquid, config)
        && state.residual().abs() < config.max_residual
        && (state
            .residual_since_ms
            .is_none_or(|since| state.now_ms - since < config.max_residual_ms)
            || dust_mode(config, state).is_some())
        && !state.active_orders().any(|(_, order)| {
            order.role == super::OrderRole::Hedge
                && !order.done()
                && (state.now_ms - order.sent_ms >= config.action_timeout_ms
                    || Some(order.generation) != state.venue(Venue::Hyperliquid).generation)
        })
}

// Some(true) allows only quantity-bounded quotes opposite the residual. Some(false)
// permits aggregation within the ordinary per-side worst-case exposure limits.
fn dust_mode(config: &Config, state: &State) -> Option<bool> {
    let (max_base, max_age_ms, reduce) = config.residual_policy.limits()?;
    let age = state.now_ms - state.residual_since_ms?;
    let residual = state.residual();
    if age < config.max_residual_ms
        || age >= max_age_ms
        || residual.abs() > max_base
        || state
            .active_orders()
            .any(|(_, order)| order.role == super::OrderRole::Hedge)
    {
        return None;
    }
    let hedge = state.venue(Venue::Hyperliquid);
    let instrument = hedge.instrument.as_ref()?;
    let side = if residual > Decimal::ZERO {
        Side::Sell
    } else {
        Side::Buy
    };
    let limit = super::hedge::ioc_limit(config, instrument, &hedge.book, side)?;
    // Require a quantity/notional shortfall at both touch and limit. A wider sell
    // limit can fall below minimum even when the observed touch can hedge the lot.
    let (bid, ask) = hedge.book.touch()?;
    let touch = if side == Side::Sell { bid } else { ask };
    let size = instrument.size_down(residual.abs());
    (!instrument.valid_size(limit.max(touch), size)).then_some(reduce)
}

fn skewed(mid: Decimal, side: Side, position: Decimal, config: &Config) -> Option<Decimal> {
    let increasing = side.signed(position) > Decimal::ZERO;
    let x = (position.abs() / config.max_inventory).min(Decimal::ONE);
    let s = mid * bps(config.half_spread_bps);
    let distance = if increasing {
        if x >= Decimal::ONE {
            return None;
        }
        (s + s * x / (Decimal::ONE - x)).min(mid / Decimal::TWO)
    } else {
        s - s * x
    };
    Some(mid - side.signed(distance))
}

fn select_bound(side: Side, first: Decimal, second: Decimal) -> Decimal {
    match side {
        Side::Buy => first.min(second),
        Side::Sell => first.max(second),
    }
}

pub fn bps(value: Decimal) -> Decimal {
    value / Decimal::from(10_000)
}

pub fn opposite(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

pub fn round_price(instrument: &Instrument, venue: Venue, side: Side, price: Decimal) -> Decimal {
    if price <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let strategy = match side {
        Side::Buy => RoundingStrategy::ToNegativeInfinity,
        Side::Sell => RoundingStrategy::ToPositiveInfinity,
    };
    price.round_dp_with_strategy(instrument.price_tick(venue, price).scale(), strategy)
}
