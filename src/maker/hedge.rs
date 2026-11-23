use rust_decimal::Decimal;

use super::state::State;
use super::strategy::{bps, round_price};
use super::{Action, Config, Kind, Mode, Venue};
use crate::intent::Side;

pub fn desired(config: &Config, state: &State) -> Option<Action> {
    let venue = Venue::Hyperliquid;
    if config.mode != Mode::Hedged
        || !state.book_fresh(venue, config)
        || !state.account_verified(venue, config)
        || state
            .active_orders()
            .any(|(_, order)| order.kind == Kind::Ioc && !order.done())
        || state
            .last_hedge_ms
            .is_some_and(|at| state.now_ms - at < config.hedge_retry_ms)
    {
        return None;
    }
    let residual = state.residual();
    let side = if residual > Decimal::ZERO {
        Side::Sell
    } else {
        Side::Buy
    };
    let hedge = state.venue(venue);
    let instrument = hedge.instrument.as_ref()?;
    let (bid, ask) = hedge.book.touch()?;
    let slippage = bps(config.slippage_bps);
    let raw_limit = match side {
        Side::Buy => ask * (Decimal::ONE + slippage),
        Side::Sell => bid * (Decimal::ONE - slippage),
    };
    // Round towards the touch so the price stays within the configured bound.
    let price = round_price(instrument, venue, side, raw_limit);
    if price <= Decimal::ZERO {
        return None;
    }
    let room = config.max_inventory - side.signed(hedge.position);
    let target = residual
        .abs()
        .min(room)
        .min(config.max_order_notional / price);
    if target < instrument.min_size {
        return None;
    }
    let (available, _, _) = hedge.book.sweep(side, target, Some(price));
    let size = instrument.size_down(available);
    instrument.valid_size(price, size).then_some(Action::Place {
        side,
        kind: Kind::Ioc,
        price,
        size,
    })
}
