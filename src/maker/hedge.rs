use rust_decimal::Decimal;

use super::state::State;
use super::strategy::{bps, round_price};
use super::{Action, Config, HedgePolicy, Kind, Mode, OrderRole, Status, Venue};
use crate::intent::Side;

pub fn desired(config: &Config, state: &State) -> Option<(Option<u64>, Action)> {
    let venue = Venue::Hyperliquid;
    let hedge = state.venue(venue);
    let generation = hedge.generation?;
    if config.mode != Mode::Hedged {
        return None;
    }
    let residual = state.residual();
    let side = if residual > Decimal::ZERO {
        Side::Sell
    } else {
        Side::Buy
    };
    let healthy = state.book_fresh(venue, config) && state.account_verified(venue, config);
    let protection = state
        .protection_ms
        .is_some_and(|expires| expires.saturating_sub(state.now_ms) > 5_000);
    let urgent = state.stop_ms.is_some()
        || residual.abs() >= config.max_residual
        || state
            .residual_since_ms
            .is_some_and(|since| state.now_ms - since >= config.max_residual_ms);
    if let Some((client, order)) = state
        .active_orders()
        .find(|(_, order)| order.role == OrderRole::Hedge)
    {
        if let HedgePolicy::PassiveThenIoc { wait_ms } = config.hedge_policy
            && order.kind == Kind::PostOnly
            && order.status != Some(Status::Terminal)
            && order
                .cancel_ms
                .is_none_or(|at| state.now_ms - at >= config.action_timeout_ms)
            && (urgent
                || !healthy
                || !protection
                || order.generation != generation
                || order.side != side
                || order.outstanding() > residual.abs()
                || state.now_ms - order.sent_ms >= wait_ms)
        {
            return Some((Some(client), Action::Cancel { oid: order.oid }));
        }
        return None;
    }
    if !healthy
        || residual.is_zero()
        || state
            .last_hedge_ms
            .is_some_and(|at| state.now_ms - at < config.hedge_retry_ms)
    {
        return None;
    }

    let instrument = hedge.instrument.as_ref()?;
    let (bid, ask) = hedge.book.touch()?;
    let passive = matches!(config.hedge_policy, HedgePolicy::PassiveThenIoc { .. })
        && !urgent
        && protection
        && state.passive_attempted_for != state.residual_since_ms;
    let slippage = bps(config.slippage_bps);
    let price = if passive {
        match side {
            Side::Buy => bid,
            Side::Sell => ask,
        }
    } else {
        let raw_limit = match side {
            Side::Buy => ask * (Decimal::ONE + slippage),
            Side::Sell => bid * (Decimal::ONE - slippage),
        };
        // Round towards the touch so the price stays within the slippage bound.
        round_price(instrument, venue, side, raw_limit)
    };
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
    let available = if passive {
        target
    } else {
        hedge.book.sweep(side, target, Some(price)).0
    };
    let size = instrument.size_down(available);
    instrument.valid_size(price, size).then_some((
        None,
        Action::Place {
            side,
            kind: if passive { Kind::PostOnly } else { Kind::Ioc },
            price,
            size,
        },
    ))
}
