use super::state::State;
use super::strategy::{Quote, SIDES};
use super::{Action, Config, Kind, Status, Venue};

pub fn reconcile(
    config: &Config,
    state: &State,
    desired: [Option<Quote>; 2],
) -> Vec<(Option<u64>, Action)> {
    let venue = config.mode.maker();
    let maker = state.venue(venue);
    let Some(generation) = maker.generation else {
        return vec![];
    };
    let Some(instrument) = maker.instrument.as_ref() else {
        return vec![];
    };
    let mut cancels = Vec::new();
    let mut places = Vec::new();

    for (side, quote) in SIDES.into_iter().zip(desired) {
        let active = state
            .active_orders()
            .find(|(_, order)| order.venue == venue && order.side == side);
        match (active, quote) {
            (None, Some(Quote { price, size, .. }))
                if state
                    .last_quote(side)
                    .is_none_or(|at| state.now_ms - at >= config.min_quote_ms.max(250)) =>
            {
                places.push((
                    None,
                    Action::Place {
                        side,
                        kind: Kind::PostOnly,
                        price,
                        size,
                    },
                ))
            }
            (Some((client, order)), quote) if order.status != Some(Status::Terminal) => {
                let timed_out = state.now_ms - order.sent_ms >= config.action_timeout_ms;
                let unsafe_quote = quote.is_none_or(|quote| {
                    side.signed(order.price - quote.guard_price) > rust_decimal::Decimal::ZERO
                        || order.outstanding() > quote.size
                });
                let move_quote = quote.is_some_and(|quote| {
                    (quote.price - order.price).abs()
                        >= instrument.price_tick(venue, order.price)
                            * rust_decimal::Decimal::from(config.reprice_ticks)
                        && state.now_ms - order.sent_ms >= config.min_quote_ms
                });
                let replace = order.status == Some(Status::Open) && move_quote;
                let ambiguous = order.status.is_none() && timed_out;
                let eligible = order
                    .cancel_ms
                    .is_none_or(|at| state.now_ms - at >= config.action_timeout_ms);
                if eligible
                    && (unsafe_quote || replace || ambiguous || generation != order.generation)
                {
                    cancels.push((Some(client), Action::Cancel { oid: order.oid }));
                }
            }
            _ => {}
        }
    }
    if venue == Venue::Lighter {
        let used = state.actions_in_last_minute();
        cancels.truncate(30_usize.saturating_sub(used));
        places.truncate(24_usize.saturating_sub(used + cancels.len()));
    }
    cancels.extend(places);
    cancels
}
