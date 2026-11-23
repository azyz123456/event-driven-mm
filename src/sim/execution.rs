use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use rust_decimal::Decimal;

use crate::intent::Side;
use crate::maker::state::Book;
use crate::maker::{Action, Fill, Input, Instrument, Intent, Kind, Status, TradePrint, Venue};

#[derive(Default)]
pub struct Exchange {
    generation: Option<u64>,
    pub book: Book,
    pub meta: Option<Instrument>,
    resting: BTreeMap<u64, Resting>,
    // Indexed by the side of the posted liquidity, net of simulated IOC consumption.
    consumed: Depth,
    printed: Depth,
    next_fill: u64,
}

type Depth = [BTreeMap<Decimal, Decimal>; 2];

struct Resting {
    side: Side,
    price: Decimal,
    remaining: Decimal,
    filled: Decimal,
    ahead: Decimal,
    arrived_us: u64,
}

impl Exchange {
    pub fn market(&mut self, at_us: u64, input: &Input, cancel_ahead: Decimal) -> Result<()> {
        match input {
            Input::Connected { generation, .. } => {
                self.generation = Some(*generation);
                self.reset_book();
            }
            Input::Disconnected { .. } => {
                self.generation = None;
                self.reset_book();
            }
            Input::Instrument { instrument, .. } => self.meta = Some(instrument.clone()),
            Input::Book {
                sequence,
                previous,
                bids,
                asks,
                ..
            } => {
                let old_sequence = self.book.sequence();
                let prior = self
                    .resting
                    .iter()
                    .map(|(&client, order)| (client, self.visible(order.side, order.price)))
                    .collect::<Vec<_>>();
                self.book.update(
                    at_us / 1000,
                    *sequence,
                    *previous,
                    bids.clone(),
                    asks.clone(),
                )?;
                if self.book.sequence() == old_sequence {
                    return Ok(());
                }
                if !self.book.has_snapshot() {
                    self.reset_book();
                    return Ok(());
                }

                if previous.is_none() || old_sequence.is_none() {
                    self.consumed = Depth::default();
                } else {
                    for (side, levels) in [(Side::Buy, bids), (Side::Sell, asks)] {
                        for level in levels {
                            self.consumed[index(side)].remove(&level.price);
                        }
                    }
                }

                for (client, old) in prior {
                    let order = &self.resting[&client];
                    let current = self.visible(order.side, order.price);
                    let changed = previous.is_none()
                        || old_sequence.is_none()
                        || current != old
                        || match order.side {
                            Side::Buy => bids,
                            Side::Sell => asks,
                        }
                        .iter()
                        .any(|level| level.price == order.price);
                    if changed {
                        let printed = self.printed[index(order.side)]
                            .remove(&order.price)
                            .unwrap_or_default();
                        let order = self.resting.get_mut(&client).expect("resting order");
                        if old_sequence.is_none() {
                            order.ahead = current;
                        } else {
                            let unexplained = (old - current - printed).max(Decimal::ZERO);
                            order.ahead =
                                (order.ahead - unexplained * cancel_ahead).max(Decimal::ZERO);
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn reset_book(&mut self) {
        self.book = Book::default();
        self.consumed = Depth::default();
        self.printed = Depth::default();
    }

    pub fn generation(&self) -> Option<u64> {
        self.generation
    }

    pub fn open_orders(&self) -> Vec<(Option<u64>, u64)> {
        self.resting
            .keys()
            .map(|&client| (Some(client), client))
            .collect()
    }

    pub fn fresh(&self, at_us: u64, stale_ms: u64) -> bool {
        self.generation.is_some()
            && self.book.touch().is_some()
            && self
                .book
                .updated_ms
                .is_some_and(|at| at_us / 1000 - at <= stale_ms)
    }

    pub fn arrival(
        &mut self,
        at_us: u64,
        generation: u64,
        intent: &Intent,
        stale_ms: u64,
        extra_taker_rate: Decimal,
    ) -> Vec<Input> {
        if self.generation != Some(generation) {
            return match intent.action {
                Action::Place { .. } => vec![status(
                    intent.venue,
                    intent.client,
                    Status::Terminal,
                    Decimal::ZERO,
                )],
                Action::Cancel { .. } => vec![],
            };
        }

        match intent.action {
            Action::Cancel { .. } => {
                self.resting
                    .remove(&intent.client)
                    .map_or_else(Vec::new, |order| {
                        vec![status(
                            intent.venue,
                            intent.client,
                            Status::Terminal,
                            order.filled,
                        )]
                    })
            }
            Action::Place {
                side,
                kind,
                price,
                size,
            } => {
                let venue = intent.venue;
                let client = intent.client;
                let rejected = || vec![status(venue, client, Status::Terminal, Decimal::ZERO)];
                let Some(meta) = self.meta.as_ref() else {
                    return rejected();
                };
                if !self.fresh(at_us, stale_ms)
                    || !meta.valid_size(price, size)
                    || meta.size_down(size) != size
                {
                    return rejected();
                }
                if kind == Kind::PostOnly {
                    let (bid, ask) = self.book.touch().expect("fresh book");
                    if (side == Side::Buy && price >= ask) || (side == Side::Sell && price <= bid) {
                        return rejected();
                    }
                    self.printed[index(side)].remove(&price);
                    self.resting.insert(
                        client,
                        Resting {
                            side,
                            price,
                            remaining: size,
                            filled: Decimal::ZERO,
                            ahead: self.visible(side, price),
                            arrived_us: at_us,
                        },
                    );
                    vec![status(venue, client, Status::Open, Decimal::ZERO)]
                } else {
                    let liquidity_side = opposite(side);
                    let prices = match side {
                        Side::Buy => self
                            .book
                            .asks
                            .iter()
                            .take_while(|&(p, _)| *p <= price)
                            .collect::<Vec<_>>(),
                        Side::Sell => self
                            .book
                            .bids
                            .iter()
                            .rev()
                            .take_while(|&(p, _)| *p >= price)
                            .collect::<Vec<_>>(),
                    };
                    let mut remaining = size;
                    let mut events = Vec::new();
                    let rate = meta.taker_fee + extra_taker_rate;
                    for (&level, &displayed) in prices {
                        let consumed = self.consumed[index(liquidity_side)]
                            .entry(level)
                            .or_default();
                        let take = meta
                            .size_down(remaining.min((displayed - *consumed).max(Decimal::ZERO)));
                        if take.is_zero() {
                            continue;
                        }
                        *consumed += take;
                        remaining -= take;
                        events.push(fill(
                            &mut self.next_fill,
                            venue,
                            client,
                            at_us,
                            side,
                            Leg {
                                price: level,
                                size: take,
                                fee_rate: rate,
                                maker: false,
                            },
                        ));
                        if remaining.is_zero() {
                            break;
                        }
                    }
                    events.push(status(venue, client, Status::Terminal, size - remaining));
                    events
                }
            }
        }
    }

    pub fn trade_at(
        &mut self,
        venue: Venue,
        at_us: u64,
        occurrence_us: u64,
        trade: TradePrint,
        stale_ms: u64,
    ) -> Result<Vec<Input>> {
        ensure!(
            trade.price > Decimal::ZERO
                && trade.price <= Decimal::from(1_000_000_000_000_u64)
                && trade.size > Decimal::ZERO
                && trade.size <= Decimal::from(1_000_000_000_000_u64),
            "invalid public print"
        );
        if !self.fresh(at_us, stale_ms) {
            return Ok(vec![]);
        }
        let side = opposite(trade.aggressor);
        let Some(meta) = &self.meta else {
            return Ok(vec![]);
        };
        let mut remaining = trade.size;
        let mut events = Vec::new();
        let mut complete = Vec::new();
        let eligible = |price| match side {
            Side::Buy => price >= trade.price,
            Side::Sell => price <= trade.price,
        };
        let mut orders = self
            .resting
            .iter_mut()
            .filter(|(_, order)| {
                order.side == side && order.arrived_us < occurrence_us && eligible(order.price)
            })
            .collect::<Vec<_>>();
        orders.sort_by_key(|(client, order)| {
            (
                if side == Side::Buy {
                    -order.price
                } else {
                    order.price
                },
                order.arrived_us,
                **client,
            )
        });
        for (&client, order) in orders {
            if order.price != trade.price {
                // A worse-priced print went through the level. Its size still caps our fill.
                order.ahead = Decimal::ZERO;
            }
            let ahead = remaining.min(order.ahead);
            remaining -= ahead;
            order.ahead -= ahead;
            let take = meta.size_down(remaining.min(order.remaining));
            if take.is_zero() {
                break;
            }
            order.remaining -= take;
            order.filled += take;
            remaining -= take;
            events.push(fill(
                &mut self.next_fill,
                venue,
                client,
                at_us,
                side,
                Leg {
                    price: order.price,
                    size: take,
                    fee_rate: meta.maker_fee,
                    maker: true,
                },
            ));
            if order.remaining.is_zero() {
                events.push(status(venue, client, Status::Terminal, order.filled));
                complete.push(client);
            }
        }
        if self
            .resting
            .values()
            .any(|order| order.price == trade.price && order.side == side)
        {
            *self.printed[index(side)].entry(trade.price).or_default() += trade.size;
        }
        for client in complete {
            if let Some(order) = self.resting.remove(&client) {
                self.printed[index(order.side)].remove(&order.price);
            }
        }
        Ok(events)
    }

    fn visible(&self, side: Side, price: Decimal) -> Decimal {
        let levels = match side {
            Side::Buy => &self.book.bids,
            Side::Sell => &self.book.asks,
        };
        levels.get(&price).copied().unwrap_or_default()
    }
}

fn opposite(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

pub(super) fn index(side: Side) -> usize {
    usize::from(side == Side::Sell)
}

fn status(venue: Venue, client: u64, status: Status, filled: Decimal) -> Input {
    Input::Order {
        venue,
        client: Some(client),
        oid: Some(client),
        status,
        filled,
    }
}

struct Leg {
    price: Decimal,
    size: Decimal,
    fee_rate: Decimal,
    maker: bool,
}

fn fill(id: &mut u64, venue: Venue, client: u64, at_us: u64, side: Side, leg: Leg) -> Input {
    *id += 1;
    Input::Fill {
        venue,
        fill: Fill {
            id: format!("sim-{}", *id),
            client: Some(client),
            oid: client,
            side,
            maker: leg.maker,
            price: leg.price,
            size: leg.size,
            fee: leg.size * leg.price * leg.fee_rate,
            venue_ms: at_us / 1000,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maker::Level;

    const VENUE: Venue = Venue::Hyperliquid;
    const STALE: u64 = 100;

    fn d(value: i64, scale: u32) -> Decimal {
        Decimal::new(value, scale)
    }

    fn depth(sequence: u64, previous: Option<u64>, ask_size: Decimal) -> Input {
        Input::Book {
            venue: VENUE,
            sequence,
            previous,
            bids: vec![Level {
                price: d(99, 0),
                size: d(1, 0),
            }],
            asks: vec![
                Level {
                    price: d(101, 0),
                    size: ask_size,
                },
                Level {
                    price: d(102, 0),
                    size: d(1, 0),
                },
            ],
        }
    }

    fn exchange(ask_size: Decimal) -> Result<Exchange> {
        let mut exchange = Exchange::default();
        exchange.market(
            0,
            &Input::Connected {
                venue: VENUE,
                generation: 1,
            },
            Decimal::ZERO,
        )?;
        exchange.market(
            0,
            &Input::Instrument {
                venue: VENUE,
                instrument: Instrument {
                    asset: 0,
                    size_decimals: 2,
                    price_decimals: 4,
                    min_size: d(1, 2),
                    min_notional: d(1, 0),
                    maker_fee: d(1, 4),
                    taker_fee: d(4, 4),
                },
            },
            Decimal::ZERO,
        )?;
        exchange.market(0, &depth(1, None, ask_size), Decimal::ZERO)?;
        Ok(exchange)
    }

    fn order(client: u64, side: Side, kind: Kind, size: Decimal) -> Intent {
        Intent {
            id: client,
            client,
            venue: VENUE,
            caused_by_seq: 0,
            ordinal: 0,
            action: Action::Place {
                side,
                kind,
                price: d(101, 0),
                size,
            },
        }
    }

    #[test]
    fn ioc_respects_price_limit_and_consumed_depth_until_next_snapshot() -> Result<()> {
        let mut exchange = exchange(d(20, 2))?;
        let make = |client| order(client, Side::Buy, Kind::Ioc, d(30, 2));
        let first = exchange.arrival(3_000, 1, &make(1), STALE, Decimal::ZERO);
        assert!(
            matches!(&first[..], [Input::Fill { fill, .. }, Input::Order { filled, status: Status::Terminal, .. }]
            if fill.price == d(101,0) && fill.size == d(20, 2) && *filled == fill.size && fill.fee == d(808, 5))
        );
        let same = exchange.arrival(4_000, 1, &make(2), STALE, Decimal::ZERO);
        assert!(
            matches!(&same[..], [Input::Order { filled, status: Status::Terminal, .. }] if filled.is_zero())
        );

        exchange.market(5_000, &depth(2, None, d(20, 2)), Decimal::ZERO)?;
        let replenished = exchange.arrival(6_000, 1, &make(3), STALE, Decimal::ZERO);
        assert!(matches!(
            &replenished[..],
            [Input::Fill { .. }, Input::Order { .. }]
        ));
        Ok(())
    }

    #[test]
    fn passive_order_joins_queue_and_cancel_preserves_racing_fill() -> Result<()> {
        let mut exchange = exchange(d(1, 0))?;
        let ask = order(1, Side::Sell, Kind::PostOnly, d(30, 2));
        let first = exchange.arrival(12_000, 1, &ask, STALE, Decimal::ZERO);
        assert!(matches!(
            &first[..],
            [Input::Order {
                status: Status::Open,
                ..
            }]
        ));
        let at_ask = |size| TradePrint {
            price: d(101, 0),
            size,
            aggressor: Side::Buy,
        };
        assert!(
            exchange
                .trade_at(VENUE, 12_000, 12_000, at_ask(d(1, 0)), STALE)?
                .is_empty()
        );
        assert!(
            exchange
                .trade_at(VENUE, 13_000, 13_000, at_ask(d(50, 2)), STALE)?
                .is_empty()
        );
        let next = exchange.trade_at(VENUE, 14_000, 14_000, at_ask(d(60, 2)), STALE)?;
        assert!(
            matches!(&next[..], [Input::Fill { fill, .. }] if fill.size == d(10, 2) && fill.maker)
        );
        let mut cancel = ask.clone();
        cancel.action = Action::Cancel { oid: Some(1) };
        let outcome = exchange.arrival(14_000, 1, &cancel, STALE, Decimal::ZERO);
        assert!(
            matches!(&outcome[..], [Input::Order { status: Status::Terminal, filled, .. }] if *filled == d(10, 2))
        );
        assert!(
            exchange
                .trade_at(VENUE, 15_000, 15_000, at_ask(d(2, 0)), STALE)?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn gaps_disable_matching_and_a_snapshot_rebuilds_queue_ahead() -> Result<()> {
        let mut exchange = exchange(d(1, 0))?;
        let ask = order(1, Side::Sell, Kind::PostOnly, d(30, 2));
        exchange.arrival(1_000, 1, &ask, STALE, Decimal::ZERO);
        exchange.market(2_000, &depth(4, Some(3), d(0, 0)), Decimal::ZERO)?;
        let print = TradePrint {
            price: d(101, 0),
            size: d(50, 2),
            aggressor: Side::Buy,
        };
        assert!(
            exchange
                .trade_at(VENUE, 3_000, 3_000, print, STALE)?
                .is_empty()
        );
        exchange.market(4_000, &depth(5, None, d(2, 0)), Decimal::ZERO)?;
        assert!(
            exchange
                .trade_at(VENUE, 5_000, 5_000, print, STALE)?
                .is_empty()
        );
        Ok(())
    }
    #[test]
    fn delayed_print_cannot_fill_an_order_arriving_after_the_trade() -> Result<()> {
        let mut exchange = exchange(d(1, 0))?;
        let ask = order(1, Side::Sell, Kind::PostOnly, d(30, 2));
        exchange.arrival(10_000, 1, &ask, STALE, Decimal::ZERO);
        let print = TradePrint {
            price: d(101, 0),
            size: d(2, 0),
            aggressor: Side::Buy,
        };
        assert!(
            exchange
                .trade_at(VENUE, 12_000, 9_000, print, STALE)?
                .is_empty()
        );
        assert!(matches!(
            &exchange.trade_at(VENUE, 13_000, 11_000, print, STALE)?[..],
            [Input::Fill { .. }, Input::Order { .. }]
        ));
        Ok(())
    }
}
