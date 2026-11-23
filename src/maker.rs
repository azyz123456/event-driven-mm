pub mod diagnostics;
mod execution;
mod hedge;
pub mod state;
mod strategy;
#[cfg(test)]
pub(crate) mod tests;

use anyhow::{Result, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use self::state::State;
use crate::intent::Side;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    Hyperliquid,
    Lighter,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Hyperliquid,
    Lighter,
    Hedged,
}

impl Mode {
    pub fn maker(self) -> Venue {
        match self {
            Self::Hyperliquid => Venue::Hyperliquid,
            Self::Lighter | Self::Hedged => Venue::Lighter,
        }
    }

    pub fn uses(self, venue: Venue) -> bool {
        self == Self::Hedged || self.maker() == venue
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HedgePolicy {
    #[default]
    Ioc,
    PassiveThenIoc {
        wait_ms: u64,
    },
}

impl HedgePolicy {
    fn is_default(&self) -> bool {
        *self == Self::Ioc
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderRole {
    Quote,
    Hedge,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub mode: Mode,
    #[serde(default, skip_serializing_if = "HedgePolicy::is_default")]
    pub hedge_policy: HedgePolicy,
    pub coin: String,
    pub lighter_market: Option<u16>,
    pub size: Decimal,
    pub max_inventory: Decimal,
    pub max_order_notional: Decimal,
    pub min_available_collateral: Decimal,
    pub half_spread_bps: Decimal,
    pub min_edge_bps: Decimal,
    pub slippage_bps: Decimal,
    pub max_residual: Decimal,
    pub max_residual_ms: u64,
    pub book_stale_ms: u64,
    pub account_stale_ms: u64,
    pub min_quote_ms: u64,
    pub reprice_ticks: u32,
    pub hedge_retry_ms: u64,
    pub action_timeout_ms: u64,
}

impl Config {
    pub fn role(&self, venue: Venue) -> OrderRole {
        if self.mode == Mode::Hedged && venue == Venue::Hyperliquid {
            OrderRole::Hedge
        } else {
            OrderRole::Quote
        }
    }

    pub fn requires_hyperliquid_protection(&self) -> bool {
        self.mode == Mode::Hyperliquid
            || matches!(self.hedge_policy, HedgePolicy::PassiveThenIoc { .. })
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.coin.is_empty()
                && self.coin.len() <= 20
                && self
                    .coin
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()),
            "configure a default-dex perpetual symbol in uppercase ASCII"
        );
        ensure!(
            self.lighter_market.is_some() == self.mode.uses(Venue::Lighter),
            "configure lighter_market for Lighter modes"
        );
        ensure!(
            self.size > Decimal::ZERO
                && self.size <= self.max_inventory
                && self.max_inventory <= Decimal::from(1_000_000)
                && self.max_order_notional > Decimal::ZERO
                && self.min_available_collateral > Decimal::ZERO,
            "invalid order, inventory, or collateral limits"
        );
        let max_bps = Decimal::from(1_000);
        ensure!(
            self.half_spread_bps > Decimal::ZERO
                && self.half_spread_bps <= max_bps
                && (Decimal::ZERO..=max_bps).contains(&self.min_edge_bps)
                && (Decimal::ZERO..=max_bps).contains(&self.slippage_bps),
            "invalid spread, edge, or slippage in basis points"
        );
        ensure!(
            self.max_residual >= self.size
                && self.max_residual <= self.max_inventory
                && self.max_residual_ms > 0
                && self.book_stale_ms > 0
                && self.account_stale_ms > 0
                && self.reprice_ticks > 0
                && self.hedge_retry_ms > 0
                && self.action_timeout_ms > 0,
            "invalid exposure, freshness, or order timing limits"
        );
        if let HedgePolicy::PassiveThenIoc { wait_ms } = self.hedge_policy {
            ensure!(
                self.mode == Mode::Hedged && wait_ms > 0 && wait_ms <= self.max_residual_ms,
                "passive hedging requires hedged mode and a positive wait within max_residual_ms"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Instrument {
    pub asset: u32,
    pub size_decimals: u32,
    pub price_decimals: u32,
    pub min_size: Decimal,
    pub min_notional: Decimal,
    pub maker_fee: Decimal,
    pub taker_fee: Decimal,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Level {
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Fill {
    pub id: String,
    pub client: Option<u64>,
    pub oid: u64,
    pub side: Side,
    pub maker: bool,
    pub price: Decimal,
    pub size: Decimal,
    pub fee: Decimal,
    pub venue_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct TradePrint {
    pub price: Decimal,
    pub size: Decimal,
    pub aggressor: Side,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Open,
    Terminal,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Input {
    Connected {
        venue: Venue,
        generation: u64,
    },
    Disconnected {
        venue: Venue,
        reason: String,
    },
    Instrument {
        venue: Venue,
        instrument: Instrument,
    },
    Book {
        venue: Venue,
        sequence: u64,
        previous: Option<u64>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Account {
        venue: Venue,
        position: Decimal,
        available: Decimal,
        open: Vec<(Option<u64>, u64)>,
        // The account query was issued at this session elapsed time.
        requested_ms: u64,
    },
    Fill {
        venue: Venue,
        fill: Fill,
    },
    Order {
        venue: Venue,
        client: Option<u64>,
        oid: Option<u64>,
        status: Status,
        filled: Decimal,
    },
    Protection {
        expires_ms: Option<u64>,
    },
    PublicBook {
        venue: Venue,
        sequence: u64,
        connection: u64,
    },
    PublicTrade {
        venue: Venue,
        id: String,
        connection: u64,
        venue_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trade: Option<TradePrint>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        is_snapshot: bool,
    },
    Tick,
    Stop,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    PostOnly,
    Ioc,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Intent {
    pub id: u64,
    pub caused_by_seq: u64,
    pub ordinal: u8,
    pub venue: Venue,
    pub client: u64,
    #[serde(flatten)]
    pub action: Action,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    Place {
        side: Side,
        kind: Kind,
        price: Decimal,
        size: Decimal,
    },
    Cancel {
        oid: Option<u64>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct End {
    pub clean: bool,
    pub positions: [Decimal; 2],
    pub live_orders: Vec<(Venue, u64)>,
}

pub struct Engine {
    pub config: Config,
    pub state: State,
    pub seed: u64,
    pub observe: bool,
    next_id: u64,
    fixed_lighter_fills: bool,
    quote_filter: Option<Side>,
    pub quote_opportunities: [Option<(Decimal, Decimal)>; 2],
}

impl Engine {
    pub fn new(config: Config, seed: u64, observe: bool) -> Result<Self> {
        config.validate()?;
        ensure!(seed > 0 && seed < (1 << 26), "invalid maker session seed");
        Ok(Self {
            config,
            state: State::default(),
            seed,
            observe,
            next_id: 1,
            fixed_lighter_fills: false,
            quote_filter: None,
            quote_opportunities: [None; 2],
        })
    }

    pub fn with_fixed_lighter_fills(mut self) -> Self {
        self.fixed_lighter_fills = true;
        self
    }

    pub fn suppress_quote(&mut self, side: Option<Side>) {
        self.quote_filter = side;
    }

    pub fn apply(&mut self, seq: u64, at_ms: u64, input: Input) -> Result<Vec<Intent>> {
        self.quote_opportunities = [None; 2];
        let is_diagnostic = matches!(input, Input::PublicTrade { .. } | Input::PublicBook { .. });
        self.state
            .apply(&self.config, at_ms, input, self.fixed_lighter_fills)?;
        if self.observe || is_diagnostic {
            return Ok(vec![]);
        }

        let mut intents = Vec::new();
        if let Some((client, action)) = hedge::desired(&self.config, &self.state) {
            self.emit(seq, Venue::Hyperliquid, client, action, &mut intents)?;
        }
        if self.fixed_lighter_fills {
            return Ok(intents);
        }
        let mut quotes = strategy::desired(&self.config, &self.state);
        let venue = self.config.mode.maker();
        let baseline = execution::reconcile(&self.config, &self.state, quotes);
        for (_, action) in &baseline {
            if let Action::Place {
                side, price, size, ..
            } = *action
            {
                self.quote_opportunities[usize::from(side == Side::Sell)] = Some((price, size));
            }
        }
        let actions = if let Some(side) = self.quote_filter {
            quotes[usize::from(side == Side::Sell)] = None;
            execution::reconcile(&self.config, &self.state, quotes)
        } else {
            baseline
        };
        for (client, action) in actions {
            self.emit(seq, venue, client, action, &mut intents)?;
        }
        Ok(intents)
    }

    fn emit(
        &mut self,
        seq: u64,
        venue: Venue,
        client: Option<u64>,
        action: Action,
        intents: &mut Vec<Intent>,
    ) -> Result<()> {
        // Lighter accepts 48-bit client order indexes.
        ensure!(self.next_id < (1 << 22), "maker action ID space exhausted");
        let id = self.next_id;
        let client = client.unwrap_or((self.seed << 22) | id);
        let intent = Intent {
            id,
            caused_by_seq: seq,
            ordinal: intents.len().try_into()?,
            venue,
            client,
            action,
        };
        self.state.sent(&intent, self.config.role(venue));
        intents.push(intent);
        self.next_id += 1;
        Ok(())
    }

    pub fn end(&self) -> End {
        let positions =
            [Venue::Hyperliquid, Venue::Lighter].map(|venue| self.state.venue(venue).position);
        let live_orders = self
            .state
            .active_orders()
            .map(|(client, order)| (order.venue, client))
            .collect::<Vec<_>>();
        let observed = self.observe
            && [Venue::Hyperliquid, Venue::Lighter]
                .into_iter()
                .filter(|&venue| self.config.mode.uses(venue))
                .all(|venue| {
                    self.state.venue(venue).saw_book && self.state.venue(venue).instrument.is_some()
                });
        let clean = observed
            || (!self.observe
                && (live_orders.is_empty()
                    && [Venue::Hyperliquid, Venue::Lighter]
                        .into_iter()
                        .filter(|&venue| self.config.mode.uses(venue))
                        .all(|venue| {
                            self.state.account_verified(venue, &self.config)
                                && self
                                    .state
                                    .stop_ms
                                    .zip(self.state.venue(venue).account_requested_ms)
                                    .is_some_and(|(stop, requested)| requested >= stop)
                        })
                    && (self.config.mode != Mode::Hedged || self.state.residual().is_zero())));
        End {
            clean,
            positions,
            live_orders,
        }
    }
}
