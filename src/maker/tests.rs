use std::io::Write;

use rust_decimal::Decimal;

use super::*;
use crate::event::{Event, MAKER_FORMAT_VERSION};
use crate::intent::Side;
use crate::journal::JournalWriter;
use crate::queue::RecordSource;

fn d(value: i64, scale: u32) -> Decimal {
    Decimal::new(value, scale)
}

pub(crate) fn config(mode: Mode) -> Config {
    Config {
        mode,
        hedge_policy: HedgePolicy::Ioc,
        coin: "BTC".into(),
        lighter_market: mode.uses(Venue::Lighter).then_some(4096),
        size: d(30, 2),
        max_inventory: d(1, 0),
        max_order_notional: d(100, 0),
        min_available_collateral: d(20, 0),
        half_spread_bps: d(10, 0),
        min_edge_bps: d(5, 0),
        slippage_bps: d(10, 0),
        max_residual: d(5, 1),
        max_residual_ms: 5_000,
        residual_policy: ResidualPolicy::Strict,
        book_stale_ms: 5_000,
        account_stale_ms: 25_000,
        min_quote_ms: 300,
        reprice_ticks: 2,
        hedge_retry_ms: 200,
        action_timeout_ms: 3_000,
    }
}

fn instrument(venue: Venue) -> Input {
    Input::Instrument {
        venue,
        instrument: Instrument {
            asset: if venue == Venue::Lighter { 4096 } else { 0 },
            size_decimals: 2,
            price_decimals: if venue == Venue::Lighter { 2 } else { 4 },
            min_size: d(1, 2),
            min_notional: d(1, 0),
            maker_fee: d(0, 0),
            taker_fee: d(4, 4),
        },
    }
}

fn book(venue: Venue, sequence: u64, previous: Option<u64>) -> Input {
    Input::Book {
        venue,
        sequence,
        previous,
        bids: vec![Level {
            price: d(99, 0),
            size: d(5, 0),
        }],
        asks: vec![Level {
            price: d(101, 0),
            size: d(5, 0),
        }],
    }
}

fn account(venue: Venue, at: u64, position: Decimal) -> Input {
    Input::Account {
        venue,
        position,
        available: d(100, 0),
        open: vec![],
        requested_ms: at,
    }
}

fn fill(id: &str, venue: Venue, order: &Intent, oid: u64, size: Decimal) -> Input {
    let Action::Place {
        side, kind, price, ..
    } = order.action
    else {
        panic!("expected placement")
    };
    Input::Fill {
        venue,
        fill: Fill {
            id: id.into(),
            client: Some(order.client),
            oid,
            side,
            maker: kind == Kind::PostOnly,
            price,
            size,
            fee: d(1, 2),
            venue_ms: 1_000,
        },
    }
}

fn update(venue: Venue, order: &Intent, oid: u64, status: Status, filled: Decimal) -> Input {
    Input::Order {
        venue,
        client: Some(order.client),
        oid: Some(oid),
        status,
        filled,
    }
}

struct Fixture {
    engine: Engine,
    journal: JournalWriter,
    file: tempfile::NamedTempFile,
    seq: u64,
}

impl Fixture {
    fn new(mode: Mode) -> Result<Self> {
        let file = tempfile::NamedTempFile::new()?;
        let mut journal = JournalWriter::new(file.reopen()?);
        let config = config(mode);
        journal.append(
            None,
            Event::MakerStart {
                version: MAKER_FORMAT_VERSION,
                config: config.clone(),
                seed: 7,
                observe: false,
                capture: None,
            },
        )?;
        Ok(Self {
            engine: Engine::new(config, 7, false)?,
            journal,
            file,
            seq: 1,
        })
    }

    fn apply(&mut self, at: u64, input: Input) -> Result<Vec<Intent>> {
        self.journal.append(
            None,
            Event::MakerInput {
                elapsed_ms: at,
                input: input.clone(),
            },
        )?;
        let intents = self.engine.apply(self.seq, at, input)?;
        for intent in &intents {
            self.journal.append(
                None,
                Event::MakerIntent {
                    intent: intent.clone(),
                },
            )?;
        }
        self.seq += 1 + intents.len() as u64;
        Ok(intents)
    }

    fn warm(&mut self, venue: Venue, generation: u64) -> Result<Vec<Intent>> {
        self.apply(1, Input::Connected { venue, generation })?;
        self.apply(2, instrument(venue))?;
        if self.engine.config.mode == Mode::Hyperliquid {
            self.apply(
                2,
                Input::Protection {
                    expires_ms: Some(60_000),
                },
            )?;
        }
        self.apply(3, book(venue, 3, None))?;
        self.apply(4, account(venue, 3, Decimal::ZERO))
    }

    fn assert_replays(&mut self) -> Result<()> {
        self.journal.append(
            None,
            Event::MakerEnd {
                summary: self.engine.end(),
            },
        )?;
        let replay = || -> Result<Vec<u8>> {
            let mut output = Vec::new();
            crate::engine::run(
                RecordSource::replay(self.file.reopen()?),
                || true,
                &mut output,
            )?;
            Ok(output)
        };
        let first = replay()?;
        assert!(String::from_utf8_lossy(&first).contains("intent "));
        assert_eq!(first, replay()?);
        let mut report = Vec::new();
        diagnostics::inspect(
            RecordSource::replay(self.file.reopen()?),
            None::<std::io::Empty>,
            &mut report,
        )?;
        assert!(String::from_utf8_lossy(&report).contains("markouts"));

        let corrupted = tempfile::NamedTempFile::new()?;
        let mut changed = false;
        for line in std::fs::read_to_string(self.file.path())?.lines() {
            let mut record: serde_json::Value = serde_json::from_str(line)?;
            if !changed && record["event"] == "maker_intent" {
                record["intent"]["ordinal"] = 3.into();
                changed = true;
            }
            writeln!(corrupted.as_file(), "{record}")?;
        }
        assert!(
            crate::engine::run(
                RecordSource::replay(corrupted.reopen()?),
                || true,
                std::io::sink()
            )
            .is_err()
        );
        Ok(())
    }
}

#[test]
fn maker_tracks_fill_during_cancel_and_replays() -> Result<()> {
    let venue = Venue::Hyperliquid;
    let mut fixture = Fixture::new(Mode::Hyperliquid)?;
    let quotes = fixture.warm(venue, 1)?;
    assert_eq!(quotes.len(), 2);
    assert!(
        matches!(quotes[0].action, Action::Place { side: Side::Buy, price, .. } if price == d(999, 1))
    );
    assert!(
        matches!(quotes[1].action, Action::Place { side: Side::Sell, price, .. } if price == d(1001, 1))
    );
    for (offset, quote) in quotes.iter().enumerate() {
        fixture.apply(
            8,
            update(
                venue,
                quote,
                10 + offset as u64,
                Status::Open,
                Decimal::ZERO,
            ),
        )?;
    }
    let cancels = fixture.apply(9, Input::Stop)?;
    assert_eq!(cancels.len(), 2);
    fixture.apply(10, update(venue, &quotes[0], 10, Status::Terminal, d(2, 1)))?;
    assert!(!fixture.engine.state.orders[&quotes[0].client].done());
    let fill = fill("first", venue, &quotes[0], 10, d(2, 1));
    fixture.apply(11, fill.clone())?;
    fixture.apply(12, fill)?;
    fixture.apply(
        13,
        update(venue, &quotes[1], 11, Status::Terminal, Decimal::ZERO),
    )?;
    fixture.apply(15, account(venue, 14, d(2, 1)))?;
    assert_eq!(fixture.engine.state.venue(venue).position, d(2, 1));
    assert_eq!(fixture.engine.state.venue(venue).fills, 1);
    assert!(fixture.engine.end().clean);
    fixture.assert_replays()
}

#[test]
fn cross_venue_uses_confirmed_fills_and_waits_for_partial_ioc() -> Result<()> {
    let hyper = Venue::Hyperliquid;
    let lighter = Venue::Lighter;
    let mut fixture = Fixture::new(Mode::Hedged)?;
    assert!(fixture.warm(hyper, 1)?.is_empty());
    fixture.apply(
        5,
        Input::Connected {
            venue: lighter,
            generation: 2,
        },
    )?;
    fixture.apply(6, instrument(lighter))?;
    fixture.apply(7, book(lighter, 3, None))?;
    let quotes = fixture.apply(8, account(lighter, 7, Decimal::ZERO))?;
    assert_eq!(quotes.len(), 2);
    assert!(
        matches!(quotes[0].action, Action::Place { side: Side::Buy, price, .. } if price < d(99, 0))
    );
    assert!(
        fixture
            .apply(
                10,
                update(lighter, &quotes[0], 10, Status::Open, Decimal::ZERO)
            )?
            .is_empty()
    );
    let maker_fill = fill("maker", lighter, &quotes[0], 10, d(30, 2));
    let hedges = fixture.apply(20, maker_fill.clone())?;
    let hedge = &hedges[0];
    assert_eq!(hedge.venue, hyper);
    assert!(
        matches!(hedge.action, Action::Place { kind: Kind::Ioc, side: Side::Sell,
        size, price } if size == d(30, 2) && price >= d(98901, 3) && price <= d(99, 0))
    );
    assert!(fixture.apply(25, maker_fill)?.is_empty());
    fixture.apply(30, update(hyper, hedge, 20, Status::Terminal, d(20, 2)))?;
    assert!(
        fixture
            .apply(240, Input::Tick)?
            .iter()
            .all(|intent| intent.venue == lighter)
    );
    let actions = fixture.apply(250, fill("hedge", hyper, hedge, 20, d(20, 2)))?;
    assert_eq!(fixture.engine.state.residual(), d(10, 2));
    let retry = actions.iter().find(|intent| intent.venue == hyper).unwrap();
    assert!(
        matches!(retry.action, Action::Place { kind: Kind::Ioc, side: Side::Sell,
        size, .. } if size == d(10, 2))
    );
    let cancels = fixture.apply(251, book(lighter, 11, Some(9)))?;
    assert!(!fixture.engine.state.venue(lighter).book.has_snapshot());
    assert!(
        cancels
            .iter()
            .all(|intent| matches!(intent.action, Action::Cancel { .. }))
    );
    assert!(fixture.apply(252, book(lighter, 12, Some(11)))?.is_empty());
    fixture.assert_replays()
}

#[test]
fn inventory_skew_and_cap_are_symmetric() -> Result<()> {
    let venue = Venue::Hyperliquid;
    for side in [Side::Buy, Side::Sell] {
        let mut fixture = Fixture::new(Mode::Hyperliquid)?;
        fixture.engine.config.max_inventory = d(3, 1);
        fixture.engine.config.max_residual = d(3, 1);
        let quotes = fixture.warm(venue, 1)?;
        let quote = &quotes[usize::from(side == Side::Sell)];
        fixture.apply(5, fill("cap", venue, quote, 10, d(3, 1)))?;
        let desired = strategy::desired(&fixture.engine.config, &fixture.engine.state, true);
        assert!(desired[usize::from(side == Side::Sell)].is_none());
        let reducing = desired[usize::from(side == Side::Buy)].unwrap();
        assert_eq!(reducing.price, d(100, 0));
        let unskewed = strategy::desired(&fixture.engine.config, &fixture.engine.state, false);
        assert!(unskewed[usize::from(side == Side::Sell)].is_none());
        let reducing = unskewed[usize::from(side == Side::Buy)].unwrap();
        let flat = if side == Side::Buy {
            d(1001, 1)
        } else {
            d(999, 1)
        };
        assert_eq!(reducing.price, flat);
    }
    Ok(())
}

#[test]
fn late_ioc_id_recovers_buffered_fill_and_a_zero_fill_can_retry() -> Result<()> {
    let hyper = Venue::Hyperliquid;
    let lighter = Venue::Lighter;
    let mut fixture = Fixture::new(Mode::Hedged)?;
    fixture.warm(hyper, 1)?;
    fixture.apply(
        5,
        Input::Connected {
            venue: lighter,
            generation: 2,
        },
    )?;
    fixture.apply(6, instrument(lighter))?;
    fixture.apply(7, book(lighter, 3, None))?;
    let quotes = fixture.apply(8, account(lighter, 7, Decimal::ZERO))?;
    let first = fixture.apply(20, fill("maker", lighter, &quotes[0], 10, d(30, 2)))?;
    let hedge = first.iter().find(|intent| intent.venue == hyper).unwrap();
    fixture.apply(
        30,
        update(hyper, hedge, 20, Status::Terminal, Decimal::ZERO),
    )?;
    assert!(fixture.apply(200, Input::Tick)?.is_empty());
    let retry = fixture
        .apply(220, Input::Tick)?
        .into_iter()
        .find(|intent| intent.venue == hyper)
        .unwrap();
    let mut observation = fill("hedge", hyper, &retry, 21, d(30, 2));
    if let Input::Fill { ref mut fill, .. } = observation {
        fill.client = None;
    }
    fixture.apply(225, observation.clone())?;
    assert_eq!(fixture.engine.state.residual(), d(30, 2));
    fixture.apply(226, update(hyper, &retry, 21, Status::Terminal, d(30, 2)))?;
    fixture.apply(227, observation)?;
    assert_eq!(fixture.engine.state.residual(), Decimal::ZERO);
    assert_eq!(fixture.engine.state.venue(hyper).fills, 1);
    assert!(fixture.engine.state.orders[&retry.client].done());
    fixture.assert_replays()
}

#[test]
fn fixed_fill_simulator_uses_lighter_flow_without_copying_historical_hyperliquid_fills()
-> Result<()> {
    let hs = Venue::Hyperliquid;
    let ls = Venue::Lighter;
    let mut fixture = Fixture::new(Mode::Hedged)?;
    fixture.warm(hs, 1)?;
    fixture.apply(
        5,
        Input::Connected {
            venue: ls,
            generation: 2,
        },
    )?;
    fixture.apply(6, instrument(ls))?;
    fixture.apply(7, book(ls, 3, None))?;
    let quotes = fixture.apply(8, account(ls, 7, Decimal::ZERO))?;
    let first = fixture.apply(50, fill("lighter-real", ls, &quotes[0], 4, d(30, 2)))?;
    let recorded_hedge = first.iter().find(|intent| intent.venue == hs).unwrap();
    fixture.apply(65, fill("hex-real", hs, recorded_hedge, 12, d(30, 2)))?;
    fixture.apply(
        66,
        update(hs, recorded_hedge, 12, Status::Terminal, d(30, 2)),
    )?;

    // A print attributable to our real hedge must not fill the simulated passive order.
    for (at, id, volume) in [(110, "hex-real", d(80, 0)), (130, "public-flow", d(530, 2))] {
        fixture.apply(
            at,
            Input::PublicTrade {
                venue: hs,
                connection: 1,
                id: id.into(),
                venue_ms: u64::MAX / 1000,
                trade: Some(TradePrint {
                    price: d(101, 0),
                    size: volume,
                    aggressor: Side::Buy,
                }),
                is_snapshot: false,
            },
        )?;
    }
    fixture.apply(380, Input::Stop)?;
    fixture.apply(550, Input::Tick)?;
    fixture.journal.append(
        None,
        Event::MakerEnd {
            summary: fixture.engine.end(),
        },
    )?;
    let mut report = Vec::new();
    let mut trace = Vec::new();
    crate::sim::run(
        fixture.file.path(),
        std::path::Path::new("examples/simulation.json"),
        crate::sim::Flow::FixedFills,
        77,
        None,
        &mut report,
        crate::sim::Diagnostics {
            trace: Some(&mut trace),
            measure_processing: false,
        },
    )?;
    let report: serde_json::Value = serde_json::from_slice(&report)?;
    assert_eq!(report["fixed_lighter_fills"], 1);
    assert_eq!(report["observed"]["hedge_ioc"]["full"], 1);
    assert_eq!(
        report["scenarios"][1]["metrics"]["hedge_passive"]["full"],
        1
    );
    for row in report["scenarios"].as_array().unwrap() {
        assert_eq!(row["metrics"]["quotes"]["placements"], 0);
        assert_eq!(row["metrics"]["venues"][0]["fills"], 1);
        assert_eq!(row["metrics"]["venues"][1]["fills"], 1);
    }
    for event in String::from_utf8(trace)?
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
    {
        let event = event?;
        if event["scenario"] == "passive_then_ioc"
            && event["event"] == "fill"
            && event["data"]["fill"]["id"]
                .as_str()
                .unwrap()
                .starts_with("sim-")
        {
            assert!(event["at_us"].as_u64().unwrap() >= 130_000);
        }
    }
    Ok(())
}

#[test]
fn passive_hedge_cancels_before_ioc_and_hedges_only_the_remainder() -> Result<()> {
    let hs = Venue::Hyperliquid;
    let ls = Venue::Lighter;
    let mut fixture = Fixture::new(Mode::Hedged)?;
    fixture.engine.config.hedge_policy = HedgePolicy::PassiveThenIoc { wait_ms: 150 };
    fixture.warm(hs, 1)?;
    fixture.apply(
        5,
        Input::Protection {
            expires_ms: Some(60_000),
        },
    )?;
    fixture.apply(
        6,
        Input::Connected {
            venue: ls,
            generation: 2,
        },
    )?;
    fixture.apply(7, instrument(ls))?;
    fixture.apply(8, book(ls, 3, None))?;
    let quotes = fixture.apply(9, account(ls, 8, Decimal::ZERO))?;
    let placements = fixture.apply(20, fill("lighter", ls, &quotes[0], 4, d(30, 2)))?;
    let passive = placements.iter().find(|intent| intent.venue == hs).unwrap();
    assert!(
        matches!(passive.action, Action::Place { side: Side::Sell, kind: Kind::PostOnly, price, .. }
        if price == d(101, 0))
    );
    fixture.apply(25, update(hs, passive, 8, Status::Open, Decimal::ZERO))?;
    let cancels = fixture.apply(170, Input::Tick)?;
    assert!(
        cancels.iter().any(|intent| intent.client == passive.client
            && matches!(intent.action, Action::Cancel { .. }))
    );
    assert!(
        fixture
            .apply(175, update(hs, passive, 8, Status::Terminal, d(10, 2)))?
            .iter()
            .all(|intent| intent.venue == ls)
    );
    let actions = fixture.apply(178, fill("partial-hedge", hs, passive, 8, d(10, 2)))?;
    assert!(actions.iter().any(|intent| intent.venue == hs
        && matches!(intent.action,
        Action::Place { side: Side::Sell, kind: Kind::Ioc, size, .. } if size == d(20, 2))));
    Ok(())
}

#[test]
fn aged_subminimum_residual_recovery_obeys_direction_size_and_age() -> Result<()> {
    let hl = Venue::Hyperliquid;
    let li = Venue::Lighter;
    let mut fixture = Fixture::new(Mode::Hedged)?;
    fixture.engine.config.max_residual_ms = 500;
    fixture.apply(
        1,
        Input::Connected {
            venue: hl,
            generation: 1,
        },
    )?;
    let mut meta = instrument(hl);
    let Input::Instrument {
        instrument: ref mut h,
        ..
    } = meta
    else {
        unreachable!()
    };
    h.min_notional = d(10, 0);
    fixture.apply(2, meta)?;
    fixture.apply(3, book(hl, 1, None))?;
    fixture.apply(4, account(hl, 3, Decimal::ZERO))?;
    fixture.apply(
        5,
        Input::Connected {
            venue: li,
            generation: 2,
        },
    )?;
    fixture.apply(6, instrument(li))?;
    fixture.apply(7, book(li, 1, None))?;
    let quotes = fixture.apply(8, account(li, 7, Decimal::ZERO))?;
    assert_eq!(quotes.len(), 2);
    assert!(
        fixture
            .apply(10, fill("dust", li, &quotes[0], 1, d(1, 1)))?
            .iter()
            .all(|intent| intent.venue != hl)
    );
    fixture.apply(511, Input::Tick)?;
    assert!(
        strategy::desired(&fixture.engine.config, &fixture.engine.state, true)
            .iter()
            .all(Option::is_none)
    );

    fixture.engine.config.residual_policy = ResidualPolicy::BoundedReduce {
        max_base: d(12, 2),
        max_age_ms: 1000,
    };
    let [bid, ask] = strategy::desired(&fixture.engine.config, &fixture.engine.state, true);
    assert!(bid.is_none());
    assert_eq!(ask.unwrap().size, d(2, 1));

    fixture.engine.config.residual_policy = ResidualPolicy::BoundedAggregate {
        max_base: d(12, 2),
        max_age_ms: 1000,
    };
    assert!(
        strategy::desired(&fixture.engine.config, &fixture.engine.state, true)
            .iter()
            .all(Option::is_some)
    );
    fixture.apply(1010, Input::Tick)?;
    assert!(
        strategy::desired(&fixture.engine.config, &fixture.engine.state, true)
            .iter()
            .all(Option::is_none)
    );
    assert_eq!(fixture.engine.state.residual(), d(1, 1));
    assert!(!fixture.engine.end().clean);
    Ok(())
}

#[test]
fn aged_hedgeable_residual_still_blocks_quotes_with_recovery_enabled() -> Result<()> {
    let hl = Venue::Hyperliquid;
    let li = Venue::Lighter;
    let mut fixture = Fixture::new(Mode::Hedged)?;
    fixture.engine.config.max_residual_ms = 500;
    fixture.engine.config.hedge_retry_ms = 1000;
    fixture.engine.config.residual_policy = ResidualPolicy::BoundedAggregate {
        max_base: d(2, 1),
        max_age_ms: 1000,
    };
    fixture.warm(hl, 1)?;
    fixture.apply(
        5,
        Input::Connected {
            venue: li,
            generation: 2,
        },
    )?;
    fixture.apply(6, instrument(li))?;
    fixture.apply(7, book(li, 1, None))?;
    let quotes = fixture.apply(8, account(li, 7, Decimal::ZERO))?;
    let intents = fixture.apply(10, fill("hedgeable", li, &quotes[0], 1, d(1, 1)))?;
    let hedge = intents.iter().find(|intent| intent.venue == hl).unwrap();
    fixture.apply(14, update(hl, hedge, 2, Status::Terminal, Decimal::ZERO))?;
    assert!(
        fixture
            .engine
            .state
            .active_orders()
            .all(|(_, order)| order.venue != hl)
    );
    fixture.apply(511, Input::Tick)?;
    assert!(
        strategy::desired(&fixture.engine.config, &fixture.engine.state, true)
            .iter()
            .all(Option::is_none)
    );
    Ok(())
}
