use std::collections::{BTreeMap, VecDeque};
use std::io::{BufRead, Write};

use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::json;

use crate::event::{Event, Record, supported_maker_version};
use crate::latency::Histogram;
use crate::queue::RecordSource;

use super::{Action, End, Engine, Fill, Input, Intent, Kind, Venue};

pub struct Replay {
    pub(crate) engine: Engine,
    generated: VecDeque<Intent>,
    end: Option<End>,
}

impl Replay {
    pub fn new(engine: Engine) -> Self {
        Self {
            engine,
            generated: VecDeque::new(),
            end: None,
        }
    }

    pub fn apply(&mut self, record: Record, output: &mut impl Write) -> Result<()> {
        ensure!(self.end.is_none(), "record after maker end");
        match record.event {
            Event::MakerInput { elapsed_ms, input } => {
                ensure!(self.generated.is_empty(), "missing recorded maker intent");
                self.generated
                    .extend(self.engine.apply(record.seq, elapsed_ms, input)?);
            }
            Event::MakerIntent { intent } => {
                let generated = self
                    .generated
                    .pop_front()
                    .context("unexpected recorded maker intent")?;
                ensure!(
                    generated == intent,
                    "maker intent mismatch at trigger {}",
                    intent.caused_by_seq
                );
                writeln!(output, "intent {}", serde_json::to_string(&generated)?)?;
            }
            Event::MakerEnd { summary } => {
                ensure!(
                    self.generated.is_empty(),
                    "maker end before recorded intent"
                );
                ensure!(self.engine.end() == summary, "maker end differs in replay");
                writeln!(output, "summary {}", serde_json::to_string(&summary)?)?;
                self.end = Some(summary);
            }
            Event::MakerPrepared { .. }
            | Event::MakerProtectionPrepared { .. }
            | Event::MakerNotice { .. }
            | Event::TcpOpen { .. }
            | Event::TcpClose { .. }
            | Event::TcpConnectFailed { .. }
            | Event::HttpRequest { .. }
            | Event::HttpResponse { .. }
            | Event::WsOut { .. }
            | Event::TxFirstWrite { .. } => {}
            _ => anyhow::bail!("unexpected event in maker journal"),
        }
        Ok(())
    }

    pub fn finish(self) -> Result<End> {
        ensure!(self.generated.is_empty(), "missing recorded maker intent");
        self.end.context("maker journal has no end record")
    }
}

pub fn inspect(
    mut reader: RecordSource,
    mut interactive: Option<impl BufRead>,
    mut output: impl Write,
) -> Result<()> {
    let record = reader.next(&|| true)?.context("empty maker journal")?;
    let Event::MakerStart {
        config,
        seed,
        observe,
        version,
        ..
    } = record.event
    else {
        anyhow::bail!("inspect-maker requires a maker journal");
    };
    ensure!(
        record.seq == 0 && supported_maker_version(version),
        "invalid maker session start"
    );
    let mut replay = Replay::new(Engine::new(config, seed, observe)?);
    let mut stats = Report::default();
    let mut line = String::new();
    while let Some(record) = reader.next(&|| true)? {
        let applies_input = matches!(record.event, Event::MakerInput { .. });
        stats.before(&record, &replay.engine);
        let seq = record.seq;
        let step = interactive
            .is_some()
            .then(|| serde_json::to_value(&record))
            .transpose()?;
        replay.apply(record, &mut std::io::sink())?;
        if applies_input {
            stats.after(&replay.engine);
        }
        if let Some(input) = &mut interactive {
            writeln!(
                output,
                "{}",
                json!({
                    "seq": seq, "record": step, "state": replay.engine.state,
                    "unhedged": replay.engine.state.residual(),
                })
            )?;
            output.flush()?;
            line.clear();
            if input.read_line(&mut line)? == 0 || line.trim() == "q" {
                return Ok(());
            }
        }
    }
    let duration_ms = replay.engine.state.now_ms;
    let venues = [Venue::Hyperliquid, Venue::Lighter].map(|venue| {
        let state = replay.engine.state.venue(venue);
        let mark = state
            .book
            .touch()
            .map(|(bid, ask)| (bid + ask) / Decimal::TWO);
        let mark_to_market = mark.map(|price| state.cash + state.position * price - state.fees);
        json!({"venue": venue, "fills": state.fills, "cash": state.cash, "fees": state.fees,
            "mark_to_market": mark_to_market, "position": state.position})
    });
    let last_observed = replay.engine.end();
    let end = replay.end;
    writeln!(
        output,
        "{}",
        json!({
            "duration_ms": duration_ms, "venues": venues, "end": end, "last_observed": last_observed,
            "diagnostics": stats.summary(duration_ms),
        })
    )?;
    Ok(())
}

#[derive(Default)]
struct Report {
    intents: u64,
    cancels: u64,
    placements: u64,
    iocs: u64,
    passive_hedges: u64,
    disconnects: u64,
    fills: BTreeMap<(Venue, String), u64>,
    public: BTreeMap<(Venue, String), BTreeMap<u64, u64>>,
    books: BTreeMap<(Venue, u64), (u64, u64, u64)>,
    hedge_writes: BTreeMap<u64, u64>,
    lots: VecDeque<Lot>,
    matched_size: Decimal,
    matched_edge: Decimal,
    ioc_outcomes: BTreeMap<u64, Decimal>,
    terminal_iocs: [u64; 3],
    max_residual: Decimal,
    max_residual_age_ms: u64,

    maker_to_intent: Histogram,
    maker_to_first_write: Histogram,
    maker_to_hedge_fill: Histogram,
    markouts: VecDeque<Markout>,
    positive: [u64; 3],
    negative: [u64; 3],
    flat: [u64; 3],
}

struct Lot {
    venue: Venue,
    at_ns: u64,
    fill: Fill,
    remaining: Decimal,
}

struct Markout {
    next: usize,
    at_ms: u64,
    fill: Fill,
}

const MARKOUTS: [u64; 3] = [100, 1_000, 5_000];

impl Report {
    fn before(&mut self, record: &Record, engine: &Engine) {
        let timing = record.receive_timing.map(|timing| timing.read_elapsed_ns);
        match &record.event {
            Event::MakerInput { input, .. } => match input {
                Input::Disconnected { .. } => self.disconnects += 1,
                Input::PublicBook {
                    venue,
                    sequence,
                    connection,
                    ..
                } => {
                    if let Some(at) = timing {
                        let (first, last, first_connection) = self
                            .books
                            .entry((*venue, *sequence))
                            .or_insert((at, at, *connection));
                        if *first_connection != *connection {
                            *first = (*first).min(at);
                            *last = (*last).max(at);
                        }
                    }
                }
                Input::PublicTrade {
                    venue,
                    id,
                    connection,
                    ..
                } => {
                    if let Some(at) = timing {
                        self.public
                            .entry((*venue, id.clone()))
                            .or_default()
                            .entry(*connection)
                            .or_insert(at);
                    }
                }
                Input::Fill { venue, fill } => {
                    let key = (*venue, fill.id.clone());
                    if let Some(at) = timing {
                        self.fills.entry(key.clone()).or_insert(at);
                    }
                }
                _ => {}
            },
            Event::MakerIntent { intent } => {
                self.intents += 1;
                match intent.action {
                    Action::Cancel { .. } => self.cancels += 1,
                    Action::Place { kind, size, .. } => {
                        if engine.config.role(intent.venue) == super::OrderRole::Quote {
                            self.placements += 1;
                        } else {
                            if kind == Kind::Ioc {
                                self.iocs += 1;
                                self.ioc_outcomes.insert(intent.client, size);
                            } else {
                                self.passive_hedges += 1;
                            }
                            if let Some(at) = self.lots.front().map(|lot| lot.at_ns) {
                                self.hedge_writes.insert(intent.id, at);
                                self.maker_to_intent.add(Some(
                                    (engine.state.now_ms * 1_000_000).saturating_sub(at),
                                ));
                            }
                        }
                    }
                }
            }
            Event::TxFirstWrite {
                intent_id,
                first_write_elapsed_ns,
                ..
            } => {
                if let Some(at) = self.hedge_writes.remove(intent_id) {
                    self.maker_to_first_write
                        .add(first_write_elapsed_ns.checked_sub(at));
                }
            }
            _ => {}
        }
    }

    fn after(&mut self, engine: &Engine) {
        for applied in &engine.state.applied_fills {
            let fill = &applied.fill;
            let at = self
                .fills
                .get(&(applied.venue, fill.id.clone()))
                .copied()
                .unwrap_or(engine.state.now_ms * 1_000_000);
            if fill.maker && engine.config.role(applied.venue) == super::OrderRole::Quote {
                self.markouts.push_back(Markout {
                    next: 0,
                    at_ms: engine.state.now_ms,
                    fill: fill.clone(),
                });
            }
            if engine.config.mode == super::Mode::Hedged {
                self.match_fill(applied.venue, fill, at);
            }
        }
        let mut done = Vec::new();
        for (&client, &size) in &self.ioc_outcomes {
            let order = &engine.state.orders[&client];
            if order.done() {
                let index = if order.filled.is_zero() {
                    0
                } else if order.filled < size {
                    1
                } else {
                    2
                };
                self.terminal_iocs[index] += 1;
                done.push(client);
            }
        }
        for client in done {
            self.ioc_outcomes.remove(&client);
        }
        self.max_residual = self.max_residual.max(engine.state.residual().abs());
        if let Some(since) = engine.state.residual_since_ms {
            self.max_residual_age_ms = self.max_residual_age_ms.max(engine.state.now_ms - since);
        }

        let venue = engine.config.mode.maker();
        if !engine.state.book_fresh(venue, &engine.config) {
            return;
        }
        let Some((bid, ask)) = engine.state.venue(venue).book.touch() else {
            return;
        };
        let mid = (bid + ask) / Decimal::TWO;
        for mark in &mut self.markouts {
            while mark.next < MARKOUTS.len()
                && engine.state.now_ms - mark.at_ms >= MARKOUTS[mark.next]
            {
                let move_in_favor = mark.fill.side.signed(mid - mark.fill.price);
                match move_in_favor.cmp(&Decimal::ZERO) {
                    std::cmp::Ordering::Greater => self.positive[mark.next] += 1,
                    std::cmp::Ordering::Less => self.negative[mark.next] += 1,
                    std::cmp::Ordering::Equal => self.flat[mark.next] += 1,
                }
                mark.next += 1;
            }
        }
        while self
            .markouts
            .front()
            .is_some_and(|mark| mark.next == MARKOUTS.len())
        {
            self.markouts.pop_front();
        }
    }

    fn match_fill(&mut self, venue: Venue, fill: &Fill, at: u64) {
        let mut remaining = fill.size;
        while let Some(lot) = self.lots.front_mut() {
            if remaining.is_zero() || lot.fill.side == fill.side {
                break;
            }
            let quantity = lot.remaining.min(remaining);
            if lot.venue != venue {
                let fee = fill.fee / fill.size + lot.fill.fee / lot.fill.size;
                self.matched_edge +=
                    quantity * (fill.side.signed(lot.fill.price - fill.price) - fee);
                self.matched_size += quantity;
                if lot.venue == Venue::Lighter && venue == Venue::Hyperliquid {
                    self.maker_to_hedge_fill.add(at.checked_sub(lot.at_ns));
                }
            }
            remaining -= quantity;
            lot.remaining -= quantity;
            if lot.remaining.is_zero() {
                self.lots.pop_front();
            }
        }
        if remaining > Decimal::ZERO {
            self.lots.push_back(Lot {
                venue,
                at_ns: at,
                fill: fill.clone(),
                remaining,
            });
        }
    }

    fn summary(&self, duration_ms: u64) -> impl Serialize {
        let mut public_first = 0;
        let mut account_first = 0;
        let mut simultaneous = 0;
        let mut arrival_gap = Histogram::new();
        let mut fanout_gap = Histogram::new();
        for (key, arrivals) in &self.public {
            let Some(first) = arrivals.values().min() else {
                continue;
            };
            if let Some(account) = self.fills.get(key) {
                arrival_gap.add(Some(first.abs_diff(*account)));
                match first.cmp(account) {
                    std::cmp::Ordering::Less => public_first += 1,
                    std::cmp::Ordering::Greater => account_first += 1,
                    std::cmp::Ordering::Equal => simultaneous += 1,
                }
            }
            if arrivals.len() > 1 {
                fanout_gap.add(arrivals.values().max().map(|last| last - first));
            }
        }
        let mut book_fanout = Histogram::new();
        for &(first, last, _) in self.books.values().filter(|(first, last, _)| first != last) {
            book_fanout.add(Some(last - first));
        }
        let hourly_fills = if duration_ms == 0 {
            Decimal::ZERO
        } else {
            Decimal::from(self.fills.len()) * Decimal::from(3_600_000) / Decimal::from(duration_ms)
        };
        json!({
            "intents": self.intents, "post_only_placements": self.placements, "cancels": self.cancels,
            "ioc_attempts": self.iocs, "passive_hedge_attempts": self.passive_hedges, "ioc_unfilled": self.terminal_iocs[0], "ioc_partial": self.terminal_iocs[1],
            "ioc_filled": self.terminal_iocs[2], "ioc_unresolved": self.ioc_outcomes.len(),
            "max_unhedged": self.max_residual, "max_unhedged_age_ms": self.max_residual_age_ms,
            "matched_base": self.matched_size, "matched_trade_edge_net_of_fees": self.matched_edge, "disconnects": self.disconnects, "wire_account_fills_per_hour": hourly_fills,
            "maker_to_ioc_intent": self.maker_to_intent.summary(),
            "maker_to_ioc_first_write": self.maker_to_first_write.summary(),
            "maker_to_ioc_fill": self.maker_to_hedge_fill.summary(),
            "public_account_matches": { "account_first": account_first, "public_first": public_first,
                "simultaneous": simultaneous, "absolute_gap": arrival_gap.summary() },
            "trade_feed_spread": fanout_gap.summary(),
            "book_feed_spread": book_fanout.summary(),
            "markouts": { "horizons_ms": MARKOUTS, "positive": self.positive,
                "negative": self.negative, "flat": self.flat, "open_samples": self.markouts.len() },
        })
    }
}
