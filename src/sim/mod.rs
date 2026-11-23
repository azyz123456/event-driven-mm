mod clock;
mod execution;
mod report;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use clap::ValueEnum;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::event::{Event, Record, supported_maker_version};
use crate::intent::Side;
use crate::journal::JournalReader;
use crate::maker::diagnostics::Replay;
use crate::maker::{Action, Engine, HedgePolicy, Input, Intent, Mode, Venue};

use self::clock::{Clock, jitter};
use self::execution::Exchange;
use self::report::{Exposure, LatencySummary, Marks, metrics};

const VENUES: [Venue; 2] = [Venue::Hyperliquid, Venue::Lighter];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Flow {
    FixedFills,
    ClosedLoop,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Model {
    fees_bps: Option<FeeOverrides>,
    scenarios: Vec<Scenario>,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FeeOverrides {
    hyperliquid_maker: Decimal,
    hyperliquid_taker: Decimal,
    lighter_maker: Decimal,
    lighter_taker: Decimal,
}

impl FeeOverrides {
    fn validate(self) -> Result<()> {
        for value in [self.hyperliquid_maker, self.lighter_maker] {
            ensure!(
                (-Decimal::from(100)..=Decimal::from(100)).contains(&value),
                "maker fee override outside ±100 bps"
            );
        }
        for value in [self.hyperliquid_taker, self.lighter_taker] {
            ensure!(
                (Decimal::ZERO..=Decimal::from(100)).contains(&value),
                "taker fee override outside 0 to 100 bps"
            );
        }
        Ok(())
    }

    fn apply(self, venue: Venue, instrument: &mut crate::maker::Instrument) {
        let (maker, taker) = match venue {
            Venue::Hyperliquid => (self.hyperliquid_maker, self.hyperliquid_taker),
            Venue::Lighter => (self.lighter_maker, self.lighter_taker),
        };
        instrument.maker_fee = maker / Decimal::from(10_000);
        instrument.taker_fee = taker / Decimal::from(10_000);
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QuoteSignal {
    threshold_bps: Decimal,
    max_age_ms: u64,
}

#[derive(Default, Serialize)]
struct Opportunities {
    eligible: [u64; 2],
    suppressed: [u64; 2],
}

type Signals = Arc<[(u64, Decimal)]>;

pub fn load_signals(path: &Path) -> Result<Signals> {
    let mut lines = BufReader::new(
        File::open(path).with_context(|| format!("open signal {}", path.display()))?,
    )
    .lines();
    ensure!(
        lines.next().transpose()?.as_deref() == Some("available_us,prediction_bps"),
        "invalid signal CSV header"
    );
    let mut points = Vec::new();
    for (idx, line) in lines.enumerate() {
        let line = line?;
        let (at, value) = line
            .split_once(',')
            .with_context(|| format!("invalid signal row {}", idx + 2))?;
        let at = at.parse()?;
        let value: Decimal = value.parse()?;
        ensure!(
            value.abs() <= Decimal::from(1_000) && points.last().is_none_or(|(last, _)| *last < at),
            "signals must have strictly increasing receipt times and bounded predictions"
        );
        points.push((at, value));
    }
    ensure!(!points.is_empty(), "signal CSV has no observations");
    Ok(points.into())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    name: String,
    hedge: HedgePolicy,
    outbound_us: u64,
    inbound_us: u64,
    jitter_us: u64,
    cancel_ahead: Decimal,
    extra_taker_fee_bps: Decimal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    quote_signal: Option<QuoteSignal>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    respect_trade_time: bool,
}

impl Scenario {
    fn validate(&self, strategy: &crate::maker::Config) -> Result<()> {
        ensure!(
            !self.name.trim().is_empty(),
            "scenario name must be nonempty"
        );
        ensure!(
            (1..=60_000_000).contains(&self.outbound_us)
                && (1..=60_000_000).contains(&self.inbound_us)
                && self.jitter_us <= 60_000_000,
            "scenario {} requires positive latency up to 60 seconds and jitter up to 60 seconds",
            self.name
        );
        ensure!(
            (Decimal::ZERO..=Decimal::ONE).contains(&self.cancel_ahead),
            "scenario {} cancel_ahead must be between zero and one",
            self.name
        );
        ensure!(
            (Decimal::ZERO..=Decimal::from(100)).contains(&self.extra_taker_fee_bps),
            "scenario {} extra taker fee must be between zero and 100 bps",
            self.name
        );
        if let Some(signal) = self.quote_signal {
            ensure!(
                signal.threshold_bps > Decimal::ZERO
                    && signal.threshold_bps <= Decimal::from(1_000)
                    && (1..=10_000).contains(&signal.max_age_ms),
                "invalid quote signal threshold or age"
            );
        }
        let mut config = strategy.clone();
        config.hedge_policy = self.hedge;
        config.validate()
    }
}

enum Scheduled {
    Outbound { generation: u64, intent: Intent },
    Inbound(Input),
}

struct Simulation {
    engine: Engine,
    clock: Clock<Scheduled>,
    venues: [Exchange; 2],
    scenario: Scenario,
    fees: Option<FeeOverrides>,
    seed: u64,
    current_us: u64,
    next_send: [u64; 2],
    next_delivery: [u64; 2],
    next_account: [u64; 2],
    anchor: u64,
    exposure: Exposure,
    cancels: usize,
    hyper_taker_notional: Decimal,
    signals: Option<Signals>,
    opportunity_bucket: [Option<u64>; 2],
    opportunities: Opportunities,
    capture_wall_ms: u64,
}

impl Simulation {
    fn new(
        config: &crate::maker::Config,
        scenario: Scenario,
        fees: Option<FeeOverrides>,
        seed: u64,
        session_seed: u64,
        flow: Flow,
    ) -> Result<Self> {
        let mut config = config.clone();
        config.hedge_policy = scenario.hedge;
        let engine = Engine::new(config, session_seed, false)?;
        let engine = if flow == Flow::FixedFills {
            engine.with_fixed_lighter_fills()
        } else {
            engine
        };
        Ok(Self {
            engine,
            scenario,
            fees,
            seed,
            clock: Clock::default(),
            venues: Default::default(),
            cancels: 0,
            current_us: 0,
            next_send: [0; 2],
            next_delivery: [0; 2],
            next_account: [0; 2],
            anchor: 0,
            exposure: Exposure::default(),
            hyper_taker_notional: Decimal::ZERO,
            signals: None,
            opportunity_bucket: [None; 2],
            opportunities: Opportunities::default(),
            capture_wall_ms: 0,
        })
    }

    fn drain(&mut self, until_us: u64, trace: &mut impl Write) -> Result<()> {
        while let Some((at, event)) = self.clock.before(until_us) {
            self.current_us = at;
            match event {
                Scheduled::Outbound { generation, intent } => {
                    let events = self.venues[venue_index(intent.venue)].arrival(
                        at,
                        generation,
                        &intent,
                        self.engine.config.book_stale_ms,
                        if intent.venue == Venue::Hyperliquid {
                            self.scenario.extra_taker_fee_bps / Decimal::from(10_000)
                        } else {
                            Decimal::ZERO
                        },
                    );
                    self.notify(intent.venue, events, intent.caused_by_seq);
                }
                Scheduled::Inbound(input) => self.decision(input, trace)?,
            }
        }
        Ok(())
    }

    fn feed(
        &mut self,
        at_us: u64,
        seq: u64,
        input: &Input,
        own_print: bool,
        trace: &mut impl Write,
    ) -> Result<()> {
        self.drain(at_us, trace)?;
        self.current_us = at_us;
        self.anchor = seq;
        match input {
            Input::Connected { venue, .. } => {
                self.next_account[venue_index(*venue)] = 0;
                self.update_market(*venue, input, trace)?;
                if *venue == Venue::Hyperliquid {
                    // Simulated cancels and transport health are modeled on this tape.
                    // A live session still needs actual exchange protection.
                    self.decision(
                        Input::Protection {
                            expires_ms: Some(u64::MAX),
                        },
                        trace,
                    )?;
                }
            }
            Input::Disconnected { venue, .. }
            | Input::Book { venue, .. }
            | Input::Instrument { venue, .. } => {
                self.update_market(*venue, input, trace)?;
            }
            Input::PublicTrade {
                venue,
                venue_ms,
                trade: Some(trade),
                ..
            } if !own_print => {
                let eligible_at = if self.scenario.respect_trade_time {
                    (*venue_ms)
                        .saturating_sub(self.capture_wall_ms)
                        .saturating_mul(1_000)
                        .min(at_us)
                } else {
                    at_us
                };
                let events = self.venues[venue_index(*venue)].trade_at(
                    *venue,
                    at_us,
                    eligible_at,
                    *trade,
                    self.engine.config.book_stale_ms,
                )?;
                self.notify(*venue, events, seq);
            }
            Input::Fill { fill, .. } => {
                trace_line(
                    trace,
                    &self.scenario.name,
                    at_us,
                    "fixed_lighter_fill",
                    fill,
                )?;
                self.decision(input.clone(), trace)?;
            }
            Input::Tick => self.decision(Input::Tick, trace)?,
            Input::Stop => self.decision(Input::Stop, trace)?,
            _ => {}
        }
        self.refresh_accounts(trace)
    }

    fn update_market(&mut self, venue: Venue, input: &Input, trace: &mut impl Write) -> Result<()> {
        let mut input = input.clone();
        if let Input::Instrument { instrument, .. } = &mut input
            && let Some(fees) = self.fees
        {
            fees.apply(venue, instrument);
        }
        self.venues[venue_index(venue)].market(
            self.current_us,
            &input,
            self.scenario.cancel_ahead,
        )?;
        self.decision(input, trace)
    }

    fn refresh_accounts(&mut self, trace: &mut impl Write) -> Result<()> {
        let now_ms = self.current_us / 1000;
        for venue in VENUES {
            let index = venue_index(venue);
            let exchange = &self.venues[index];
            if exchange.generation().is_none() || self.current_us < self.next_account[index] {
                continue;
            }
            let available = self.engine.config.min_available_collateral;
            self.decision(
                Input::Account {
                    venue,
                    available,
                    open: exchange.open_orders(),
                    requested_ms: now_ms,
                    position: self.engine.state.venue(venue).position,
                },
                trace,
            )?;
            let delay_ms = if self.engine.state.venue(venue).account_ms == Some(now_ms) {
                (self.engine.config.account_stale_ms / 2).clamp(1, 500)
            } else {
                1
            };
            self.next_account[index] = self.current_us.saturating_add(delay_ms * 1000);
        }
        Ok(())
    }

    fn notify(&mut self, venue: Venue, events: Vec<Input>, key: u64) {
        if events.is_empty() {
            return;
        }
        let index = venue_index(venue);
        let jitter = jitter(self.seed, key, 10 + index as u64, self.scenario.jitter_us);
        let at =
            (self.current_us + self.scenario.inbound_us + jitter).max(self.next_delivery[index]);
        self.next_delivery[index] = at;
        for event in events {
            self.clock.push(at, Scheduled::Inbound(event));
        }
    }

    fn decision(&mut self, input: Input, trace: &mut impl Write) -> Result<()> {
        let now = self.current_us;
        if matches!(input, Input::Order { .. }) {
            trace_line(trace, &self.scenario.name, now, "order_update", &input)?;
        }
        let forecast = self
            .scenario
            .quote_signal
            .zip(self.signals.as_ref())
            .and_then(|(settings, samples)| {
                let next = samples.partition_point(|(at, _)| *at <= now);
                samples
                    .get(next.checked_sub(1)?)
                    .filter(|(at, _)| now - *at <= settings.max_age_ms * 1_000)
                    .map(|point| (settings, *point))
            });
        let suppressed = forecast.and_then(|(settings, (_, bps))| {
            (bps.abs() >= settings.threshold_bps).then_some(if bps > Decimal::ZERO {
                Side::Sell
            } else {
                Side::Buy
            })
        });
        self.engine.suppress_quote(suppressed);
        let intents = self.engine.apply(self.anchor, now / 1000, input)?;
        if self.signals.is_some() {
            for (idx, side) in [Side::Buy, Side::Sell].into_iter().enumerate() {
                if let Some((price, size)) = self.engine.quote_opportunities[idx]
                    && self.opportunity_bucket[idx] != Some(now / 100_000)
                {
                    self.opportunity_bucket[idx] = Some(now / 100_000);
                    self.opportunities.eligible[idx] += 1;
                    let filtered = suppressed == Some(side);
                    self.opportunities.suppressed[idx] += u64::from(filtered);
                    trace_line(
                        trace,
                        &self.scenario.name,
                        now,
                        "quote_opportunity",
                        &json!({"side":side, "price":price, "size":size, "suppressed":filtered,
                            "forecast":forecast.map(|(_, point)| point), "source_seq":self.anchor}),
                    )?;
                }
            }
        }
        self.exposure.observe(now, &self.engine, true);
        for match_ in &self.engine.state.applied_fills {
            trace_line(trace, &self.scenario.name, now, "fill", match_)?;
            if match_.venue == Venue::Hyperliquid && !match_.fill.maker {
                self.hyper_taker_notional += match_.fill.price * match_.fill.size;
            }
        }

        for intent in intents {
            let index = venue_index(intent.venue);
            let generation = self.venues[index]
                .generation()
                .context("intent with no simulated connection")?;
            let stage = match intent.action {
                Action::Cancel { .. } => 2,
                Action::Place { side, .. } => execution::index(side) as u64,
            };
            if matches!(intent.action, Action::Cancel { .. }) {
                self.cancels += 1;
            }
            let delay = self.scenario.outbound_us
                + jitter(
                    self.seed,
                    self.anchor ^ now.rotate_left(13),
                    20 + 3 * index as u64 + stage,
                    self.scenario.jitter_us,
                );
            let at = (now + delay).max(self.next_send[index]);
            self.next_send[index] = at;
            trace_line(trace, &self.scenario.name, now, "intent", &intent)?;
            self.clock
                .push(at, Scheduled::Outbound { generation, intent });
        }
        Ok(())
    }
}

#[derive(Default)]
struct Reference {
    own_prints: BTreeSet<(Venue, String)>,
    maker_fills: usize,
    acknowledged_writes: BTreeMap<u64, u64>,
    intents: BTreeMap<(Venue, u64), u64>,
    intent_to_write: Vec<u64>,
    first_private_event: Vec<u64>,
    exposure: Exposure,
    cancels: usize,
}

impl Reference {
    fn record(&mut self, record: &Record, engine: &Engine) {
        match &record.event {
            Event::MakerIntent { intent } => match intent.action {
                Action::Place { .. } => {
                    self.intents
                        .insert((intent.venue, intent.client), intent.id);
                }
                Action::Cancel { .. } => self.cancels += 1,
            },
            Event::TxFirstWrite {
                intent_id,
                enqueued_elapsed_ns,
                first_write_elapsed_ns,
            } => {
                self.acknowledged_writes
                    .insert(*intent_id, *first_write_elapsed_ns);
                self.intent_to_write.extend(
                    first_write_elapsed_ns
                        .checked_sub(*enqueued_elapsed_ns)
                        .map(|ns| ns / 1000),
                );
            }
            Event::MakerInput { input, .. } => {
                let key = match input {
                    Input::Order {
                        venue, client, oid, ..
                    } => Some((*venue, *client, *oid)),
                    Input::Fill { venue, fill } => Some((*venue, fill.client, Some(fill.oid))),
                    _ => None,
                };
                if let Some((venue, client, oid)) = key
                    && let Some(timing) = record.receive_timing
                    && let Some(client) = client
                        .or_else(|| oid.and_then(|oid| engine.state.client_for_oid(venue, oid)))
                    && let Some(id) = self.intents.get(&(venue, client))
                    && let Some(&write_ns) = self.acknowledged_writes.get(id)
                    && let Some(delta) = timing.decoded_elapsed_ns.checked_sub(write_ns)
                {
                    self.acknowledged_writes.remove(id);
                    self.first_private_event.push(delta / 1000);
                }
            }
            _ => {}
        }
    }
}

pub fn run(
    log: &Path,
    config_path: &Path,
    flow: Flow,
    seed: u64,
    signal_path: Option<&Path>,
    mut output: impl Write,
    mut trace: impl Write,
) -> Result<()> {
    let Model {
        scenarios,
        fees_bps,
    } = serde_json::from_reader(
        File::open(config_path)
            .with_context(|| format!("open simulation config {}", config_path.display()))?,
    )?;
    if let Some(fees) = fees_bps {
        fees.validate()?;
    }
    ensure!(
        !scenarios.is_empty() && scenarios.len() <= 32,
        "provide between 1 and 32 scenarios"
    );
    let mut names = BTreeSet::new();
    ensure!(
        scenarios
            .iter()
            .all(|scenario| names.insert(&scenario.name)),
        "duplicate scenario names"
    );

    let signals = signal_path.map(load_signals).transpose()?;
    ensure!(
        signals.is_some()
            || scenarios
                .iter()
                .all(|scenario| scenario.quote_signal.is_none()),
        "scenario quote_signal requires --quote-signal"
    );
    ensure!(
        flow == Flow::ClosedLoop
            || scenarios
                .iter()
                .all(|scenario| scenario.quote_signal.is_none()),
        "quote filtering requires closed-loop simulation"
    );

    let mut reader = open(log)?;
    let start = reader.next_record()?.context("empty simulation journal")?;
    let capture_wall_ms = start.local_time_unix_ms;
    let Event::MakerStart {
        version,
        config,
        seed: session_seed,
        observe,
        capture,
    } = start.event
    else {
        anyhow::bail!("simulate requires a maker session journal");
    };
    ensure!(
        supported_maker_version(version) && config.mode == Mode::Hedged,
        "simulate requires a supported hedged maker journal"
    );
    ensure!(
        flow == Flow::ClosedLoop || !observe,
        "fixed-fills needs a signed capture with Lighter fills; use closed-loop for observe captures"
    );
    ensure!(
        !observe || fees_bps.is_some(),
        "observe captures require explicit fees_bps in the simulation config"
    );
    let mut reference = Reference::default();
    let mut detailed_prints = [0; 2];
    let mut missing_print_details = [false; 2];
    while let Some(record) = reader.next_record()? {
        if let Event::MakerInput { input, .. } = record.event {
            match input {
                Input::Fill { venue, fill } => {
                    reference.own_prints.insert((venue, fill.id));
                }
                Input::PublicTrade {
                    venue,
                    trade: None,
                    is_snapshot: false,
                    ..
                } => {
                    missing_print_details[venue_index(venue)] = true;
                }
                _ => {}
            }
        }
    }
    reader.finish()?;
    let needs_passive = scenarios
        .iter()
        .any(|scenario| matches!(scenario.hedge, HedgePolicy::PassiveThenIoc { .. }));
    ensure!(
        !needs_passive || (version >= 5 && !missing_print_details[0]),
        "passive hedging needs Hyperliquid prints with price, size, and aggressor side"
    );
    ensure!(
        flow != Flow::ClosedLoop || (version >= 5 && !missing_print_details[1]),
        "closed-loop quoting needs Lighter prints with price, size, and aggressor side"
    );

    let mut sims = scenarios
        .into_iter()
        .map(|scenario| {
            scenario.validate(&config)?;
            let mut sim = Simulation::new(&config, scenario, fees_bps, seed, session_seed, flow)?;
            sim.signals = signals.clone();
            sim.capture_wall_ms = capture_wall_ms;
            Ok(sim)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut reader = open(log)?;
    reader.next_record()?;
    let mut original = Replay::new(Engine::new(config, session_seed, observe)?);
    let mut marks = Marks::new();
    let mut observed_prints = BTreeSet::new();
    let mut now_us = 0;
    let mut last_seq = 0;
    while let Some(record) = reader.next_record()? {
        if !observe {
            match &record.event {
                Event::MakerIntent { intent } => {
                    trace_line(&mut trace, "observed", now_us, "intent", intent)?
                }
                Event::TxFirstWrite {
                    first_write_elapsed_ns,
                    ..
                } => trace_line(
                    &mut trace,
                    "observed",
                    *first_write_elapsed_ns / 1000,
                    "first_write",
                    &record.event,
                )?,
                _ => {}
            }
        }
        reference.record(&record, &original.engine);
        let seq = record.seq;
        last_seq = seq;
        let work = match &record.event {
            Event::MakerInput { elapsed_ms, input } => {
                let elapsed_us = elapsed_ms
                    .checked_mul(1000)
                    .context("capture time out of range")?;
                let decoded_us = record
                    .receive_timing
                    .map_or(0, |timing| timing.decoded_elapsed_ns / 1000);
                now_us = now_us.max(elapsed_us).max(decoded_us);
                ensure!(now_us < u64::MAX - 240_000_000, "capture time out of range");
                Some(input.clone())
            }
            _ => None,
        };
        original
            .apply(record, &mut io::sink())
            .with_context(|| format!("verify source record {seq}"))?;
        let Some(input) = work else {
            continue;
        };
        reference.exposure.observe(now_us, &original.engine, true);
        for applied in &original.engine.state.applied_fills {
            trace_line(&mut trace, "observed", now_us, "fill", applied)?;
        }
        if let Input::Book { venue, .. } = &input
            && let Some((bid, ask)) = original.engine.state.venue(*venue).book.touch()
        {
            marks.insert(*venue, (now_us, (bid + ask) / Decimal::TWO));
        }
        if matches!(input, Input::Order { .. } | Input::Fill { .. }) {
            if flow == Flow::FixedFills {
                for applied in &original.engine.state.applied_fills {
                    if applied.venue == Venue::Lighter && applied.fill.maker {
                        reference.maker_fills += 1;
                        let fixed = Input::Fill {
                            venue: Venue::Lighter,
                            fill: applied.fill.clone(),
                        };
                        for sim in &mut sims {
                            sim.feed(now_us, seq, &fixed, false, &mut trace)?;
                        }
                    }
                }
            }
            continue;
        }
        if matches!(input, Input::Account { .. } | Input::Protection { .. }) {
            continue;
        }
        let own_print = if let Input::PublicTrade {
            venue,
            id,
            venue_ms,
            is_snapshot,
            trade: Some(_),
            ..
        } = &input
        {
            if *is_snapshot || *venue_ms < capture_wall_ms {
                continue;
            }
            let key = (*venue, id.clone());
            if !observed_prints.insert(key.clone()) {
                continue;
            }
            detailed_prints[venue_index(*venue)] += 1;
            reference.own_prints.contains(&key)
        } else {
            false
        };
        for sim in &mut sims {
            sim.feed(now_us, seq, &input, own_print, &mut trace)?;
        }
    }
    reader.finish()?;
    ensure!(
        flow != Flow::FixedFills || reference.maker_fills > 0,
        "capture has no applied Lighter maker fills"
    );
    ensure!(
        marks.len() == 2,
        "simulation needs usable books from both venues"
    );
    let observed = metrics(
        &original.engine,
        &marks,
        now_us,
        reference.exposure,
        reference.cancels,
    );
    let source_end = original.finish()?;

    let mut results = Vec::new();
    for mut sim in sims {
        // Stop at the last real observation. Orders still in flight remain explicit in the report.
        sim.drain(now_us + 1, &mut trace)?;
        if sim.engine.state.stop_ms.is_none() {
            sim.feed(now_us, last_seq, &Input::Stop, false, &mut trace)?;
        }
        let pending = sim.clock.len();
        let metrics = metrics(&sim.engine, &marks, now_us, sim.exposure, sim.cancels);
        results.push((
            sim.scenario,
            metrics,
            pending,
            sim.hyper_taker_notional,
            sim.opportunities,
        ));
    }
    let baseline = results[0].1.net_pnl;
    let calibration = (!observe).then_some(&observed);
    let rows = results.iter().map(|(scenario, metrics, pending, taker_notional, opportunities)| {
        let delta = metrics.net_pnl.zip(baseline).map(|(pnl, baseline)| pnl - baseline);
        let break_even = delta.filter(|gain| *gain > Decimal::ZERO
            && !taker_notional.is_zero() && scenario.extra_taker_fee_bps.is_zero())
            .map(|gain| gain * Decimal::from(10_000) / *taker_notional);
        json!({
            "scenario": scenario, "metrics": metrics, "pending_events": pending, "quote_opportunities": opportunities,
            "delta_pnl_vs_first": delta,
            "delta_pnl_vs_observed": calibration.and_then(|observed| metrics.net_pnl.zip(observed.net_pnl).map(|(sim, actual)| sim - actual)),
            "hyperliquid_taker_notional": taker_notional,
            "additional_fee_break_even_bps": break_even,
        })
    }).collect::<Vec<_>>();
    let report = json!({
        "version": 1, "journal_version": version, "capture": capture, "mode": flow, "seed": seed,
        "duration_us": now_us, "fixed_lighter_fills": reference.maker_fills,
        "detailed_trade_counts": {"hyperliquid": detailed_prints[0], "lighter": detailed_prints[1]},
        "signal_rows": signals.as_ref().map(|s| s.len()), "fees_bps_override": fees_bps, "funding": "excluded", "end_valuation": "last_valid_venue_mid",
        "observed": calibration, "source_end": source_end,
        "recorded_timing": {
            "intent_to_first_write": LatencySummary::of(reference.intent_to_write),
            "first_write_to_first_private_observation": LatencySummary::of(reference.first_private_event),
        },
        "scenarios": rows,
    });
    serde_json::to_writer_pretty(&mut output, &report)?;
    writeln!(output)?;
    output.flush()?;
    trace.flush()?;
    Ok(())
}

fn open(log: &Path) -> Result<JournalReader> {
    Ok(JournalReader::new(File::open(log).with_context(|| {
        format!("open simulation journal {}", log.display())
    })?))
}

fn venue_index(venue: Venue) -> usize {
    usize::from(venue == Venue::Lighter)
}

fn trace_line(
    trace: &mut impl Write,
    name: &str,
    at_us: u64,
    event: &str,
    data: &impl Serialize,
) -> Result<()> {
    #[derive(Serialize)]
    struct Line<'a, T> {
        scenario: &'a str,
        at_us: u64,
        event: &'a str,
        data: &'a T,
    }
    serde_json::to_writer(
        &mut *trace,
        &Line {
            scenario: name,
            at_us,
            event,
            data,
        },
    )?;
    writeln!(trace)?;
    Ok(())
}
