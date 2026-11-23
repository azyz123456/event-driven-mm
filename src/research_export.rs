//! Point-in-time market and quote records from an original maker journal.
use std::io::{self, Write};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

use crate::event::{Event, supported_maker_version};
use crate::maker::{Engine, Input, Venue, diagnostics::Replay};
use crate::queue::RecordSource;

pub fn run(mut reader: RecordSource, mut output: impl Write) -> Result<()> {
    let start = reader.next(&|| true)?.context("empty maker journal")?;
    let Event::MakerStart {
        version,
        config,
        seed,
        observe,
        capture,
    } = start.event
    else {
        anyhow::bail!("research-export requires a maker journal");
    };
    ensure!(
        supported_maker_version(version),
        "unsupported maker journal version"
    );
    let header = json!({"type":"header", "journal_version":version, "start_unix_ms":start.local_time_unix_ms,
        "strategy":config, "observe":observe, "capture":capture});
    line(&mut output, &header)?;
    let mut replay = Replay::new(Engine::new(config, seed, observe)?);
    let mut at_us = 0;
    while let Some(record) = reader.next(&|| true)? {
        let seq = record.seq;
        let timing = record
            .receive_timing
            .map(|stamp| stamp.decoded_elapsed_ns / 1000);
        let is_input = matches!(record.event, Event::MakerInput { .. });
        let mut book = None;
        let mut data = None;
        match &record.event {
            Event::MakerInput { elapsed_ms, input } => {
                at_us = at_us
                    .max(
                        elapsed_ms
                            .checked_mul(1000)
                            .context("research time overflow")?,
                    )
                    .max(timing.unwrap_or_default());
                match input {
                    Input::Book { venue, .. } => book = Some(*venue),
                    Input::PublicTrade { trade: Some(_), .. }
                    | Input::Order { .. }
                    | Input::Connected { .. }
                    | Input::Disconnected { .. }
                    | Input::Instrument { .. } => data = Some(serde_json::to_value(input)?),
                    _ => {}
                }
            }
            Event::MakerIntent { intent } => {
                data = Some(json!({"type":"intent", "intent": intent}))
            }
            _ => {}
        }
        replay
            .apply(record, &mut io::sink())
            .with_context(|| format!("verify source record {seq}"))?;
        if let Some(venue) = book {
            data = Some(book_at(&replay.engine, venue));
        }
        if let Some(data) = data {
            line(
                &mut output,
                &json!({"source_seq":seq, "available_us":at_us, "data":data}),
            )?;
        }
        if is_input {
            for fill in &replay.engine.state.applied_fills {
                line(
                    &mut output,
                    &json!({"source_seq":seq, "available_us":at_us, "data":{"type":"fill",
                    "venue":fill.venue, "fill":fill.fill}}),
                )?;
            }
        }
    }
    let end = replay.finish()?;
    line(
        &mut output,
        &json!({"type":"end", "available_us":at_us, "source_end":end}),
    )?;
    output.flush()?;
    Ok(())
}

fn book_at(engine: &Engine, venue: Venue) -> Value {
    let book = &engine.state.venue(venue).book;
    json!({"type":"book", "venue":venue, "sequence":book.sequence(), "fresh":engine.state.book_fresh(venue, &engine.config),
        "bids":book.bids.iter().rev().take(5).collect::<Vec<_>>(), "asks":book.asks.iter().take(5).collect::<Vec<_>>()})
}

fn line(output: &mut impl Write, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *output, value)?;
    writeln!(output)?;
    Ok(())
}
