use std::path::Path;
use std::process::{Command, Output};

use anyhow::Result;
use rust_decimal::Decimal;
use serde_json::Value;

const CAPTURE: &str = "tests/fixtures/sim-public.jsonl";
const SIM_CONFIG: &str = "examples/simulation.json";

fn simulate(log: &Path, mode: &str, trace: Option<&Path>) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_event-driven-mm"));
    command
        .args([
            "simulate", "--mode", mode, "--seed", "71", "--config", SIM_CONFIG, "--log",
        ])
        .arg(log);
    if let Some(path) = trace {
        command.arg("--trace").arg(path);
    }
    Ok(command.output()?)
}

#[test]
fn closed_loop_compares_policies_with_byte_identical_reports_and_traces() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let first_path = tmp.path().join("first");
    let second_path = tmp.path().join("second");
    let first = simulate(Path::new(CAPTURE), "closed-loop", Some(&first_path))?;
    let second = simulate(Path::new(CAPTURE), "closed-loop", Some(&second_path))?;
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(first.stdout, second.stdout);
    let trace = std::fs::read_to_string(&first_path)?;
    assert_eq!(trace, std::fs::read_to_string(&second_path)?);

    let report: Value = serde_json::from_slice(&first.stdout)?;
    assert_eq!(report["detailed_trade_counts"]["lighter"], 1);
    assert_eq!(report["detailed_trade_counts"]["hyperliquid"], 1);
    assert!(report["scenarios"][0]["processing_ns_histograms"].is_null());
    assert!(report["observed"].is_null());
    let ioc = &report["scenarios"][0]["metrics"];
    let passive = &report["scenarios"][1]["metrics"];
    assert_eq!(ioc["hedge_ioc"]["full"], 1);
    assert_eq!(passive["hedge_passive"]["full"], 1);
    assert_eq!(passive["hedge_ioc"]["placements"], 0);
    assert_eq!(
        ioc["end_residual"].as_str().unwrap().parse::<Decimal>()?,
        Decimal::ZERO
    );
    assert_eq!(
        passive["end_residual"]
            .as_str()
            .unwrap()
            .parse::<Decimal>()?,
        Decimal::ZERO
    );
    let pnl =
        |row: &Value| -> Decimal { row["metrics"]["net_pnl"].as_str().unwrap().parse().unwrap() };
    let scenarios = &report["scenarios"];
    assert_eq!(pnl(&scenarios[0]), Decimal::new(4512, 5));
    assert_eq!(pnl(&scenarios[1]), Decimal::new(652455, 6));
    assert!(pnl(&scenarios[3]) > pnl(&scenarios[4]));
    assert!(trace.contains("order_update") && trace.contains("fill"));
    let events = trace
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for scenario in scenarios.as_array().unwrap() {
        let mut positions = [Decimal::ZERO; 2];
        let mut area = [Decimal::ZERO; 2];
        let mut last = 0;
        let mut add_interval = |end: u64, positions: [Decimal; 2]| {
            let elapsed = Decimal::from(end - last);
            area[0] += positions[0].abs() * elapsed;
            area[1] += (positions[0] + positions[1]).abs() * elapsed;
            last = end;
        };
        for fill in events.iter().filter(|event| {
            event["scenario"] == scenario["scenario"]["name"] && event["event"] == "fill"
        }) {
            add_interval(fill["at_us"].as_u64().unwrap(), positions);
            let index = usize::from(fill["data"]["venue"] == "hyperliquid");
            let fill = &fill["data"]["fill"];
            let size = fill["size"].as_str().unwrap().parse::<Decimal>()?;
            positions[index] += if fill["side"] == "buy" { size } else { -size };
        }
        add_interval(report["duration_us"].as_u64().unwrap(), positions);
        for (key, area) in ["abs_maker_inventory_base_us", "abs_net_inventory_base_us"]
            .into_iter()
            .zip(area)
        {
            assert_eq!(
                scenario["metrics"][key]
                    .as_str()
                    .unwrap()
                    .parse::<Decimal>()?,
                area
            );
        }
    }
    Ok(())
}

#[test]
fn replay_processing_samples_count_market_events_and_preserve_results() -> Result<()> {
    let run = |measured| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_event-driven-mm"));
        command.args([
            "simulate",
            "--mode",
            "closed-loop",
            "--config",
            SIM_CONFIG,
            "--log",
            CAPTURE,
        ]);
        if measured {
            command.arg("--measure-processing");
        }
        command.output()
    };
    let ordinary = run(false)?;
    let timed = run(true)?;
    assert!(ordinary.status.success() && timed.status.success());
    let ordinary: Value = serde_json::from_slice(&ordinary.stdout)?;
    let mut timed: Value = serde_json::from_slice(&timed.stdout)?;
    let total = |key: &str| -> u64 {
        timed[key]
            .as_object()
            .unwrap()
            .values()
            .map(|v| v.as_u64().unwrap())
            .sum()
    };
    let books = total("book_events");
    let trades = total("trade_events");
    assert!(books > 0);
    assert_eq!(trades, 2);
    for scenario in timed["scenarios"].as_array_mut().unwrap() {
        let histograms = scenario["processing_ns_histograms"].take();
        for (kind, count) in [("books", books), ("trades", trades)] {
            let frequencies = histograms[kind].as_object().unwrap();
            assert_eq!(
                frequencies
                    .values()
                    .map(|v| v.as_u64().unwrap())
                    .sum::<u64>(),
                count
            );
        }
    }
    assert_eq!(ordinary, timed);
    Ok(())
}

#[test]
fn observe_capture_cannot_be_used_as_a_fixed_fill_baseline() -> Result<()> {
    let result = simulate(Path::new(CAPTURE), "fixed-fills", None)?;
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("fixed-fills needs a signed capture"));
    Ok(())
}

#[test]
fn old_maker_journal_replays_and_explains_why_passive_simulation_needs_prints() -> Result<()> {
    let mut rows = include_str!("fixtures/sim-public.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    rows[0]["version"] = 4.into();
    for record in &mut rows {
        if record["input"]["type"] == "public_trade" {
            let input = record["input"].as_object_mut().unwrap();
            input.remove("trade");
            input.remove("is_snapshot");
        }
    }
    let mut journal = tempfile::NamedTempFile::new()?;
    use std::io::Write;
    for record in rows {
        writeln!(journal, "{record}")?;
    }
    let replay = Command::new(env!("CARGO_BIN_EXE_event-driven-mm"))
        .args(["replay", "--log"])
        .arg(journal.path())
        .output()?;
    assert!(
        replay.status.success(),
        "{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    let result = simulate(journal.path(), "closed-loop", None)?;
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stderr)
            .contains("passive hedging needs Hyperliquid prints")
    );
    Ok(())
}

#[test]
fn quiet_version_five_capture_produces_zero_fills() -> Result<()> {
    use std::io::Write;
    let mut journal = tempfile::NamedTempFile::new()?;
    let rows = include_str!("fixtures/sim-public.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for (seq, mut record) in rows
        .into_iter()
        .filter(|record| {
            record["input"]["type"] != "public_trade" || record["input"]["is_snapshot"] == true
        })
        .enumerate()
    {
        record["seq"] = seq.into();
        writeln!(journal, "{record}")?;
    }
    let result = simulate(journal.path(), "closed-loop", None)?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Value = serde_json::from_slice(&result.stdout)?;
    assert_eq!(report["detailed_trade_counts"]["lighter"], 0);
    assert_eq!(report["detailed_trade_counts"]["hyperliquid"], 0);
    assert_eq!(report["scenarios"][0]["metrics"]["venues"][0]["fills"], 0);
    Ok(())
}

#[test]
fn faster_hyperliquid_arrival_can_beat_a_book_move_and_reports_break_even_fee() -> Result<()> {
    use std::io::Write;
    let mut journal = tempfile::NamedTempFile::new()?;
    let mut rows = include_str!("fixtures/sim-public.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let at = rows
        .iter()
        .position(|record| record["elapsed_ms"] == 110)
        .unwrap();
    rows.insert(
        at,
        serde_json::json!({
            "event":"maker_input", "local_time_unix_ms":1_000_095, "elapsed_ms":95,
            "input":{"type":"book","venue":"hyperliquid","sequence":2,"previous":null,
                "bids":[{"price":"98","size":"1"}],"asks":[{"price":"101","size":"1"}]}
        }),
    );
    for (seq, mut record) in rows.into_iter().enumerate() {
        record["seq"] = seq.into();
        writeln!(journal, "{record}")?;
    }
    let result = simulate(journal.path(), "closed-loop", None)?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Value = serde_json::from_slice(&result.stdout)?;
    let baseline = &report["scenarios"][0]["metrics"];
    let faster = &report["scenarios"][3];
    assert!(baseline["hedge_ioc"]["empty"].as_u64().unwrap() > 0);
    assert_eq!(faster["metrics"]["hedge_ioc"]["full"], 1);
    assert!(
        faster["additional_fee_break_even_bps"]
            .as_str()
            .unwrap()
            .parse::<Decimal>()?
            > Decimal::ZERO
    );
    assert!(report["scenarios"][4]["additional_fee_break_even_bps"].is_null());
    Ok(())
}

#[test]
fn signal_only_filters_the_adverse_side_after_the_signal_arrives() -> Result<()> {
    use std::io::Write;
    let tmp = tempfile::tempdir()?;
    let model_path = tmp.path().join("model.json");
    let signal_path = tmp.path().join("signal.csv");
    let trace_path = tmp.path().join("trace.jsonl");
    let mut config: Value = serde_json::from_str(include_str!("../examples/simulation.json"))?;
    let mut filtered = config["scenarios"][0].clone();
    filtered["name"] = "filtered".into();
    filtered["quote_signal"] = serde_json::json!({"threshold_bps":"0.25","max_age_ms":250});
    config["scenarios"] = serde_json::json!([config["scenarios"][0].clone(), filtered]);
    std::fs::write(&model_path, serde_json::to_string(&config)?)?;
    let mut file = std::fs::File::create(&signal_path)?;
    writeln!(file, "available_us,prediction_bps\n40000,-1")?;
    let result = Command::new(env!("CARGO_BIN_EXE_event-driven-mm"))
        .args([
            "simulate",
            "--mode",
            "closed-loop",
            "--log",
            CAPTURE,
            "--config",
        ])
        .arg(&model_path)
        .arg("--quote-signal")
        .arg(&signal_path)
        .arg("--trace")
        .arg(&trace_path)
        .output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Value = serde_json::from_slice(&result.stdout)?;
    assert_eq!(report["scenarios"][0]["metrics"]["venues"][0]["fills"], 1);
    assert_eq!(report["scenarios"][1]["metrics"]["venues"][0]["fills"], 0);
    let rows = std::fs::read_to_string(trace_path)?;
    let mut saw_cancel = false;
    for row in rows.lines().map(serde_json::from_str::<Value>) {
        let row = row?;
        if row["scenario"] != "filtered" {
            continue;
        }
        let at = row["at_us"].as_u64().unwrap();
        if row["event"] == "intent"
            && row["data"]["venue"] == "lighter"
            && row["data"]["action"] == "cancel"
            && at < 80_000
        {
            assert!(at >= 40_000);
            saw_cancel = true;
        }
        if row["event"] == "quote_opportunity" && row["data"]["suppressed"] == true {
            assert!(at >= 40_000);
            assert_eq!(row["data"]["side"], "buy");
        }
    }
    assert!(saw_cancel);
    Ok(())
}

#[test]
fn research_export_uses_monotonic_times_and_journal_sequences() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_event-driven-mm"))
        .args(["research-export", "--log", CAPTURE])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows = std::str::from_utf8(&output.stdout)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(rows.first().unwrap()["type"], "header");
    assert_eq!(rows.last().unwrap()["type"], "end");
    for pair in rows[1..rows.len() - 1].windows(2) {
        assert!(pair[0]["available_us"].as_u64() <= pair[1]["available_us"].as_u64());
        assert!(pair[0]["source_seq"].as_u64() <= pair[1]["source_seq"].as_u64());
    }
    assert!(
        rows.iter()
            .any(|row| row["data"]["trade"]["aggressor"] == "sell")
    );
    Ok(())
}

#[test]
fn value_signal_widens_only_selected_side_without_looking_ahead() -> anyhow::Result<()> {
    use std::io::Write;
    let tmp = tempfile::tempdir()?;
    let model_path = tmp.path().join("model.json");
    let signal_path = tmp.path().join("signal.csv");
    let trace_path = tmp.path().join("trace.jsonl");
    let mut config: serde_json::Value =
        serde_json::from_str(include_str!("../examples/simulation.json"))?;
    let mut wider = config["scenarios"][0].clone();
    wider["name"] = "wider".into();
    wider["value_signal_max_age_ms"] = 250.into();
    config["scenarios"] = serde_json::json!([config["scenarios"][0].clone(), wider]);
    std::fs::write(&model_path, serde_json::to_string(&config)?)?;
    writeln!(
        std::fs::File::create(&signal_path)?,
        "available_us,bid_widen_bps,ask_widen_bps\n40000,10,0"
    )?;
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_event-driven-mm"))
        .args([
            "simulate",
            "--mode",
            "closed-loop",
            "--log",
            "tests/fixtures/sim-public.jsonl",
            "--config",
        ])
        .arg(model_path)
        .arg("--quote-signal")
        .arg(signal_path)
        .arg("--trace")
        .arg(&trace_path)
        .output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let trace = std::fs::read_to_string(trace_path)?;
    let first = trace
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for row in first
        .iter()
        .filter(|row| row["event"] == "quote_opportunity" && row["scenario"] == "wider")
    {
        let at = row["at_us"].as_u64().unwrap();
        let bps = row["data"]["widen_bps"]
            .as_str()
            .unwrap()
            .parse::<rust_decimal::Decimal>()?;
        if at < 40_000 || row["data"]["side"] == "sell" || at > 290_000 {
            assert_eq!(bps, rust_decimal::Decimal::ZERO);
        }
    }
    Ok(())
}

#[test]
fn timely_value_signal_changes_the_placement_price_without_suppressing_either_side()
-> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let model_path = tmp.path().join("model.json");
    let signal_path = tmp.path().join("signal.csv");
    let trace_path = tmp.path().join("trace.jsonl");
    let mut config: serde_json::Value =
        serde_json::from_str(include_str!("../examples/simulation.json"))?;
    let mut wider = config["scenarios"][0].clone();
    wider["name"] = "wider".into();
    wider["value_signal_max_age_ms"] = 250.into();
    config["scenarios"] = serde_json::json!([config["scenarios"][0].clone(), wider]);
    std::fs::write(&model_path, serde_json::to_string(&config)?)?;
    std::fs::write(
        &signal_path,
        "available_us,bid_widen_bps,ask_widen_bps\n0,10,0\n",
    )?;
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_event-driven-mm"))
        .args([
            "simulate",
            "--mode",
            "closed-loop",
            "--log",
            "tests/fixtures/sim-public.jsonl",
            "--config",
        ])
        .arg(model_path)
        .arg("--quote-signal")
        .arg(signal_path)
        .arg("--trace")
        .arg(&trace_path)
        .output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let trace = std::fs::read_to_string(trace_path)?;
    let mut prices = std::collections::BTreeMap::new();
    for row in trace.lines().map(serde_json::from_str::<serde_json::Value>) {
        let row = row?;
        if row["event"] == "intent"
            && row["data"]["venue"] == "lighter"
            && row["data"]["action"] == "place"
        {
            let key = (
                row["scenario"].as_str().unwrap().to_owned(),
                row["data"]["side"].as_str().unwrap().to_owned(),
            );
            prices.entry(key).or_insert(
                row["data"]["price"]
                    .as_str()
                    .unwrap()
                    .parse::<rust_decimal::Decimal>()?,
            );
        }
    }
    assert!(prices[&("wider".into(), "buy".into())] < prices[&("ioc".into(), "buy".into())]);
    assert_eq!(
        prices[&("wider".into(), "sell".into())],
        prices[&("ioc".into(), "sell".into())]
    );
    Ok(())
}
