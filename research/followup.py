"""Run paired recovery and quote-value scenarios, or check signed calibration evidence."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess

import numpy as np

from .archive import FEES
from .evaluate import cohort_markouts, hourly_equity, interval, market_mid, read_trace

DAYS = ["2026-09-01", "2026-10-01", "2026-11-01"]
AGGREGATE = {
    "kind": "bounded_aggregate",
    "max_base": "0.00020",
    "max_age_ms": 1_800_000,
}
REDUCE = AGGREGATE | {"kind": "bounded_reduce"}


def model():
    def scenario(name, latency=12_000, **options):
        return dict(
            name=name,
            hedge={"kind": "ioc"},
            outbound_us=latency,
            inbound_us=latency,
            jitter_us=2000,
            cancel_ahead="0",
            extra_taker_fee_bps="0",
            respect_trade_time=True,
            **options,
        )

    return {
        "fees_bps": FEES,
        "scenarios": [
            scenario("strict"),
            scenario("larger_lot", maker_size="0.00050"),
            scenario("bounded_reduce", residual_policy=REDUCE),
            scenario("bounded_aggregate", residual_policy=AGGREGATE),
            scenario(
                "value_widen", residual_policy=AGGREGATE, value_signal_max_age_ms=5_000
            ),
            scenario("aggregate_150ms", 150_000, residual_policy=AGGREGATE),
            scenario(
                "value_150ms",
                150_000,
                residual_policy=AGGREGATE,
                value_signal_max_age_ms=5_000,
            ),
        ],
    }


def evaluate(day, binary, work, raw, values, out, config, existing):
    log = work / f"{day}.jsonl"
    trace = out / f"{day}_trace.jsonl"
    report = out / f"{day}_simulation.json"
    if not existing:
        command = [
            str(binary),
            "simulate",
            "--log",
            str(log),
            "--config",
            str(config),
            "--mode",
            "closed-loop",
            "--seed",
            "71",
            "--quote-signal",
            str(values / f"{day}_widen.csv"),
            "--trace",
            str(trace),
        ]
        with report.open("w") as output:
            subprocess.run(command, check=True, stdout=output)
    stats = json.loads(report.read_text())
    quotes, fills = read_trace(trace, None)
    books = {venue: market_mid(raw, day, venue) for venue in ("hyperliquid", "lighter")}
    rows = {}
    for row in stats["scenarios"]:
        name = row["scenario"]["name"]
        metrics = row["metrics"]
        equity = hourly_equity(fills.get(name, []), books)
        if not np.isclose(equity[-1], float(metrics["net_pnl"]), atol=1e-6):
            raise ValueError(f"ledger reconciliation failed for {day} {name}")
        eligible = quotes[quotes.name == name] if len(quotes) else quotes
        count = int(eligible.filled.gt(0).sum()) if len(eligible) else 0
        executed = fills.get(name, [])
        turnover = sum(event[3] * event[4] for event in executed)
        rows[name] = {
            "metrics": metrics,
            "hourly_equity": equity.tolist(),
            "maker_filled_orders": count,
            "maker_filled_order_fraction": count / len(eligible)
            if len(eligible)
            else None,
            "net_pnl_per_filled_order": float(equity[-1]) / count if count else None,
            "two_venue_turnover_usdc": turnover,
            "net_bps_per_turnover": float(equity[-1]) * 10_000 / turnover
            if turnover
            else None,
            "max_hourly_drawdown_usdc": float(
                (np.maximum.accumulate(equity) - equity).max()
            ),
            "lighter_post_fill_markout": cohort_markouts(
                executed, books["lighter"], [500, 2_000, 5_000], 1_000
            ),
            "cancels_per_fill": metrics["order_cancels"] / metrics["venues"][0]["fills"]
            if metrics["venues"][0]["fills"]
            else None,
        }
    deltas = {}
    for base, candidate in [
        ("strict", "bounded_aggregate"),
        ("strict", "bounded_reduce"),
        ("strict", "larger_lot"),
        ("bounded_aggregate", "value_widen"),
        ("aggregate_150ms", "value_150ms"),
    ]:
        dif = np.array(rows[candidate]["hourly_equity"]) - np.array(
            rows[base]["hourly_equity"]
        )
        deltas[candidate] = {
            "baseline": base,
            "change_usdc": float(dif[-1]),
            "ci95": interval(dif, 71),
        }
    print(
        day,
        {
            n: (x["metrics"]["net_pnl"], x["metrics"]["venues"][0]["fills"])
            for n, x in rows.items()
        },
        flush=True,
    )
    return {"day": day, "scenarios": rows, "paired": deltas}


def calibrate(path):
    report = json.loads(path.read_text())
    if (
        report["mode"] != "fixed_fills"
        or not report.get("observed")
        or not report["source_end"]["clean"]
        or report["fixed_lighter_fills"] == 0
    ):
        raise ValueError(
            "calibration requires a clean signed fixed-fills report with Lighter fills"
        )
    observed = report["observed"]
    if not any(
        float(observed[f"hedge_{kind}"]["filled_base"]) > 0
        for kind in ("ioc", "passive")
    ):
        raise ValueError("no observed Hyperliquid hedge fills")
    fields = [
        "net_pnl",
        "end_residual",
        "max_residual_age_us",
        "exposure_to_first_hedge_fill",
        "hedge_ioc",
        "hedge_passive",
    ]
    return {
        "sample_lighter_fills": report["fixed_lighter_fills"],
        "timing": report["recorded_timing"],
        "observed": {k: observed[k] for k in fields},
        "simulated": [
            {"name": s["scenario"]["name"], **{k: s["metrics"][k] for k in fields}}
            for s in report["scenarios"]
        ],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="command", required=True)
    cp = sub.add_parser("calibrate")
    cp.add_argument("--report", type=Path, required=True)
    run = sub.add_parser("closed-loop")
    for key in ("work", "raw", "values", "out"):
        run.add_argument("--" + key, type=Path, required=True)
    run.add_argument(
        "--binary", type=Path, default=Path("target/release/event-driven-mm")
    )
    run.add_argument("--days", nargs="+", default=DAYS)
    run.add_argument("--summarize-only", action="store_true")
    args = ap.parse_args()
    if args.command == "calibrate":
        print(json.dumps(calibrate(args.report), indent=2, allow_nan=False))
        return
    args.out.mkdir(parents=True, exist_ok=True)
    config = args.out / "scenarios.json"
    config.write_text(json.dumps(model(), indent=2) + "\n")
    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs = [
            pool.submit(
                evaluate,
                d,
                args.binary,
                args.work,
                args.raw,
                args.values,
                args.out,
                config,
                args.summarize_only,
            )
            for d in args.days
        ]
        days = [job.result() for job in jobs]
    (args.out / "closed_loop.json").write_text(
        json.dumps({"model": model(), "days": days}, indent=2, allow_nan=False) + "\n"
    )


if __name__ == "__main__":
    main()
