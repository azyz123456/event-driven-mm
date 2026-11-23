"""Compare inventory skew on paired archive days and benchmark replay processing."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import platform
import subprocess
from tempfile import TemporaryDirectory

import numpy as np

from .archive import FEES, write_archive
from .features import VENUES, download, source, study_config, url

PILOT_DAYS = ["2026-09-01", "2026-10-01", "2026-11-01"]
EVALUATION_DAYS = ["2026-05-01", "2026-06-01", "2026-07-01", "2026-08-01"]
LATENCY_DAY = EVALUATION_DAYS[0]
POLICIES = ("fixed_spread", "inventory_skew")
SEED = 71


def model(policies):
    return {
        "fees_bps": FEES,
        "scenarios": [
            {
                "name": name,
                "inventory_skew": name == "inventory_skew",
                "hedge": {"kind": "ioc"},
                "outbound_us": 12_000,
                "inbound_us": 12_000,
                "jitter_us": 2_000,
                "cancel_ahead": "0",
                "extra_taker_fee_bps": "0",
                "respect_trade_time": True,
            }
            for name in policies
        ],
    }


def run(binary, tape, settings, report_path, measure=False):
    command = [
        str(binary),
        "simulate",
        "--mode",
        "closed-loop",
        "--log",
        str(tape),
        "--config",
        str(settings),
        "--seed",
        str(SEED),
    ]
    if measure:
        command.append("--measure-processing")
    with report_path.open("w") as report:
        subprocess.run(command, stdout=report, check=True)
    return json.loads(report_path.read_text())


def day_metrics(day, report):
    duration = report["duration_us"]
    if duration <= 0:
        raise ValueError(f"replay for {day} has no elapsed time")
    result = {
        "day": day,
        "duration_us": duration,
        "books": report["book_events"],
        "trades": report["trade_events"],
        "market_events": sum(report["book_events"].values())
        + sum(report["trade_events"].values()),
        "empty_book_inputs": report["empty_book_inputs"],
        "timer_ticks": report["tick_events"],
        "policies": {},
    }
    for row in report["scenarios"]:
        metric = row["metrics"]
        maker = Decimal(metric["abs_maker_inventory_base_us"])
        net = Decimal(metric["abs_net_inventory_base_us"])
        result["policies"][row["scenario"]["name"]] = {
            "mean_abs_lighter_btc": float(maker / duration),
            "mean_abs_net_btc": float(net / duration),
            "abs_lighter_btc_us": str(maker),
            "abs_net_btc_us": str(net),
            "filled_lighter_orders": metric["quotes"]["filled_orders"],
            "lighter_filled_btc": float(metric["quotes"]["filled_base"]),
            "filled_notional_usdc": float(metric["filled_notional"]),
            "net_pnl_usdc": float(metric["net_pnl"]),
            "end_residual_btc": float(metric["end_residual"]),
            "order_updates": row["order_updates"],
            "fill_messages": row["fill_messages"],
        }
    return result


def paired_effect(days, columns, draws=20_000):
    area = np.array(columns, dtype=float)
    durations = np.array([d["duration_us"] for d in days])

    def effect(totals):
        base, skew = totals
        return 100 * (base - skew) / base if base > 0 else None

    totals = area.sum(axis=0)
    estimates = []
    if len(days) > 1:
        indices = np.random.default_rng(SEED).integers(0, len(days), (draws, len(days)))
        estimates = [
            p for pair in area[indices].sum(axis=1) if (p := effect(pair)) is not None
        ]
    return {
        "average_btc": dict(zip(POLICIES, (totals / durations.sum()).tolist())),
        "reduction_pct": effect(totals),
        "paired_day_bootstrap_ci95_pct": np.quantile(estimates, [0.025, 0.975]).tolist()
        if estimates
        else None,
        "bootstrap_draws_with_positive_baseline": len(estimates),
        "leave_one_day_out_pct": {
            day["day"]: effect(totals - area[i]) for i, day in enumerate(days)
        }
        if len(days) > 1
        else {},
    }


def cohort(days):
    return {
        "market_events": sum(d["market_events"] for d in days),
        "duration_us": sum(d["duration_us"] for d in days),
        "lighter_inventory": paired_effect(
            days,
            [
                [d["policies"][name]["abs_lighter_btc_us"] for name in POLICIES]
                for d in days
            ],
        ),
        "net_inventory": paired_effect(
            days,
            [
                [d["policies"][name]["abs_net_btc_us"] for name in POLICIES]
                for d in days
            ],
        ),
        "other_totals": {
            name: {
                metric: sum(d["policies"][name][metric] for d in days)
                for metric in (
                    "filled_lighter_orders",
                    "lighter_filled_btc",
                    "filled_notional_usdc",
                    "net_pnl_usdc",
                )
            }
            for name in POLICIES
        },
        "days": days,
    }


def timing_percentiles(hist):
    count = sum(hist.values())
    targets = {f"p{p}_ns": (count * p + 99) // 100 for p in (50, 95, 99)}
    result = dict.fromkeys(targets)
    seen = 0
    for ns, frequency in sorted(hist.items()):
        seen += frequency
        for key, rank in targets.items():
            if result[key] is None and seen >= rank:
                result[key] = ns
    return {"samples": count, **result}


def latency_summary(reports, expected):
    full = {"books": Counter(), "trades": Counter()}
    repeats = []
    for report in reports:
        observed = day_metrics(LATENCY_DAY, report)
        if observed["policies"]["inventory_skew"] != expected["policies"][
            "inventory_skew"
        ] or any(
            observed[key] != expected[key] for key in ("duration_us", "books", "trades")
        ):
            raise ValueError(
                "timed replay diverged from the untimed inventory-skew scenario"
            )
        hist = {
            kind: Counter({int(ns): n for ns, n in values.items()})
            for kind, values in report["scenarios"][0][
                "processing_ns_histograms"
            ].items()
        }
        for kind, values in hist.items():
            if sum(values.values()) != sum(expected[kind].values()):
                raise ValueError(f"latency sample count mismatch for {kind}")
            full[kind].update(values)
        repeats.append(
            {
                kind: timing_percentiles(h)
                for kind, h in (hist | {"all": hist["books"] + hist["trades"]}).items()
            }
        )
    return {
        "day": LATENCY_DAY,
        "unique_market_events": expected["market_events"],
        "policy": "inventory_skew",
        "runs": repeats,
        "pooled": {
            kind: timing_percentiles(h)
            for kind, h in (full | {"all": full["books"] + full["trades"]}).items()
        },
    }


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def system_command(*args):
    try:
        return subprocess.check_output(args, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def plot(report, path):
    from .render import BLUE, ORANGE, MUTED, plt, save, setup

    setup()
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 3.8), layout="constrained")
    daily = report["evaluation"]["days"]
    x = np.arange(len(daily))
    for ax, field, title in zip(
        axes,
        ("mean_abs_lighter_btc", "mean_abs_net_btc"),
        ("Lighter position", "Net position across venues"),
    ):
        for offset, color, policy, label in [
            (-0.17, BLUE, "fixed_spread", "Fixed spread"),
            (0.17, ORANGE, "inventory_skew", "Inventory skew"),
        ]:
            ax.bar(
                x + offset,
                [d["policies"][policy][field] * 1e6 for d in daily],
                width=0.32,
                color=color,
                label=label,
            )
        ax.set(
            title=title,
            xlabel="Evaluation day in 2026",
            ylabel="Time-weighted absolute inventory (µBTC)",
        )
        ax.set_xticks(x, [d["day"][5:] for d in daily])
        ax.grid(axis="y", color=MUTED, alpha=0.2)
        ax.set_axisbelow(True)
    axes[0].legend(frameon=False, fontsize=9)
    fig.suptitle(
        "Inventory skew against a matched fixed-spread baseline",
        fontsize=14,
        fontweight="bold",
    )
    save(fig, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", required=True, type=Path)
    parser.add_argument("--journals", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument(
        "--binary", type=Path, default=Path("target/release/event-driven-mm")
    )
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--jobs", type=int, default=3)
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be positive")

    days = PILOT_DAYS + EVALUATION_DAYS
    config = study_config() | {"days": days}
    if args.download:
        download(args.raw, config)
    args.out.mkdir(parents=True, exist_ok=True)
    args.journals.mkdir(parents=True, exist_ok=True)
    settings = args.out / "paired-scenarios.json"
    benchmark_settings = args.out / "latency-scenario.json"
    settings.write_text(json.dumps(model(POLICIES), indent=2) + "\n")
    benchmark_settings.write_text(json.dumps(model((POLICIES[1],)), indent=2) + "\n")
    binary = args.binary.resolve(strict=True)
    rustc = system_command("rustc", "-Vv")
    cpu_model = (
        system_command("sysctl", "-n", "machdep.cpu.brand_string")
        if platform.system() == "Darwin"
        else None
    )

    sources = {}
    for day in days:
        for venue in VENUES:
            for kind in ("book_snapshot_5", "trades"):
                archive = source(args.raw, day, venue, kind)
                sources[archive.name] = {
                    "url": url(config, day, venue, kind),
                    "sha256": sha256(archive),
                }

    def compare(day):
        tape = args.journals / f"{day}.jsonl"
        if not tape.exists():
            with TemporaryDirectory(dir=args.journals) as tmp:
                converted = Path(tmp) / tape.name
                write_archive(args.raw, day, converted)
                try:
                    tape.hardlink_to(converted)
                except FileExistsError:
                    pass
        print(f"Replaying {day}", flush=True)
        result = day_metrics(
            day, run(binary, tape, settings, args.out / f"{day}-paired.json")
        )
        result["journal_sha256"] = sha256(tape)
        print(
            f"Finished {day} with {result['market_events']:,} market events", flush=True
        )
        return result

    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        rows = list(pool.map(compare, days))
    expected = next(row for row in rows if row["day"] == LATENCY_DAY)
    tape = args.journals / f"{LATENCY_DAY}.jsonl"
    timed = []
    # Run the benchmark without competing replay processes.
    for repeat in range(1, 4):
        print(f"Timing replay {repeat} on {LATENCY_DAY}", flush=True)
        timed.append(
            run(
                binary,
                tape,
                benchmark_settings,
                args.out / f"timing-{repeat}.json",
                measure=True,
            )
        )

    result = {
        "seed": SEED,
        "model": model(POLICIES),
        "environment": {
            "cpu": cpu_model or platform.processor(),
            "os": platform.platform(),
            "rustc": rustc,
            "binary_sha256": sha256(binary),
        },
        "sources": sources,
        "pilot": cohort(rows[: len(PILOT_DAYS)]),
        "evaluation": cohort(rows[len(PILOT_DAYS) :]),
        "processing": latency_summary(timed, expected),
    }
    (args.out / "inventory_benchmark.json").write_text(
        json.dumps(result, indent=2, allow_nan=False) + "\n"
    )
    plot(result, args.out / "inventory-benchmark.png")


if __name__ == "__main__":
    main()
