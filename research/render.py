"""Render the documented research figures from stored study results."""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

BLUE = "#2166ac"
ORANGE = "#b34e10"
TEAL = "#007f72"
MUTED = "#4b5563"


def setup():
    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 10, "axes.spines.top": False,
        "axes.spines.right": False, "axes.labelcolor": "#182230", "text.color": "#182230",
        "xtick.color": "#364153", "ytick.color": "#364153", "figure.facecolor": "white",
        "axes.facecolor": "white", "axes.titleweight": "bold",
    })


def save(fig, path):
    fig.savefig(path, dpi=180, facecolor="white", bbox_inches="tight")
    plt.close(fig)


def forecast_chart(result, out):
    dates = result["config"]["days"][1:]
    h = result["config"]["horizons_ms"]
    fig, axes = plt.subplots(1, 3, figsize=(11, 3.65), sharey=True, layout="constrained")
    model_names = ["hyperliquid", "lighter", "two_venue"]
    labels = ["Hyperliquid only", "Lighter only", "Both venues"]
    for i, day in enumerate(dates):
        ax = axes[i]
        rows = {x["horizon_ms"]: x for x in result["results"] if x["variant"] == "receive" and x["day"] == day}
        for j, (key, label, color) in enumerate(zip(model_names, labels, [BLUE, ORANGE, TEAL])):
            values = [100 * rows[step]["models"][key]["r2_zero"] for step in h]
            ax.bar(np.arange(len(h)) + (j-1)*.23, values, width=.21, color=color, label=label)
        ax.set_title(day + (" · holdout" if i == len(dates)-1 else ""))
        ax.axhline(0, color=MUTED, lw=.8)
        ax.set_xticks(np.arange(len(h)), [f"{x/1000:g} s" for x in h])
        ax.set_xlabel("Forecast horizon")
        ax.grid(axis="y", color="#e3e8ef", linewidth=.7)
        ax.set_axisbelow(True)
    axes[0].set_ylabel("Out-of-sample R² vs zero (%)")
    axes[0].legend(loc="upper left", frameon=False, fontsize=8)
    fig.suptitle("Lighter adds the most information at the two-second horizon", fontsize=14, fontweight="bold")
    save(fig, out / "forecast-r2.png")


def latency_chart(result, audit, out):
    fig, (ax, cadence) = plt.subplots(1, 2, figsize=(10.7, 3.7), layout="constrained")
    rows = [x for x in result["common_holdout_delay"] if x["horizon_ms"] == 2000]
    delays = [0 if x["variant"] == "receive" else int(x["variant"].rsplit("_",1)[1]) for x in rows]
    rows, delays = zip(*sorted(zip(rows, delays), key=lambda pair: pair[1]))
    skill = [100*x["two_venue_r2"] for x in rows]
    gain = [100*x["incremental_r2"]["estimate"] for x in rows]
    intervals = np.asarray([x["incremental_r2"]["ci95"] for x in rows]).T*100
    ax.plot(delays, skill, "o-", color=TEAL, label="Two-venue R² vs zero (%)")
    ax.errorbar(delays, gain, yerr=[gain-intervals[0], intervals[1]-gain], color=BLUE, marker="s",
                linestyle="--", capsize=3, label="Gain vs Hyperliquid only (pp)")
    ax.axhline(0, lw=.8, color=MUTED)
    ax.set(xlabel="Extra delay on Lighter inputs (ms)", ylabel="Skill (%) / gain (percentage points)",
           title="2 s holdout forecast on identical rows")
    ax.legend(loc="upper right", frameon=False, fontsize=8)
    ax.grid(axis="y", color="#e3e8ef")
    date = result["config"]["days"][-1]
    book = audit[date]["venues"]
    y = np.arange(4)
    order = [("hyperliquid", "books"), ("lighter", "books"), ("hyperliquid", "trades"), ("lighter", "trades")]
    values = [book[v][k]["timestamp_lag_ms"] for v,k in order]
    medians = np.array([x[1] for x in values])
    lower = np.array([x[0] for x in values])
    upper = np.array([x[2] for x in values])
    for i, color in enumerate([BLUE, ORANGE, BLUE, ORANGE]):
        cadence.errorbar(medians[i], y[i], xerr=[[medians[i]-lower[i]], [upper[i]-medians[i]]],
                        fmt="o", color=color, capsize=3)
    cadence.set_yticks(y, ["Hyperliquid book", "Lighter book", "Hyperliquid trades", "Lighter trades"])
    cadence.invert_yaxis()
    cadence.set(xlabel="Exchange timestamp to archive receipt (ms)", title="Median and 1st–95th percentiles")
    cadence.grid(axis="x", color="#e3e8ef")
    fig.suptitle("Forecast skill is sensitive to feed timing", fontsize=14, fontweight="bold")
    save(fig, out / "latency-sensitivity.png")


def execution_chart(execution, out):
    fig, axes = plt.subplots(1, len(execution["days"]), figsize=(11, 3.7), sharey=True, layout="constrained")
    for i, (day, ax) in enumerate(zip(execution["days"], np.atleast_1d(axes))):
        rows = day["scenarios"]
        for key, color, label in [("baseline_base", BLUE, "Baseline"), ("filtered_base", ORANGE, "Signal filter")]:
            values = rows[key]["equity"]
            ax.plot(range(len(values)), values, color=color, label=label, lw=1.7)
        ax.axhline(0, color=MUTED, lw=.7)
        ax.set_xlabel("UTC hour")
        ax.set_xticks(np.arange(0,25,4))
        ax.set_title(day["day"] + (" · holdout" if i == len(execution["days"])-1 else " · development"))
        ax.grid(axis="y", color="#e3e8ef")
    np.atleast_1d(axes)[0].set_ylabel("Cumulative modeled net equity (USDC)")
    np.atleast_1d(axes)[0].legend(frameon=False)
    fig.suptitle("Closed-loop quote-filter comparison", fontsize=14, fontweight="bold")
    save(fig, out / "execution-equity.png")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    setup()
    read = lambda name: json.loads((args.out / (name+".json")).read_text())
    forecast = read("forecast_results")
    audit = read("data_manifest")
    forecast_chart(forecast, args.out)
    latency_chart(forecast, audit, args.out)
    if (args.out/"execution_results.json").exists():
        execution_chart(read("execution_results"), args.out)


if __name__ == "__main__":
    main()
