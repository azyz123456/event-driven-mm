"""Render figures from the follow-up's machine-readable results."""
import argparse
import json
from pathlib import Path

import numpy as np

from .render import BLUE, ORANGE, TEAL, MUTED, plt, save, setup


def timing(data, path):
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 3.7), layout="constrained")
    labels = ["Hyperliquid BBO", "Hyperliquid L2", "Lighter ticker"]
    for i, (channel, label, color) in enumerate(
        zip(["bbo", "l2Book", "ticker"], labels, [BLUE, BLUE, TEAL])
    ):
        median, tail = data["audit"][channel]["apparent_age_ms_p50_p95"]
        axes[0].plot([median, tail], [i, i], color=color, lw=2)
        axes[0].scatter([median], [i], color=color, zorder=2)
    axes[0].set_yticks(np.arange(len(labels)), labels)
    axes[0].invert_yaxis()
    axes[0].set(
        xlabel="Apparent receipt age (ms)", title="Same-host median to 95th percentile"
    )
    for clock, line in (("receive", "o-"), ("exchange", "x--")):
        key = (
            "paired_executable" if clock == "receive" else "exchange_paired_executable"
        )
        paths = [
            r for r in data[key] if r["channel"] == "bbo" and r["horizon_ms"] == 2000
        ]
        for metric, fee, color in (
            ("gross_bps", "gross", TEAL),
            ("net_bps", "net", ORANGE),
        ):
            axes[1].plot(
                [r["delay_ms"] for r in paths],
                [r[metric] for r in paths],
                line,
                label=f"{clock.title()} {fee}",
                color=color,
            )
    axes[1].axhline(0, color=MUTED, lw=0.8)
    axes[1].set(
        xlabel="Modeled delay to entry (ms)",
        ylabel="Mean round-trip return (bps)",
        title="Executable Hyperliquid touches at 2 s",
    )
    axes[1].legend(frameon=False, fontsize=8, ncols=2)
    fig.suptitle(
        "Direct feed timing and an executable-touch check",
        fontsize=14,
        fontweight="bold",
    )
    save(fig, path)


def matched(data, path):
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 3.8), layout="constrained")
    days = data["results"]
    dates = [r["day"][5:] for r in days]
    x = np.arange(len(dates))
    d = np.array([r["paired_value"]["delta_bps_per_opportunity"] for r in days])
    bounds = np.array([r["paired_value"]["ci95"] for r in days]).T
    axes[0].errorbar(
        x,
        d,
        yerr=np.maximum(0, [d - bounds[0], bounds[1] - d]),
        fmt="o",
        capsize=4,
        color=TEAL,
    )
    axes[0].axhline(0, color=MUTED, lw=0.8)
    axes[0].set(
        ylabel="Change (bps per candidate opportunity)",
        title="Value-selected width minus baseline",
    )
    for name, label, color, off in [
        ("baseline", "Baseline", BLUE, -0.16),
        ("value_widen", "Value-selected width", ORANGE, 0.16),
    ]:
        v = [r[name]["filled_quotes"] for r in days]
        bars = axes[1].bar(x + off, v, width=0.3, label=label, color=color)
        axes[1].bar_label(bars, padding=2, fontsize=8)
    axes[1].set(ylabel="Filled candidate quotes", title="Same eligible times and sides")
    axes[1].legend(frameon=False)
    for ax in axes:
        ax.set_xticks(x, dates)
        ax.set_xlabel("UTC date in 2026")
    fig.suptitle(
        "Matched quote opportunities separate selection from inventory feedback",
        fontsize=13,
        fontweight="bold",
    )
    save(fig, path)


def closed(data, path):
    fig, axes = plt.subplots(
        1, len(data["days"]), figsize=(11, 3.6), sharey=True, layout="constrained"
    )
    for r, ax in zip(data["days"], axes):
        for name, label, color in [
            ("strict", "Strict baseline", BLUE),
            ("bounded_aggregate", "Aggregate", TEAL),
            ("value_widen", "Aggregate + value width", ORANGE),
        ]:
            ax.plot(
                range(25),
                r["scenarios"][name]["hourly_equity"],
                color=color,
                label=label,
            )
        ax.axhline(0, color=MUTED, lw=0.8)
        ax.set(title=r["day"], xlabel="Hour (UTC)")
        ax.set_xticks([0, 6, 12, 18, 24])
    axes[0].set_ylabel("Modeled marked net equity (USDC)")
    axes[0].legend(frameon=False, fontsize=8)
    fig.suptitle(
        "Policy paths diverge once fills and residual limits feed back",
        fontsize=14,
        fontweight="bold",
    )
    save(fig, path)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    setup()
    for name, fn in [
        ("timing", timing),
        ("execution_value", matched),
        ("closed_loop", closed),
    ]:
        path = args.out / f"{name}.json"
        if path.exists():
            fn(json.loads(path.read_text()), args.out / f"{name.replace('_','-')}.png")


if __name__ == "__main__":
    main()
