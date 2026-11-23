"""Capture public feeds on one host and test arrival-time and executable-touch labels."""
import argparse
import asyncio
import gzip
import hashlib
import json
from pathlib import Path
import time

import numpy as np
import websockets

from .evaluate import asof
from .train import Moments, metrics, paired_ci, ridge

ENDPOINTS = {
    "hyperliquid": "wss://api.hyperliquid.xyz/ws",
    "lighter": "wss://mainnet.zklighter.elliot.ai/stream?readonly=true",
}


def parse(venue, data):
    """Normalize the top of book from each of the three public channels."""
    if venue == "hyperliquid":
        channel = data.get("channel")
        if channel not in {"bbo", "l2Book"}:
            return None
        book = data["data"]
        if book["coin"] != "BTC":
            return None
        bid, ask = (
            book["bbo"]
            if channel == "bbo"
            else [side[0] if side else None for side in book["levels"]]
        )
        if bid is None or ask is None:
            return (channel, int(book["time"]) * 1_000, None)
        return (
            channel,
            int(book["time"]) * 1_000,
            (float(bid["px"]), float(ask["px"]), float(bid["sz"]), float(ask["sz"])),
        )
    if data.get("channel") == "ticker:1" and data.get("type") in {
        "update/ticker",
        "subscribed/ticker",
    }:
        tick = data["ticker"]
        bid, ask = tick.get("b"), tick.get("a")
        when = data.get("last_updated_at") or tick.get("last_updated_at")
        when = int(when) if when else int(data["timestamp"]) * 1_000
        levels = (
            None
            if not bid or not ask
            else (
                float(bid["price"]),
                float(ask["price"]),
                float(bid["size"]),
                float(ask["size"]),
            )
        )
        return "ticker", when, levels
    return None


async def capture(path, seconds):
    start = time.monotonic_ns()
    deadline = start + int(seconds * 1e9)
    with path.open("x", buffering=1 << 18) as stream:

        def write(type_, **row):
            stream.write(
                json.dumps({"type": type_, **row}, separators=(",", ":")) + "\n"
            )

        write(
            "start",
            utc_us=time.time_ns() // 1000,
            endpoints=ENDPOINTS,
            duration_s=seconds,
        )

        async def feed(venue):
            subs = (
                [
                    {
                        "method": "subscribe",
                        "subscription": {"type": kind, "coin": "BTC"},
                    }
                    for kind in ("bbo", "l2Book")
                ]
                if venue == "hyperliquid"
                else [{"type": "subscribe", "channel": "ticker/1"}]
            )
            while time.monotonic_ns() < deadline:
                try:
                    async with websockets.connect(
                        ENDPOINTS[venue],
                        open_timeout=7,
                        close_timeout=2,
                        user_agent_header="event-driven-mm-research/1",
                    ) as socket:
                        for sub in subs:
                            await socket.send(json.dumps(sub))
                        current = start
                        while current < deadline:
                            remaining = (deadline - time.monotonic_ns()) / 1e9
                            if remaining <= 0:
                                break
                            try:
                                raw = await asyncio.wait_for(
                                    socket.recv(), timeout=min(25, remaining)
                                )
                            except asyncio.TimeoutError:
                                if time.monotonic_ns() >= deadline:
                                    break
                                await socket.send(
                                    '{"method":"ping"}'
                                    if venue == "hyperliquid"
                                    else '{"type":"ping"}'
                                )
                                continue
                            current = time.monotonic_ns()
                            utc = time.time_ns() // 1000
                            result = parse(venue, json.loads(raw))
                            if result is not None:
                                channel, source, levels = result
                                write(
                                    "quote",
                                    venue=venue,
                                    channel=channel,
                                    at_us=(current - start) // 1000,
                                    utc_us=utc,
                                    source_us=source,
                                    levels=levels,
                                )
                except (
                    OSError,
                    ValueError,
                    KeyError,
                    websockets.WebSocketException,
                ) as exc:
                    write(
                        "disconnect",
                        venue=venue,
                        at_us=(time.monotonic_ns() - start) // 1000,
                        reason=type(exc).__name__,
                    )
                    await asyncio.sleep(1)

        await asyncio.gather(*(feed(venue) for venue in ENDPOINTS))
        write("end", at_us=(time.monotonic_ns() - start) // 1000)


def analyze(path, fee_bps=4.5):
    feeds = {k: [] for k in ("bbo", "l2Book", "ticker")}
    gaps = []
    if path.suffix == ".gz":
        stream = gzip.open(path, "rt")
    else:
        stream = path.open()
    with stream as source:
        start = json.loads(next(source))
        for line in source:
            row = json.loads(line)
            if row["type"] == "quote":
                levels = row["levels"] or [np.nan] * 4
                bid, ask, bs, a_s = levels
                if not (0 < bid < ask and bs > 0 and a_s > 0):
                    levels = [np.nan] * 4
                feeds[row["channel"]].append(
                    [
                        row["at_us"],
                        row["utc_us"] - row["source_us"],
                        *levels,
                        row["source_us"] - start["utc_us"],
                    ]
                )
            elif row["type"] == "disconnect":
                gaps.append(row)
                for channel in (
                    ("bbo", "l2Book") if row["venue"] == "hyperliquid" else ("ticker",)
                ):
                    feeds[channel].append([row["at_us"], *([np.nan] * 5), row["at_us"]])
            elif row["type"] == "end":
                end = row["at_us"]
    if "end" not in locals() or any(len(rows) < 20 for rows in feeds.values()):
        raise ValueError("capture needs a complete session and all three feeds")
    feeds = {k: np.asarray(rows) for k, rows in feeds.items()}
    source_feeds = {k: f[np.argsort(f[:, 6], kind="stable")] for k, f in feeds.items()}
    receipt_wall = np.concatenate(
        [f[np.isfinite(f[:, 1])][:, (0, 1, 6)] for f in feeds.values()]
    )
    receipt_wall = receipt_wall[np.argsort(receipt_wall[:, 0], kind="stable")]
    wall_at_receipt = receipt_wall[:, 1] + receipt_wall[:, 2]

    def to_wall(times):
        return np.interp(times, receipt_wall[:, 0], wall_at_receipt)

    audit = {
        k: {
            "events": int(np.isfinite(f[:, 2]).sum()),
            "apparent_age_ms_p50_p95": np.nanquantile(
                f[:, 1] / 1000, [0.5, 0.95]
            ).tolist(),
            "cadence_ms_p50_p95": (
                np.quantile(np.diff(f[:, 0]), [0.5, 0.95]) / 1000
            ).tolist(),
        }
        for k, f in feeds.items()
    }
    grid = np.arange(5_000_000, end - 5_000_000, 100_000)
    split = end // 2
    result = {
        "start": start,
        "duration_s": end / 1e6,
        "audit": audit,
        "disconnects": gaps,
        "fee_bps_per_leg": fee_bps,
        "scores": [],
        "common_clock_scores": [],
        "paired_executable": [],
        "exchange_paired_executable": [],
    }
    age = 1_000_000
    result["host_wall_minus_monotonic_ms_p05_p50_p95"] = np.nanquantile(
        (wall_at_receipt - receipt_wall[:, 0]) / 1000, [0.05, 0.5, 0.95]
    ).tolist()

    def at(channel, times, exchange=False):
        data, clock = (source_feeds[channel], 6) if exchange else (feeds[channel], 0)
        if exchange:
            times = to_wall(times)
        return np.column_stack(
            [asof(data[:, clock], data[:, i], times, age) for i in range(2, 6)]
        )

    def predict(x, y, train):
        mom = Moments.from_sample(x[train], y[train])
        predictions = {}
        for name, columns in {
            "hl_only": [0, 1, 2],
            "lighter_only": [3, 4, 5],
            "both": list(range(7)),
        }.items():
            coef, inter = ridge(mom, columns, 0.01)
            predictions[name] = x[:, columns] @ coef + inter
        return predictions

    def report_scores(y, predictions, train, test):
        return {
            "train_rows": int(train.sum()),
            "holdout_rows": int(test.sum()),
            "metrics": {k: metrics(y[test], v[test]) for k, v in predictions.items()},
            "both_vs_hl": paired_ci(
                grid[test],
                y[test],
                predictions["both"][test],
                predictions["hl_only"][test],
                60_000_000,
                71,
                2000,
            ),
        }

    def executable(channel, horizon, direction, entry, exit, delay):
        buy = direction > 0
        ep = np.where(buy, entry[:, 1], entry[:, 0])
        xp = np.where(buy, exit[:, 0], exit[:, 1])
        gross = np.where(buy, 1, -1) * (xp - ep) / ep * 10_000
        net = gross - fee_bps * (1 + xp / ep)
        return {
            "channel": channel,
            "horizon_ms": horizon,
            "delay_ms": delay,
            "n": len(direction),
            "gross_bps": float(gross.mean()),
            "net_bps": float(net.mean()),
            "positive_net_fraction": float((net > 0).mean()),
        }

    lighter = at("ticker", grid)
    old_li = at("ticker", grid - 500_000)
    lm = lighter[:, :2].mean(axis=1)
    for channel in ("bbo", "l2Book"):
        hl = at(channel, grid)
        old = at(channel, grid - 500_000)
        hm = hl[:, :2].mean(axis=1)
        basis = (lm - hm) / hm * 10_000
        hlx = np.column_stack(
            [
                (hl[:, 1] - hl[:, 0]) / hm * 10_000,
                (hl[:, 2] - hl[:, 3]) / (hl[:, 2] + hl[:, 3]),
                (hm / old[:, :2].mean(axis=1) - 1) * 10_000,
            ]
        )
        lix = np.column_stack(
            [
                (lighter[:, 1] - lighter[:, 0]) / lm * 10_000,
                (lighter[:, 2] - lighter[:, 3]) / (lighter[:, 2] + lighter[:, 3]),
                (lm / old_li[:, :2].mean(axis=1) - 1) * 10_000,
            ]
        )
        x = np.column_stack([hlx, lix, basis])
        source_mid = at(channel, grid, exchange=True)[:, :2].mean(axis=1)
        for horizon in (500, 2_000):
            horizon_grid = grid + horizon * 1_000
            target = at(channel, horizon_grid)
            y = (target[:, :2].mean(axis=1) / hm - 1) * 10_000
            valid = np.isfinite(x).all(axis=1) & np.isfinite(y)
            tr = valid & (horizon_grid < split)
            te = valid & (grid >= split)
            if min(tr.sum(), te.sum()) < 20:
                continue
            predictions = predict(x, y, tr)
            result["scores"].append(
                {
                    "channel": channel,
                    "horizon_ms": horizon,
                    **report_scores(y, predictions, tr, te),
                }
            )
            # Re-clock the labels only. Features still use what the host had received.
            source_target = at(channel, horizon_grid, exchange=True)
            y_source = (source_target[:, :2].mean(axis=1) / source_mid - 1) * 10_000
            sf = source_feeds[channel]
            known_start = asof(sf[:, 6], sf[:, 0], to_wall(grid), age)
            known_end = asof(sf[:, 6], sf[:, 0], to_wall(horizon_grid), age)
            tr_common = (
                tr
                & np.isfinite(y_source)
                & (np.maximum(known_start, known_end) < split)
            )
            te_common = te & np.isfinite(y_source)
            if min(tr_common.sum(), te_common.sum()) >= 20:
                for clock, labels in (("receive", y), ("exchange", y_source)):
                    result["common_clock_scores"].append(
                        {
                            "channel": channel,
                            "horizon_ms": horizon,
                            "clock": clock,
                            **report_scores(
                                labels,
                                predict(x, labels, tr_common),
                                tr_common,
                                te_common,
                            ),
                        }
                    )
            pred = predictions["both"]
            threshold = np.quantile(np.abs(pred[tr]), 0.9)
            delays = (0, 25, 100, 250, 500) if horizon > 500 else (0, 25, 100, 250)
            entries = {delay: at(channel, grid + delay * 1000) for delay in delays}
            exchange_entries = {
                delay: at(channel, grid + delay * 1000, exchange=True)
                for delay in delays
            }
            common = te_common & (np.abs(pred) >= threshold)
            for entry in (*entries.values(), *exchange_entries.values()):
                common &= np.isfinite(entry).all(axis=1)
            if common.any():
                for delay in delays:
                    for key, entry, exit in (
                        ("paired_executable", entries[delay], target),
                        (
                            "exchange_paired_executable",
                            exchange_entries[delay],
                            source_target,
                        ),
                    ):
                        result[key].append(
                            executable(
                                channel,
                                horizon,
                                pred[common],
                                entry[common],
                                exit[common],
                                delay,
                            )
                        )
    with path.open("rb") as sample:
        result["capture_sha256"] = hashlib.file_digest(sample, "sha256").hexdigest()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    cap = commands.add_parser("capture")
    cap.add_argument("--out", type=Path, required=True)
    cap.add_argument("--seconds", type=float, required=True)
    run = commands.add_parser("analyze")
    run.add_argument("--log", type=Path, required=True)
    run.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "capture":
        if args.seconds <= 0:
            parser.error("--seconds must be positive")
        asyncio.run(capture(args.out, args.seconds))
    else:
        args.out.write_text(
            json.dumps(analyze(args.log), indent=2, allow_nan=False) + "\n"
        )


if __name__ == "__main__":
    main()
