"""Run the frozen quote-filter experiment and reconcile fill and hourly PnL traces."""
import argparse
import json
from pathlib import Path
import subprocess

import numpy as np
import pandas as pd

from .archive import FEES
from .features import DAY_US, read_source, start_us, study_config, valid_book


def make_model(threshold, study):
    rows = []
    for suffix, ahead, delay in [("base", "0", 12000), ("queue", "0.5", 12000), ("slow", "0", 150000)]:
        for filter_on in (False, True):
            row = dict(name=("filtered_" if filter_on else "baseline_") + suffix,
                       hedge={"kind": "ioc"}, outbound_us=delay, inbound_us=delay, jitter_us=2000,
                       cancel_ahead=ahead, extra_taker_fee_bps="0", respect_trade_time=True)
            if filter_on:
                row["quote_signal"] = {"threshold_bps": f"{threshold:.9f}", "max_age_ms": study["signal_max_age_ms"]}
            rows.append(row)
    return {"fees_bps": FEES, "scenarios": rows}


def market_mid(raw, day, venue):
    book = read_source(raw, day, venue, "book_snapshot_5")
    times = book["local_timestamp"].to_numpy() - start_us(day)
    mid = ((book["asks[0].price"] + book["bids[0].price"]) / 2).to_numpy()
    mid[~valid_book(book).to_numpy()] = np.nan
    return times, mid


def asof(times, values, targets, max_age=None):
    i = np.searchsorted(times, targets, side="right") - 1
    idx = np.maximum(i, 0)
    valid = i >= 0
    if max_age is not None:
        valid &= targets - times[idx] <= max_age
    return np.where(valid, values[idx], np.nan)


def read_trace(path, signal):
    positions = {}
    fills = {}
    for line in path.open():
        row = json.loads(line)
        name, at, kind, data = (row[x] for x in ("scenario", "at_us", "event", "data"))
        if name == "observed":
            continue
        if kind == "intent" and data["venue"] == "lighter" and data["action"] == "place":
            positions[(name, data["client"])] = {
                "name": name, "at": at, "client": data["client"], "side": data["side"],
                "requested_size": float(data["size"]), "filled": 0., "terminal": False,
            }
        elif kind == "order_update" and data["venue"] == "lighter" and data["status"] == "terminal":
            key = name, data["client"]
            if key in positions:
                positions[key]["terminal"] = True
        elif kind == "fill":
            fill = data["fill"]
            qty, price, fee = (float(fill[k]) for k in ("size", "price", "fee"))
            side = 1 if fill["side"] == "buy" else -1
            fills.setdefault(name, []).append((at, data["venue"], side, qty, price, fee, fill["client"]))
            if data["venue"] == "lighter":
                positions[(name, fill["client"])]["filled"] += qty
    frame = pd.DataFrame(positions.values())
    if not frame.empty and signal is not None:
        forecasts = asof(signal["available_us"].to_numpy(), signal["prediction_bps"].to_numpy(),
                        frame["at"].to_numpy(), study_config()["signal_max_age_ms"] * 1000)
        frame["adverse"] = np.where(frame["side"] == "buy", -forecasts, forecasts)
    return frame, fills


def health(frame):
    value = {"orders": len(frame), "filled_orders": int((frame["filled"] > 0).sum()) if len(frame) else 0}
    value["fill_rate"] = value["filled_orders"] / len(frame) if len(frame) else None
    value["unresolved"] = int((~frame["terminal"]).sum()) if len(frame) else 0
    return value


def hourly_equity(fills, books):
    hours = np.arange(25, dtype=np.int64) * 3_600_000_000
    results = np.zeros(len(hours))
    for venue, (time, mid) in books.items():
        events = sorted((e for e in fills if e[1] == venue), key=lambda e: e[0])
        if not events:
            continue
        ts = np.asarray([e[0] for e in events], dtype=np.int64)
        signed = np.asarray([e[2] * e[3] for e in events])
        cash = [-change * e[4] - e[5] for change, e in zip(signed, events)]
        n = np.searchsorted(ts, hours, side="right")
        position = np.r_[0., np.cumsum(signed)][n]
        valid = np.isfinite(mid)
        mark = asof(time[valid], mid[valid], hours)
        results += np.where(np.abs(position) < 1e-12, 0, position * mark) + np.r_[0., np.cumsum(cash)][n]
    return results


def cohort_markouts(fills, book, horizons, age_ms):
    events = [fill for fill in fills if fill[1] == "lighter"]
    if not events:
        return {}
    time, mid = book
    at, side, qty = (np.asarray([e[i] for e in events]) for i in [0, 2, 3])
    int_at = at.astype(np.int64)
    mark = asof(time, mid, int_at, age_ms * 1000)
    results = {}
    for h in horizons:
        later = asof(time, mid, int_at + h * 1000, age_ms * 1000)
        values = side * (later - mark) / mark * 10_000
        good = np.isfinite(values)
        if good.any():
            results[str(h)] = {"matched": int(good.sum()), "all_fills": len(events),
                "quantity_weighted_bps": float(np.average(values[good], weights=qty[good])),
                "adverse_share": float((values[good] < 0).mean())}
    return results


def interval(delta, seed):
    # Fixed non-overlapping two-hour time blocks inside the simulated day.
    blocks = np.diff(delta).reshape(-1, 2).sum(axis=1)
    rng = np.random.default_rng(seed)
    ids = rng.integers(0, len(blocks), (2000, len(blocks)))
    return np.quantile(blocks[ids].sum(axis=1), [.025, .975]).tolist()


def summarize_day(output, raw, models, day, threshold, study):
    report = json.loads((output / f"{day}_simulation.json").read_text())
    signal = pd.read_csv(models / f"{day}_signal.csv")
    frame, fills = read_trace(output / f"{day}_trace.jsonl", signal)
    books = {venue: market_mid(raw, day, venue) for venue in ["hyperliquid", "lighter"]}
    data = {"day": day, "scenarios": {}, "paired": {}}
    for row in report["scenarios"]:
        name = row["scenario"]["name"]
        orders = frame[frame["name"] == name] if not frame.empty else frame
        equity = hourly_equity(fills.get(name, []), books)
        if not np.isclose(equity[-1], float(row["metrics"]["net_pnl"]), atol=1e-6):
            raise ValueError(f"hourly ledger does not reconcile with Rust for {day} {name}")
        result = health(orders)
        result.update(metrics=row["metrics"], opportunities=row["quote_opportunities"],
                      equity=equity.tolist(), markouts=cohort_markouts(fills.get(name, []), books["hyperliquid"],
                          study["horizons_ms"], study["fresh_ms"]))
        if name.startswith("baseline_") and len(orders):
            cohorts = {
                "flagged": orders[orders.adverse >= threshold],
                "unflagged": orders[orders.adverse.notna() & (orders.adverse < threshold)],
                "missing_signal": orders[orders.adverse.isna()],
            }
            result["decision_cohort"] = {}
            for label, selection in cohorts.items():
                clients = set(selection.client)
                group = [e for e in fills.get(name, []) if e[1] == "lighter" and e[6] in clients]
                result["decision_cohort"][label] = health(selection) | {
                    "markouts": cohort_markouts(group, books["hyperliquid"], study["horizons_ms"], study["fresh_ms"])
                }
        data["scenarios"][name] = result
    for suffix in ("base", "queue", "slow"):
        a, b = (data["scenarios"][f"{name}_{suffix}"] for name in ("baseline", "filtered"))
        delta = np.array(b["equity"]) - np.array(a["equity"])
        data["paired"][suffix] = {"net_pnl_delta": float(delta[-1]),
            "net_pnl_delta_ci95": interval(delta, study["seed"]),
            "filled_orders_delta": b["filled_orders"] - a["filled_orders"]}
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", required=True, type=Path)
    parser.add_argument("--models", required=True, type=Path)
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--binary", type=Path, default=Path("target/debug/event-driven-mm"))
    parser.add_argument("--days", nargs="+", default=["2026-10-01", "2026-11-01"])
    parser.add_argument("--summarize-only", action="store_true")
    args = parser.parse_args()
    study = study_config()
    threshold = json.loads((args.models / "forecast_results.json").read_text())["threshold_bps"]
    args.out.mkdir(parents=True, exist_ok=True)
    model = args.out / "quote_filter_model.json"
    model.write_text(json.dumps(make_model(threshold, study), indent=2) + "\n")
    datasets = []
    for day in args.days:
        if not args.summarize_only:
            command = [str(args.binary), "simulate", "--mode", "closed-loop", "--seed", str(study["seed"]),
                       "--log", str(args.work / f"{day}.jsonl"), "--config", str(model),
                       "--quote-signal", str(args.models / f"{day}_signal.csv"),
                       "--trace", str(args.out / f"{day}_trace.jsonl")]
            with (args.out / f"{day}_simulation.json").open("w") as out:
                subprocess.run(command, check=True, stdout=out)
        result = summarize_day(args.out, args.raw, args.models, day, threshold, study)
        datasets.append(result)
        print(day, result["paired"], flush=True)
    (args.out / "execution_results.json").write_text(json.dumps({"threshold_bps": threshold, "days": datasets}, indent=2) + "\n")


if __name__ == "__main__":
    main()
