"""Label matched quote opportunities and fit a walk-forward two-part execution model."""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from numba import njit
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import average_precision_score
from sklearn.preprocessing import StandardScaler

from .archive import FEES, MAKER, levels
from .features import (
    DAY_US,
    VENUES,
    book_features,
    download,
    lag_stats,
    read_source,
    source,
    start_us,
    study_config,
    trade_features,
    url,
)

DAYS = [f"2026-{month:02d}-01" for month in range(5, 12)]
OFFSETS = np.array([0.0, 0.25, 0.5])
STEP_US = 5_000_000
HOLD_US = 2_000_000
DELAY_US = 12_000
FRESH_US = 750_000
QTY = float(MAKER["size"])
MAKER_FEE = float(FEES["lighter_maker"]) / 10_000
TAKER_FEE = float(FEES["hyperliquid_taker"]) / 10_000


@njit(cache=True)
def fill_path(
    decision,
    price,
    side,
    quote,
    delay,
    btime,
    bid,
    ask,
    trades,
    occurred,
    px,
    qty,
    buy,
    queue_credit,
):
    """Receipt-time matching with an exchange timestamp eligibility guard."""
    arrival = decision + delay
    book_idx = np.searchsorted(btime, arrival, side="right") - 1
    if book_idx < 0 or arrival - btime[book_idx] > FRESH_US:
        return 0.0, 0
    if not (0 < bid[book_idx, 0] < ask[book_idx, 0]):
        return 0.0, 0
    if (
        side == 1
        and price >= ask[book_idx, 0]
        or side == -1
        and price <= bid[book_idx, 0]
    ):
        return 0.0, 0
    depth = bid if side == 1 else ask
    ahead = 0.0
    deepest = depth[book_idx, 8]
    if not np.isfinite(deepest) or side * (price - deepest) < -1e-6:
        ahead = np.inf
    for level in range(0, 10, 2):
        if abs(depth[book_idx, level] - price) < 1e-6:
            ahead = depth[book_idx, level + 1] * queue_credit
            break
    start = np.searchsorted(trades, arrival, side="right")
    end = np.searchsorted(trades, arrival + HOLD_US, side="right")
    total, last = 0.0, 0
    for j in range(start, end):
        if occurred[j] <= arrival or buy[j] == (side == 1):
            continue
        through = side * (price - px[j])
        if through < -1e-6:
            continue
        if through > 1e-6:
            ahead = 0.0
        remaining_print = max(qty[j] - ahead, 0.0)
        ahead = max(ahead - qty[j], 0.0)
        if remaining_print > 0:
            quantity = (
                np.floor(min(remaining_print, quote - total) * 100_000 + 1e-7) / 100_000
            )
            if quantity > 0:
                total += quantity
                last = trades[j]
        if total >= quote - 1e-9:
            break
    return total, last


@njit(cache=True)
def price_mark(when, times, bid, ask, side):
    i = np.searchsorted(times, when, side="right") - 1
    if i < 0 or when - times[i] > FRESH_US or not (0 < bid[i, 0] < ask[i, 0]):
        return np.nan
    return bid[i, 0] if side == 1 else ask[i, 0]


@njit(cache=True)
def hedge_label(quantity, when, side, times, bid, ask):
    i = np.searchsorted(times, when, side="right") - 1
    if i < 0 or when - times[i] > FRESH_US or not (0 < bid[i, 0] < ask[i, 0]):
        return 0.0, 0.0
    depth = bid if side == 1 else ask
    touch = depth[i, 0]
    if quantity * touch < 10.0:
        return 0.0, 0.0
    limit = touch * (1 - side * 0.0001)
    fill, cash = 0.0, 0.0
    for j in range(0, 10, 2):
        p, q = depth[i, j], depth[i, j + 1]
        if not (p > 0 and q > 0 and side * (p - limit) >= 0):
            break
        amount = min(q, quantity - fill)
        cash += amount * p
        fill += amount
        if fill >= quantity - 1e-9:
            break
    if cash < 10.0:
        return 0.0, 0.0
    return fill, cash


@njit(cache=True)
def labels(
    grid,
    prices,
    sides,
    queue,
    delay,
    hl_time,
    hl_bid,
    hl_ask,
    li_time,
    li_bid,
    li_ask,
    tr_time,
    tr_when,
    tr_px,
    tr_qty,
    tr_buy,
):
    n = len(grid)
    out = np.zeros((n, 4))
    for i in range(n):
        at, p, side = grid[i], prices[i], sides[i]
        qty, last = fill_path(
            at,
            p,
            side,
            QTY,
            delay,
            li_time,
            li_bid,
            li_ask,
            tr_time,
            tr_when,
            tr_px,
            tr_qty,
            tr_buy,
            queue,
        )
        out[i, 0] = qty
        if qty == 0:
            continue
        hedged, cash = hedge_label(qty, last + 2 * delay, side, hl_time, hl_bid, hl_ask)
        residual = max(qty - hedged, 0.0)
        terminal = price_mark(at + HOLD_US + 3 * delay, hl_time, hl_bid, hl_ask, side)
        if residual > 1e-9 and not np.isfinite(terminal):
            out[i, 1:] = np.nan
            continue
        marked = residual * terminal if residual > 1e-9 else 0.0
        out[i, 1] = (
            (
                side * (cash + marked - p * qty)
                - TAKER_FEE * (cash + marked)
                - MAKER_FEE * p * qty
            )
            / (QTY * p)
            * 10_000
        )
        out[i, 2] = hedged
        out[i, 3] = residual
    return out


def label_day(raw, day, config):
    start = start_us(day)
    t = np.arange(
        start + 10_000_000, start + DAY_US - 10_000_000, STEP_US, dtype=np.int64
    )
    books, prints, values, features, audit = {}, {}, {}, {}, {}
    good = np.ones(len(t), dtype=bool)
    for venue in VENUES:
        book = read_source(raw, day, venue, "book_snapshot_5")
        trade = read_source(raw, day, venue, "trades")
        values[venue], vg, _ = book_features(book, t, venue, "receive", config)
        good &= vg
        values[venue].update(trade_features(trade, t, venue, "receive", config))
        books[venue] = (
            book["local_timestamp"].to_numpy(),
            levels(book, "bids"),
            levels(book, "asks"),
        )
        prints[venue] = trade
        audit[venue] = {
            k: lag_stats(d) for k, d in (("books", book), ("trades", trade))
        }
    ht, hb, ha = books["hyperliquid"]
    lt, lb, la = books["lighter"]
    hi, li = np.maximum(np.searchsorted(ht, t, side="right") - 1, 0), np.maximum(
        np.searchsorted(lt, t, side="right") - 1, 0
    )
    hm = values["hyperliquid"]["mid"]
    lm = values["lighter"]["mid"]
    trade = prints["lighter"]
    arrays = (
        *books["hyperliquid"],
        *books["lighter"],
        trade["local_timestamp"].to_numpy(),
        trade["timestamp"].to_numpy(),
        trade.price.to_numpy(),
        trade.amount.to_numpy(),
        trade.side.eq("buy").to_numpy(),
    )
    rows = []
    for side in (1, -1):
        hv = hb if side == 1 else ha
        # Tight executable-depth approximation is a cohort construction rule.
        # The Rust simulator independently prices each live candidate at depth.
        hv1 = hv[hi, 0]
        left = np.full(len(hi), QTY)
        sweep_value = np.zeros(len(hi))
        for level in range(0, 10, 2):
            px, qty = hv[hi, level], hv[hi, level + 1]
            valid = (
                (px > 0) & (qty > 0) & (side * (px - hv1 * (1 - side * 0.0001)) >= 0)
            )
            take = np.where(valid, np.minimum(left, qty), 0.0)
            sweep_value += np.where(valid, take * px, 0.0)
            left -= take
        h = sweep_value / QTY
        edge_price = h * (1 - side * (TAKER_FEE + 0.00005 + 0.0001))
        lo = np.minimum(lm * (1 - 0.000025), edge_price)
        up = np.maximum(lm * (1 + 0.000025), edge_price)
        base = (
            np.minimum(lo, la[li, 0] - 0.1)
            if side == 1
            else np.maximum(up, lb[li, 0] + 0.1)
        )
        valid = good & (left < 1e-8) & np.isfinite(base) & (base > 0)
        for venue in VENUES:
            prefix = "hl" if venue == "hyperliquid" else "li"
            for name in (
                "spread",
                "imbalance_1",
                "imbalance_5",
                "micro",
                "log_depth",
                "return_250",
                "return_1000",
                "return_5000",
                "flow_250",
                "flow_1000",
                "flow_5000",
                "log_volume_1000",
            ):
                directional = name.startswith(("imbalance", "micro", "return", "flow"))
                features[prefix + "_" + name] = values[venue][name] * (
                    side if directional else 1.0
                )
        features["basis"] = side * (lm / hm - 1) * 10_000
        depth = lb if side == 1 else la
        features["side"] = np.full(len(t), side)
        features["touch_depth"] = np.log1p(depth[li, 1])
        valid &= np.isfinite(np.column_stack(list(features.values()))).all(axis=1)
        for offset in OFFSETS:
            p = base * (1 - side * offset / 10_000)
            p = side * np.floor(side * p * 10 + 1e-7) / 10
            labels_ = labels(
                t[valid], p[valid], np.full(valid.sum(), side), 1.0, DELAY_US, *arrays
            )
            rows.append(
                pd.DataFrame(
                    {
                        "time_us": t[valid] - start,
                        "side": side,
                        "offset": offset,
                        "cross_edge": side * (h[valid] - p[valid]) / p[valid] * 10_000,
                        "distance": side * (lm[valid] - p[valid]) / lm[valid] * 10_000,
                        **{
                            key: col[valid]
                            for key, col in features.items()
                            if key != "side"
                        },
                        "filled": labels_[:, 0],
                        "net_bps": labels_[:, 1],
                        "hedged": labels_[:, 2],
                        "residual": labels_[:, 3],
                    }
                )
            )
    return pd.concat(rows, ignore_index=True), audit


def predict(training, target, feature_cols):
    xs = StandardScaler()
    x = xs.fit_transform(training[feature_cols])
    xt = xs.transform(target[feature_cols])
    filled = training.filled.to_numpy() > 0
    n = int(filled.sum())
    if n < 5 or filled.all():
        raise ValueError(f"need at least five fills and negative cases, got {n}")
    clf = LogisticRegression(C=0.1, max_iter=500).fit(x, filled)
    clean = filled & training.net_bps.notna().to_numpy()
    if clean.sum() < 5:
        raise ValueError("fewer than five fully labelled training fills")
    reg = Ridge(alpha=20).fit(x[clean], training.net_bps.to_numpy()[clean])
    lo, hi = np.quantile(training.net_bps.to_numpy()[clean], [0.01, 0.99])
    cond = np.clip(reg.predict(xt), lo, hi)
    result = target.copy()
    result["probability"] = clf.predict_proba(xt)[:, 1]
    result["conditional_net_bps"] = cond
    result["expected_value"] = result.probability * result.conditional_net_bps
    return result, {
        "training_candidate_fills": n,
        "training_conditional_fills": int(clean.sum()),
        "training_probability": float(
            training.loc[training.offset.eq(0), "filled"].gt(0).mean()
        ),
    }


def outcomes(rows):
    filled = rows.filled.gt(0)
    return {
        "opportunities": len(rows),
        "filled_quotes": int(filled.sum()),
        "labelled_rows": int(rows.net_bps.notna().sum()),
        "net_bps_per_opportunity": float(rows.net_bps.mean()),
        "net_bps_per_fill": float(rows.loc[filled, "net_bps"].mean())
        if rows.loc[filled, "net_bps"].notna().any()
        else None,
        "filled_quantity": float(rows.filled.sum()),
        "hedged_quantity": float(rows.hedged.sum()),
        "residual_quantity": float(rows.residual.sum()),
    }


def paired_value(base, candidate):
    keys = ["time_us", "side"]
    paired = base[keys + ["net_bps"]].merge(
        candidate[keys + ["net_bps"]],
        on=keys,
        validate="one_to_one",
        suffixes=("_base", "_candidate"),
    )
    delta = paired.net_bps_candidate - paired.net_bps_base
    blocks = delta.groupby(paired.time_us // 1_800_000_000)
    sums = blocks.sum().to_numpy()
    sizes = blocks.size().to_numpy()
    draw = np.random.default_rng(71).integers(0, len(sums), (2000, len(sums)))
    ratios = sums[draw].sum(axis=1) / sizes[draw].sum(axis=1)
    return {
        "delta_bps_per_opportunity": float(delta.mean()),
        "blocks": len(sums),
        "ci95": np.quantile(ratios, [0.025, 0.975]).tolist(),
    }


def walk(work, out):
    frames = []
    rows = []
    columns = None
    for idx, day in enumerate(DAYS):
        current = pd.read_parquet(work / f"{day}_labels.parquet")
        if columns is None:
            columns = [
                col
                for col in current
                if col not in ("time_us", "filled", "net_bps", "hedged", "residual")
            ]
        if idx >= 3:
            labelled, model = predict(
                pd.concat(frames, ignore_index=True), current, columns
            )
            group = ["time_us", "side"]
            # Identical eligible candidate groups. Retain uncertain economics for the
            # probability fit, and drop unknown PnL symmetrically for value comparisons.
            eligible = (
                labelled.groupby(group).net_bps.transform("count").eq(len(OFFSETS))
            )
            scored = labelled.loc[eligible]
            chosen = scored.sort_values(
                ["expected_value", "offset"], ascending=[False, True]
            ).drop_duplicates(group)
            base = scored[scored.offset == 0.0]
            baseline = labelled[labelled.offset == 0.0]
            y = baseline.filled.gt(0).to_numpy()
            probs = baseline.probability.to_numpy()
            prior = model["training_probability"]
            eval_ = {
                "day": day,
                "train_days": DAYS[:idx],
                **model,
                "baseline_ap": float(average_precision_score(y, probs)),
                "baseline_prevalence": float(y.mean()),
                "baseline_brier": float(np.mean((y - probs) ** 2)),
                "constant_brier": float(np.mean((y - prior) ** 2)),
                "baseline": outcomes(base),
                "value_widen": outcomes(chosen),
                "paired_value": paired_value(base, chosen),
                "fixed_widen": {
                    str(x): outcomes(scored[scored.offset == x]) for x in OFFSETS[1:]
                },
                "chosen_offsets": {
                    str(x): int((chosen.offset == x).sum()) for x in OFFSETS
                },
                "unknown_economics_groups": int(len(baseline) - len(base)),
                "probability_calibration": [
                    {
                        "cases": len(indices),
                        "forecast": float(probs[indices].mean()),
                        "event_rate": float(y[indices].mean()),
                    }
                    for indices in np.array_split(np.argsort(probs), 5)
                ],
            }
            # The action stream uses scores for every candidate group, including missing future labels.
            actions = labelled.sort_values(
                ["expected_value", "offset"], ascending=[False, True]
            ).drop_duplicates(group)
            action_grid = (
                actions.pivot(index="time_us", columns="side", values="offset")
                .reindex(columns=[1, -1])
                .fillna(0)
            )
            action_grid.columns = ["bid_widen_bps", "ask_widen_bps"]
            action_grid.to_csv(
                out / f"{day}_widen.csv",
                index_label="available_us",
                float_format="%.6f",
            )
            rows.append(eval_)
            print(day, eval_["baseline"], eval_["value_widen"], flush=True)
        if idx < len(DAYS) - 1:
            frames.append(current)
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--raw", required=True, type=Path)
    ap.add_argument("--work", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--download", action="store_true")
    ap.add_argument("--reuse-labels", action="store_true")
    ap.add_argument(
        "--verify-manifest", type=Path, help="Check archived source SHA-256 hashes"
    )
    args = ap.parse_args()
    args.work.mkdir(parents=True, exist_ok=True)
    args.out.mkdir(parents=True, exist_ok=True)
    config = study_config() | {"days": DAYS}
    if args.download:
        download(args.raw, config)
    expected = (
        json.loads(args.verify_manifest.read_text())["sources"]
        if args.verify_manifest
        else None
    )
    manifest = {}
    for day in DAYS:
        for venue in VENUES:
            for kind in ("book_snapshot_5", "trades"):
                file = source(args.raw, day, venue, kind)
                with file.open("rb") as stream:
                    digest = hashlib.file_digest(stream, "sha256").hexdigest()
                if (
                    expected is not None
                    and expected.get(file.name, {}).get("sha256") != digest
                ):
                    raise ValueError(f"archive source SHA-256 mismatch for {file.name}")
                manifest[file.name] = {
                    "sha256": digest,
                    "bytes": file.stat().st_size,
                    "url": url(config, day, venue, kind),
                }

    audit = {}
    for day in DAYS:
        label_file = args.work / f"{day}_labels.parquet"
        if not args.reuse_labels:
            labelled, summary = label_day(args.raw, day, config)
            labelled.to_parquet(label_file, index=False)
            print(
                "labels",
                day,
                len(labelled),
                int(labelled.filled.gt(0).sum()),
                flush=True,
            )
        else:
            summary = {
                venue: {
                    key: lag_stats(read_source(args.raw, day, venue, kind))
                    for key, kind in [
                        ("books", "book_snapshot_5"),
                        ("trades", "trades"),
                    ]
                }
                for venue in VENUES
            }
        audit[day] = summary
    result = {
        "days": DAYS,
        "opportunity_step_us": STEP_US,
        "hold_us": HOLD_US,
        "one_way_us": DELAY_US,
        "quantity": QTY,
        "candidate_widen_bps": OFFSETS.tolist(),
        "results": walk(args.work, args.out),
        "data": audit,
        "sources": manifest,
    }
    (args.out / "execution_value.json").write_text(
        json.dumps(result, indent=2, allow_nan=False) + "\n"
    )


if __name__ == "__main__":
    main()
