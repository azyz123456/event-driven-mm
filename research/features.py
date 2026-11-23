"""Download source data and build as-of features on a fixed UTC grid."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import shutil
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd

VENUES = ("hyperliquid", "lighter")
BOOK_COLUMNS = [
    f"{side}[{level}].{field}"
    for level in range(5)
    for side in ("asks", "bids")
    for field in ("price", "amount")
]
DAY_US = 86_400_000_000


def study_config():
    return json.loads(Path(__file__).with_name("study.json").read_text())


def start_us(day):
    return int(pd.Timestamp(day, tz="UTC").value // 1_000)


def source(raw, day, venue, kind):
    return raw / f"{day}_{venue}_{kind}.csv.gz"


def url(config, day, venue, kind):
    return (
        f"https://datasets.tardis.dev/v1/{venue}/{kind}/{day.replace('-', '/')}/"
        f"{config['symbols'][venue]}.csv.gz"
    )


def download(raw, config):
    raw.mkdir(parents=True, exist_ok=True)

    def copy(spec):
        day, venue, kind = spec
        path = source(raw, day, venue, kind)
        if path.exists():
            return
        tmp = path.with_suffix(".part")
        try:
            with urlopen(Request(url(config, *spec), headers={"User-Agent": "event-driven-mm-research/1"}),
                         timeout=120) as response, tmp.open("wb") as dest:
                shutil.copyfileobj(response, dest, length=1024 * 1024)
            tmp.replace(path)
        finally:
            tmp.unlink(missing_ok=True)

    specs = [(d, v, k) for d in config["days"] for v in VENUES
             for k in ("book_snapshot_5", "trades")]
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(copy, specs))


def valid_book(frame):
    ask, bid = frame["asks[0].price"], frame["bids[0].price"]
    ask_size, bid_size = frame["asks[0].amount"], frame["bids[0].amount"]
    return ((bid > 0) & (bid < ask) & (ask_size > 0) & (bid_size > 0)
            & np.isfinite(ask) & np.isfinite(bid) & np.isfinite(ask_size) & np.isfinite(bid_size))


def read_source(raw, day, venue, kind):
    path = source(raw, day, venue, kind)
    details = BOOK_COLUMNS if kind == "book_snapshot_5" else ["id", "side", "price", "amount"]
    types = {"symbol": "string"}
    if kind == "trades":
        types["id"] = "string"
    frame = pd.read_csv(path, usecols=["exchange", "symbol", "timestamp", "local_timestamp", *details], dtype=types)
    symbol = study_config()["symbols"][venue]
    if len(frame) < 2 or not ((frame["exchange"] == venue) & (frame["symbol"] == symbol)).all():
        raise ValueError(f"unexpected or empty market data in {path}")
    frame = frame.drop(columns=["exchange", "symbol"])

    received = frame["local_timestamp"]
    start = start_us(day)
    if not received.is_monotonic_increasing or not ((received >= start) & (received < start + DAY_US)).all():
        raise ValueError(f"unexpected receipt ordering or date in {path}")
    if kind == "trades":
        valid = ((frame["price"] > 0) & (frame["amount"] > 0)
                 & np.isfinite(frame["price"]) & np.isfinite(frame["amount"])
                 & frame["side"].isin(["buy", "sell"]) & frame["id"].notna())
        frame = frame.loc[valid].drop_duplicates("id", keep="first")
    return frame


def lag_stats(frame):
    lag_ms = (frame["local_timestamp"] - frame["timestamp"]).to_numpy() / 1_000
    gaps = np.diff(frame["local_timestamp"].to_numpy()) / 1_000
    return {
        "rows": len(frame),
        "timestamp_lag_ms": np.quantile(lag_ms, [.01, .50, .95, .99]).round(3).tolist(),
        "gap_ms": np.quantile(gaps, [.50, .95, .99, 1]).round(3).tolist(),
        "negative_lag": int((lag_ms < 0).sum()),
    }


def availability(frame, venue, variant, config):
    if variant == "exchange_clock":
        return frame["timestamp"].to_numpy() + config["exchange_clock_delay_ms"] * 1_000
    delay = int(variant.removeprefix("lighter_delay_")) if variant.startswith("lighter_delay_") else 0
    return frame["local_timestamp"].to_numpy() + (delay * 1_000 if venue == "lighter" else 0)


def book_features(books, grid, venue, variant, config):
    raw_times = availability(books, venue, variant, config)
    order = np.argsort(raw_times, kind="stable")
    times = raw_times[order]
    books = books.iloc[order]
    valid = valid_book(books).to_numpy()
    # Invalid states and feed holes start a new segment. Align on all rows so an
    # invalid book cannot silently carry a formerly valid state forward.
    breaks = np.r_[True, (np.diff(times) > config["gap_ms"] * 1_000) | ~valid[1:] | ~valid[:-1]]
    segments = np.cumsum(breaks)
    bid, ask = (books[f"{side}[0].price"].to_numpy() for side in ("bids", "asks"))
    bid_size, ask_size = (books[f"{side}[0].amount"].to_numpy() for side in ("bids", "asks"))
    mid = (bid + ask) / 2
    micro = (ask * bid_size + bid * ask_size) / (bid_size + ask_size)
    amounts = {
        side: books[[f"{side}[{i}].amount" for i in range(5)]].fillna(0).sum(axis=1).to_numpy()
        for side in ("bids", "asks")
    }

    def at(points):
        idx = np.searchsorted(times, points, side="right") - 1
        rows = np.maximum(idx, 0)
        age = points - times[rows]
        fresh = (idx >= 0) & valid[rows] & (age <= config["fresh_ms"] * 1_000)
        return rows, fresh

    idx, good = at(grid)
    output = {
        "mid": mid[idx],
        "age_ms": (grid - times[idx]) / 1_000,
        "spread": (ask[idx] - bid[idx]) / mid[idx] * 10_000,
        "imbalance_1": (bid_size[idx] - ask_size[idx]) / (bid_size[idx] + ask_size[idx]),
        "imbalance_5": (amounts["bids"][idx] - amounts["asks"][idx])
        / (amounts["bids"][idx] + amounts["asks"][idx]),
        "micro": (micro[idx] - mid[idx]) / mid[idx] * 10_000,
        "log_depth": np.log1p(amounts["bids"][idx] + amounts["asks"][idx]),
    }
    core_fresh = good.copy()
    for window in config["windows_ms"]:
        old, fresh = at(grid - window * 1_000)
        output[f"return_{window}"] = np.log(mid[idx] / mid[old]) * 10_000
        good &= fresh & (segments[idx] == segments[old])
    if venue == "hyperliquid":
        for horizon in config["horizons_ms"]:
            end, fresh = at(grid + horizon * 1_000)
            label_ok = core_fresh & fresh & (segments[idx] == segments[end])
            output[f"target_{horizon}"] = np.where(label_ok, np.log(mid[end] / mid[idx]) * 10_000, np.nan)
    return output, good, core_fresh


def trade_features(trades, grid, venue, variant, config):
    raw_times = availability(trades, venue, variant, config)
    order = np.argsort(raw_times, kind="stable")
    times = raw_times[order]
    trades = trades.iloc[order]
    quote = (trades["price"] * trades["amount"]).to_numpy()
    signed = np.where(trades["side"].to_numpy() == "buy", quote, -quote)
    volume = np.r_[0., np.cumsum(quote)]
    flow = np.r_[0., np.cumsum(signed)]
    end = np.searchsorted(times, grid, side="right")
    out = {}
    for window in config["windows_ms"]:
        start = np.searchsorted(times, grid - window * 1_000, side="right")
        amount = volume[end] - volume[start]
        signed_amount = flow[end] - flow[start]
        out[f"flow_{window}"] = np.divide(signed_amount, amount, out=np.zeros(len(grid)), where=amount > 0)
        out[f"log_volume_{window}"] = np.log1p(amount)
    return out


def prepare_day(raw, work, day, variant, config, record_manifest):
    start = start_us(day)
    grid = np.arange(start, start + DAY_US, config["step_ms"] * 1_000, dtype=np.int64)
    data = {"at_us": grid}
    valid = np.ones(len(grid), dtype=bool)
    audit = {"day": day, "variant": variant, "grid_rows": len(grid), "venues": {}, "sources": {}}
    for venue in VENUES:
        book = read_source(raw, day, venue, "book_snapshot_5")
        trade = read_source(raw, day, venue, "trades")
        prefix = "hl" if venue == "hyperliquid" else "li"
        book_cols, good, fresh = book_features(book, grid, venue, variant, config)
        valid &= good
        flow = trade_features(trade, grid, venue, variant, config)
        for name, column in (book_cols | flow).items():
            data[name if name.startswith("target_") else f"{prefix}_{name}"] = column
        audit["venues"][venue] = {
            "fresh_grid_fraction": float(fresh.mean()),
            "invalid_books": int((~valid_book(book)).sum()),
            "books": lag_stats(book),
            "trades": lag_stats(trade),
        }
        if record_manifest:
            for kind in ("book_snapshot_5", "trades"):
                file = source(raw, day, venue, kind)
                with file.open("rb") as stream:
                    digest = hashlib.file_digest(stream, "sha256").hexdigest()
                audit["sources"][file.name] = {"sha256": digest, "bytes": file.stat().st_size,
                                               "url": url(config, day, venue, kind)}
    data["basis"] = np.log(data["li_mid"] / data["hl_mid"]) * 10_000
    # Exclude start/end windows that would borrow data from another daily file.
    valid &= (grid >= start + max(config["windows_ms"]) * 1_000) & (grid + max(config["horizons_ms"]) * 1_000 < start + DAY_US)
    frame = pd.DataFrame({k: v[valid] if k == "at_us" else v[valid].astype(np.float32)
                          for k, v in data.items()})
    feature_columns = [c for c in frame if c != "at_us" and not c.startswith("target_")]
    frame = frame.loc[np.isfinite(frame[feature_columns].to_numpy()).all(axis=1)]
    audit["usable_rows"] = len(frame)
    audit["label_rows"] = {str(h): int(frame[f"target_{h}"].notna().sum()) for h in config["horizons_ms"]}
    if frame.empty:
        raise ValueError(f"no aligned market observations for {day} {variant}")
    work.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(work / f"{day}_{variant}.parquet", index=False)
    (work / f"{day}_{variant}.json").write_text(json.dumps(audit, indent=2) + "\n")
    print(day, variant, audit["usable_rows"], audit["label_rows"], flush=True)
    return audit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--manifest", type=Path, help="Write receipt-timed source audit as JSON")
    parser.add_argument("--verify-manifest", type=Path, help="Verify SHA-256 hashes against a previous run")
    parser.add_argument("--variants", nargs="+", default=["receive"])
    args = parser.parse_args()
    config = study_config()
    if args.download:
        download(args.raw, config)
    allowed = {"receive", "exchange_clock", *(f"lighter_delay_{ms}" for ms in config["light_delays_ms"])}
    expected = json.loads(args.verify_manifest.read_text()) if args.verify_manifest else None
    audits = {}
    for variant in args.variants:
        if variant not in allowed:
            parser.error(f"unknown variant {variant}")
        for day in config["days"]:
            audit = prepare_day(args.raw, args.work, day, variant, config, variant == "receive")
            if variant == "receive":
                audits[day] = audit
                if expected is not None:
                    for name, record in audit["sources"].items():
                        if expected[day]["sources"][name]["sha256"] != record["sha256"]:
                            raise ValueError(f"archive content changed for {name}")
    if args.manifest:
        if not audits:
            parser.error("--manifest requires the receive variant")
        args.manifest.parent.mkdir(parents=True, exist_ok=True)
        args.manifest.write_text(json.dumps(audits, indent=2) + "\n")


if __name__ == "__main__":
    main()
