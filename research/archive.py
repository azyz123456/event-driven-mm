"""Convert the archive to a v5 observe journal for closed-loop simulation."""
import argparse
from datetime import date
import heapq
import json
from pathlib import Path

import numpy as np

from .features import DAY_US, VENUES, read_source, start_us, valid_book

# Fixed before the economic comparison. Dollar limits cover one small BTC order at these dates.
MAKER = dict(mode="hedged", coin="BTC", lighter_market=1, size="0.00030", max_inventory="0.0015",
             max_order_notional="100", min_available_collateral="100", half_spread_bps="0.25",
             min_edge_bps="0.50", slippage_bps="1.00", max_residual="0.00060", max_residual_ms=5000,
             book_stale_ms=2500, account_stale_ms=30000, min_quote_ms=1500, reprice_ticks=5,
             hedge_retry_ms=500, action_timeout_ms=4000)
FEES = dict(hyperliquid_maker="1.5", hyperliquid_taker="4.5", lighter_maker="0", lighter_taker="0")


def decimal(value):
    # All archived inputs here are BTC ticks/lots; render decimal numbers as strings.
    return format(value, ".10f").rstrip("0").rstrip(".") or "0"


def levels(frame, side):
    return np.column_stack([frame[f"{side}[{i}].{field}"].to_numpy()
                            for i in range(5) for field in ("price", "amount")])


def books(raw, day, venue):
    frame = read_source(raw, day, venue, "book_snapshot_5")
    times = frame["local_timestamp"].to_numpy()
    valid = valid_book(frame).to_numpy()
    bids, asks = (levels(frame, side) for side in ("bids", "asks"))
    previous = True
    for seq, (at, okay, bids_, asks_) in enumerate(zip(times, valid, bids, asks)):
        if not okay and not previous:
            continue
        previous = okay

        def normalize(values):
            return [{"price": decimal(px), "size": decimal(sz)} for px, sz in values.reshape(5, 2)
                    if np.isfinite(px) and np.isfinite(sz) and px > 0 and sz > 0]

        yield int(at), 0, dict(type="book", venue=venue, sequence=seq+1, previous=None,
                               bids=normalize(bids_) if okay else [], asks=normalize(asks_) if okay else [])


def trades(raw, day, venue):
    frame = read_source(raw, day, venue, "trades")
    connection = VENUES.index(venue) + 1
    for row in frame.itertuples(index=False):
        yield row.local_timestamp, 1, dict(type="public_trade", venue=venue, id=row.id,
            connection=connection, venue_ms=row.timestamp // 1000,
            trade=dict(price=decimal(row.price), size=decimal(row.amount), aggressor=row.side))


def write_archive(raw, day, out):
    start = start_us(day)
    seq = 0

    def append(stream, at, event, **payload):
        nonlocal seq
        record = dict(seq=seq, local_time_unix_ms=at // 1000, event=event, **payload)
        stream.write(json.dumps(record, separators=(",", ":")) + "\n")
        seq += 1

    def record_input(stream, at, input_):
        elapsed = at - start
        # Retain the archive's microsecond capture precision within the v5 fields.
        append(stream, at, "maker_input", elapsed_ms=elapsed // 1000, input=input_,
               receive_timing=dict(batch_id=seq, read_elapsed_ns=elapsed*1000,
                                   decoded_elapsed_ns=elapsed*1000, read_unix_ns=at*1000))

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("x") as stream:
        append(stream, start, "maker_start", version=5, config=MAKER, seed=71, observe=True,
               capture=dict(hyperliquid=f"tardis:hyperliquid:BTC:{day}", lighter=f"tardis:lighter:1:{day}", feed_copies=1))
        for generation, venue in enumerate(VENUES, 1):
            record_input(stream, start, dict(type="connected", venue=venue, generation=generation))
            prefix = venue + "_"
            record_input(stream, start, dict(type="instrument", venue=venue, instrument=dict(
                asset=0 if venue == "hyperliquid" else 1, size_decimals=5, price_decimals=1,
                min_size="0.00001" if venue == "hyperliquid" else "0.00007", min_notional="10",
                maker_fee=decimal(float(FEES[prefix+"maker"])/10_000),
                taker_fee=decimal(float(FEES[prefix+"taker"])/10_000))))
        close = start + DAY_US - 1_000_000
        ticks = ((at, 2, dict(type="tick")) for at in range(start, start+DAY_US, 100_000))
        stops = [(close, 2, dict(type="stop"))]
        feeds = [*(books(raw, day, venue) for venue in VENUES),
                 *(trades(raw, day, venue) for venue in VENUES), ticks, stops]
        for at, _, item in heapq.merge(*feeds, key=lambda row: row[:2]):
            record_input(stream, at, item)
        append(stream, start+DAY_US-1, "maker_end", summary=dict(clean=True, positions=["0", "0"], live_orders=[]))
    print(day, seq, out, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", required=True, type=Path)
    parser.add_argument("--day", required=True, type=date.fromisoformat, metavar="YYYY-MM-DD")
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    write_archive(args.raw, args.day.isoformat(), args.out)


if __name__ == "__main__":
    main()
