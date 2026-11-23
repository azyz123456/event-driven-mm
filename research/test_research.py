"""Regression checks for causal time alignment and train-only statistics."""
import unittest

import numpy as np
import pandas as pd

from research.evaluate import asof, hourly_equity, interval
from research.features import book_features, trade_features, study_config
from research.train import Moments, metrics, paired_ci, ridge


class ResearchTests(unittest.TestCase):
    def test_asof_never_borrows_next_quote(self):
        times = np.array([100, 200])
        observed = asof(times, np.array([1., 2.]), np.array([90, 100, 150, 200, 251]), 50)
        np.testing.assert_allclose(observed, [np.nan, 1., 1., 2., np.nan], equal_nan=True)

    def test_book_gap_invalidates_future_label(self):
        config = study_config() | {"windows_ms": [1], "horizons_ms": [2],
                                    "fresh_ms": 2, "gap_ms": 1}
        time = [0, 1_000, 3_000, 4_000]
        rows = {"timestamp": time, "local_timestamp": time}
        for i in range(5):
            rows[f"asks[{i}].price"] = [101, 101, 102, 102]
            rows[f"bids[{i}].price"] = [99, 99, 100, 100]
            for side in ("bids", "asks"):
                rows[f"{side}[{i}].amount"] = [1, 1, 1, 1]
        data, good, _ = book_features(pd.DataFrame(rows), np.array([1_000, 4_000]),
                                       "hyperliquid", "receive", config)
        self.assertTrue(good[0])
        self.assertTrue(np.isnan(data["target_2"][0]))

    def test_trade_delay_removes_flow_until_received(self):
        config = study_config() | {"windows_ms": [500]}
        trades = pd.DataFrame({"timestamp": [0], "local_timestamp": [1000],
                               "price": [100.], "amount": [1.], "side": ["buy"]})
        data = trade_features(trades, np.array([500, 1000, 11000]), "lighter", "lighter_delay_100", config)
        np.testing.assert_equal(data["flow_500"], [0., 0., 0.])

    def test_ridge_from_merged_training_moments(self):
        x = np.arange(24.).reshape(12, 2)
        y = x[:, 0] - 3
        combined = Moments.from_sample(x[:5], y[:5]) + Moments.from_sample(x[5:9], y[5:9])
        expected = ridge(Moments.from_sample(x[:9], y[:9]), [0, 1], .01)
        actual = ridge(combined, [0, 1], .01)
        np.testing.assert_allclose(actual[0], expected[0])
        self.assertAlmostEqual(actual[1], expected[1])
        self.assertGreater(metrics(y[9:], x[9:] @ actual[0] + actual[1])["r2_zero"], .99)

    def test_block_bootstrap_and_reconciled_equity(self):
        ci = paired_ci(np.arange(6), np.ones(6), np.ones(6), np.zeros(6), 2, 71, 100)
        self.assertEqual(ci, {"blocks": 3, "estimate": 1., "ci95": [1., 1.]})
        fills = [(10, "lighter", 1, .5, 99., .1, 1), (20, "hyperliquid", -1, .5, 101., .2, 2)]
        tape = {v: (np.array([1]), np.array([100.])) for v in ["lighter", "hyperliquid"]}
        equity = hourly_equity(fills, tape)
        self.assertAlmostEqual(equity[-1], .7)
        self.assertEqual(interval(np.zeros(25), 71), [0., 0.])


class FollowupTests(unittest.TestCase):
    def test_public_bbo_and_trading_clock_are_explicit(self):
        from research.timing import parse
        event = {"channel":"bbo","data":{"coin":"BTC","time":50,
                 "bbo":[{"px":"99","sz":"1"},{"px":"101","sz":"2"}]}}
        self.assertEqual(parse("hyperliquid",event),("bbo",50_000,(99.,101.,1.,2.)))

    def test_clock_diagnostic_matches_rows_and_purges_future_receipts(self):
        import json
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from research.timing import analyze

        start = 100_000_000
        rows = [{"type": "start", "utc_us": start}]
        for i, at in enumerate(range(0, 40_000_000, 100_000)):
            for channel, lag in (("bbo", 300_000), ("l2Book", 400_000), ("ticker", 0)):
                price = 100 + np.sin((at - lag) / 1_000_000) + 0.01 * (i % 3)
                rows.append({
                    "type": "quote", "channel": channel, "at_us": at,
                    "utc_us": start + at, "source_us": start + at - lag,
                    "levels": [price - .01, price + .01, 1, 2],
                })
        rows.append({"type": "end", "at_us": 40_000_000})
        with TemporaryDirectory() as output:
            path = Path(output) / "capture.jsonl"
            path.write_text("\n".join(map(json.dumps, rows)) + "\n")
            results = analyze(path)
        def bbo_500(row):
            return row["channel"] == "bbo" and row["horizon_ms"] == 500

        raw = next(filter(bbo_500, results["scores"]))
        receive, exchange = filter(bbo_500, results["common_clock_scores"])
        self.assertEqual([receive["clock"], exchange["clock"]], ["receive", "exchange"])
        self.assertEqual(receive["holdout_rows"], exchange["holdout_rows"])
        self.assertEqual(receive["train_rows"], exchange["train_rows"])
        self.assertEqual(raw["train_rows"] - exchange["train_rows"], 3)

    def test_matched_quote_uses_trade_occurrence_and_lot(self):
        from research.execution_value import fill_path
        book = np.array([[99.,1.,98.,1.,97.,1.,96.,1.,95.,1.]])
        asks = np.array([[101.,1.,102.,1.,103.,1.,104.,1.,105.,1.]])
        fill, last = fill_path(100,99.,1,.00030,10,np.array([100]),book,asks,
            np.array([120,130,140]),np.array([105,125,130]),np.array([98.,98.,98.]),
            np.array([1.,.00014,.0005]),np.array([False,False,False]),1.)
        self.assertAlmostEqual(fill,.00030)
        self.assertEqual(last,140)

    def test_calibration_rejects_observe_only_reports(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from research.followup import calibrate
        with TemporaryDirectory() as path:
            report = Path(path)/"observe.json"
            report.write_text('{"mode":"fixed_fills","observed":null,"source_end":{"clean":true}}')
            with self.assertRaisesRegex(ValueError,"clean signed"):
                calibrate(report)


if __name__ == "__main__":
    unittest.main()
