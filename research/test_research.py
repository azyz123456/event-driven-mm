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


if __name__ == "__main__":
    unittest.main()
