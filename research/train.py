"""Fit chronological ridge baselines and publish strictly out-of-sample forecasts."""
import argparse
from dataclasses import dataclass
import json
from pathlib import Path

import numpy as np
import pandas as pd

from .features import DAY_US, start_us, study_config


@dataclass
class Moments:
    count: int
    sum_x: np.ndarray
    sum_y: float
    cross_x: np.ndarray
    cross_y: np.ndarray

    def __add__(self, other):
        return Moments(self.count + other.count, self.sum_x + other.sum_x, self.sum_y + other.sum_y,
                       self.cross_x + other.cross_x, self.cross_y + other.cross_y)

    @classmethod
    def from_sample(cls, x, y):
        return cls(len(y), x.sum(axis=0), y.sum(), x.T @ x, x.T @ y)


def ridge(moments, columns, penalty):
    count = moments.count
    average = moments.sum_x[columns] / count
    y_mean = moments.sum_y / count
    covariance = moments.cross_x[np.ix_(columns, columns)] / count - np.outer(average, average)
    scale = np.sqrt(np.maximum(np.diag(covariance), 1e-12))
    covariance /= np.outer(scale, scale)
    cross = (moments.cross_y[columns] / count - average * y_mean) / scale
    coefficients = np.linalg.solve(covariance + penalty * np.eye(len(columns)), cross) / scale
    return coefficients, y_mean - average @ coefficients


def columns_by_model(names):
    hl = [i for i, name in enumerate(names) if name.startswith("hl_")]
    li = [i for i, name in enumerate(names) if name.startswith("li_")]
    return {
        "hyperliquid": hl,
        "lighter": li,
        "two_venue": list(range(len(names))),
        "no_trade_flow": [i for i, name in enumerate(names) if "_flow_" not in name and "_log_volume_" not in name],
        "no_basis": [i for i, name in enumerate(names) if name != "basis"],
    }


def metrics(y, forecast):
    error = y - forecast
    squared = error @ error
    denom = y @ y
    moving = (y != 0) & (forecast != 0)
    return {
        "rows": len(y),
        "r2_zero": float(1 - squared / denom) if denom else None,
        "rmse_bps": float(np.sqrt(squared / len(y))),
        "mae_bps": float(np.abs(error).mean()),
        "direction_hit": float((np.sign(y[moving]) == np.sign(forecast[moving])).mean()) if moving.any() else None,
        "nonzero_return_share": float((y != 0).mean()),
    }


def paired_ci(times, y, better, reference, block_us, seed, draws):
    block = (times - times[0]) // block_us
    _, ids = np.unique(block, return_inverse=True)
    delta = (y - reference) ** 2 - (y - better) ** 2
    numerator = np.bincount(ids, weights=delta)
    denominator = np.bincount(ids, weights=y ** 2)
    if len(numerator) < 2:
        return None
    rng = np.random.default_rng(seed)
    sample = rng.integers(0, len(numerator), (draws, len(numerator)))
    ratios = numerator[sample].sum(axis=1) / denominator[sample].sum(axis=1)
    return {"blocks": len(numerator), "estimate": float(delta.sum() / (y @ y)),
            "ci95": np.quantile(ratios[np.isfinite(ratios)], [.025, .975]).tolist()}


def analyze_variant(work, out, variant, config):
    days = config["days"]
    features = None
    groups = None
    results, models = [], {}
    training = {}
    final = []
    validation_thresholds = None
    for idx, day in enumerate(days):
        path = work / f"{day}_{variant}.parquet"
        frame = pd.read_parquet(path)
        if features is None:
            features = [name for name in frame.columns if name not in {"at_us", "hl_mid", "li_mid"}
                        and not name.startswith("target_")]
            groups = columns_by_model(features)
        x = frame[features].to_numpy(dtype=np.float64)
        time = frame["at_us"].to_numpy()
        start = start_us(day)
        forecast_file = {"available_us": time - start}
        for h in config["horizons_ms"]:
            target = frame[f"target_{h}"].to_numpy(dtype=np.float64)
            valid = np.isfinite(target) & (time + h * 1000 < start + DAY_US)
            y = target[valid]
            xt = x[valid]
            if idx:
                row = {"day": day, "horizon_ms": h, "variant": variant,
                       "train_days": days[:idx], "models": {"zero": metrics(y, np.zeros_like(y))}}
                forecasts = {}
                for name, columns in groups.items():
                    coefficient, intercept = ridge(training[h], columns, config["ridge_penalty"])
                    prediction = xt[:, columns] @ coefficient + intercept
                    forecasts[name] = prediction
                    row["models"][name] = metrics(y, prediction)
                    if day == days[-1]:
                        models[f"{h}_{name}"] = {
                            "intercept": float(intercept),
                            "coefficients": dict(zip([features[i] for i in columns], coefficient.tolist())),
                            "train_days": days[:idx],
                        }
                    if h == config["execution_target_ms"] and variant == "receive" and name == "two_venue":
                        # Forecasts exist even where the future label is missing. No outcome-based selection.
                        forecast_file["prediction_bps"] = x[:, columns] @ coefficient + intercept
                row["two_vs_hl"] = paired_ci(time[valid], y, forecasts["two_venue"], forecasts["hyperliquid"],
                    config["bootstrap_block_minutes"] * 60_000_000, config["seed"], config["bootstrap_draws"])
                if day == days[-1]:
                    final.append(pd.DataFrame({
                        "at_us": time[valid], "horizon_ms": h, "target": y,
                        "hyperliquid": forecasts["hyperliquid"], "two_venue": forecasts["two_venue"],
                    }))
                results.append(row)
                print(day, variant, h, len(y), *(f"{v['r2_zero']:.6f}" for v in row["models"].values()), flush=True)
            if idx < len(days) - 1:
                moments = Moments.from_sample(xt, y)
                training[h] = training[h] + moments if h in training else moments
        if "prediction_bps" in forecast_file:
            if idx == 1:
                validation_thresholds = float(np.quantile(np.abs(forecast_file["prediction_bps"]), config["threshold_quantile"]))
            elif idx > 1:
                pd.DataFrame(forecast_file).to_csv(out / f"{day}_signal.csv", index=False, float_format="%.9f")
    pd.concat(final, ignore_index=True).to_parquet(out / f"holdout_{variant}.parquet", index=False)
    return results, models, validation_thresholds


def sensitivity(out, variants, config):
    base = pd.read_parquet(out / "holdout_receive.parquet")
    # Common held-out row keys across the receive-time variants; same HL targets.
    lagged = [v for v in variants if v.startswith("lighter_delay_")]
    datasets = {"receive": base, **{v: pd.read_parquet(out / f"holdout_{v}.parquet") for v in lagged}}
    common = base[["at_us", "horizon_ms"]]
    for variant in lagged:
        common = common.merge(datasets[variant][["at_us", "horizon_ms"]],
                              on=["at_us", "horizon_ms"], validate="one_to_one")
    result = []
    for name, rows in datasets.items():
        rows = common.merge(rows, on=["at_us", "horizon_ms"], validate="one_to_one")
        for h, group in rows.groupby("horizon_ms"):
            y, hl, cross = (group[c].to_numpy() for c in ("target", "hyperliquid", "two_venue"))
            result.append({
                "variant": name, "horizon_ms": int(h), "rows": len(y),
                "two_venue_r2": metrics(y, cross)["r2_zero"],
                "incremental_r2": paired_ci(group["at_us"].to_numpy(), y, cross, hl,
                    config["bootstrap_block_minutes"] * 60_000_000, config["seed"], config["bootstrap_draws"]),
            })
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--variants", nargs="+", default=["receive"])
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    config = study_config()
    summary = {"config": config, "results": [], "threshold_bps": None}
    for variant in args.variants:
        results, models, threshold = analyze_variant(args.work, args.out, variant, config)
        summary["results"].extend(results)
        if variant == "receive":
            summary["threshold_bps"] = threshold
            (args.out / "model_coefficients.json").write_text(json.dumps(models, indent=2) + "\n")
    if "receive" in args.variants:
        summary["common_holdout_delay"] = sensitivity(args.out, args.variants, config)
    (args.out / "forecast_results.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
