"""Small statistics helpers for run_benchmarks.py (standard library only).

Percentiles use linear interpolation between closest ranks (Hyndman & Fan
type 7; the default of numpy.percentile and R's quantile). With n samples the
p-th percentile sits at 0-based position (n - 1) * p / 100.
"""
from __future__ import annotations

import math
import statistics

DRIFT_THRESHOLD_PCT = 15.0


def percentile(values: list[float], p: float) -> float:
    if not values:
        raise ValueError("percentile of empty sample")
    if not 0 <= p <= 100:
        raise ValueError("p must be in [0, 100]")
    xs = sorted(values)
    pos = (len(xs) - 1) * p / 100.0
    lo, hi = math.floor(pos), math.ceil(pos)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def summarize(values: list[float]) -> dict:
    """Descriptive statistics. p99 with ~30-90 samples is interpolated from the
    top one or two observations: treat it as approximate."""
    n = len(values)
    if n == 0:
        raise ValueError("summarize of empty sample")
    q1, q3 = percentile(values, 25), percentile(values, 75)
    return {
        "n": n,
        "mean": statistics.fmean(values),
        "median": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "p99_approximate": n < 100,
        "min": min(values),
        "max": max(values),
        "stdev": statistics.stdev(values) if n > 1 else 0.0,
        "q1": q1,
        "q3": q3,
        "iqr": q3 - q1,
    }


def drift(batch_medians: list[float], threshold_pct: float = DRIFT_THRESHOLD_PCT) -> dict:
    """Spread of control-batch medians relative to the smallest one.
    Flags the comparison for review if the spread exceeds threshold_pct."""
    if not batch_medians:
        raise ValueError("no batch medians")
    lo, hi = min(batch_medians), max(batch_medians)
    spread = 100.0 * (hi - lo) / lo if lo > 0 else math.inf
    return {"batch_medians": batch_medians, "spread_pct": spread,
            "threshold_pct": threshold_pct, "flagged": spread > threshold_pct}


def compare(baseline: list[float], optimized: list[float]) -> dict:
    b, o = percentile(baseline, 50), percentile(optimized, 50)
    return {"baseline_median": b, "optimized_median": o,
            "speedup_ratio": b / o if o > 0 else math.inf,
            "median_change_pct": 100.0 * (o - b) / b}
