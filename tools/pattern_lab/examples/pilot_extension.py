"""Explicit M5 research extension: causal prior-high breakout and downside RMS."""

import numpy as np

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.study import contracts


def parameters(raw):
    contracts.closed_keys(raw, ("lookback",), "breakout parameters")
    value = raw.get("lookback", 20)
    if type(value) is not int or value < 1:
        raise PatternLabDataError("lookback must be a positive integer")
    return {"lookback": value}


def breakout(series, params, features):
    """Close strictly above prior highs; require the whole contiguous window.

    Current high is excluded. Prepared bars are valid observations; slot gaps
    invalidate the window and require a fresh lookback before another decision.
    Warmup bars participate even when they precede the research interval.
    """
    lookback = params["lookback"]
    count = series.row_count
    value = np.zeros(count, dtype=bool)
    valid = np.zeros(count, dtype=bool)
    if count > lookback:
        highs = np.lib.stride_tricks.sliding_window_view(series.high[:-1], lookback)
        contiguous = series.contiguous_with_previous().astype(np.int64)
        prefix = np.concatenate(([0], np.cumsum(contiguous)))
        starts = np.arange(count - lookback)
        valid[lookback:] = prefix[starts + lookback + 1] - prefix[starts + 1] == lookback
        value[lookback:] = (series.close[lookback:] > highs.max(axis=1)) & valid[lookback:]
    return contracts.ConditionValue(value, valid)


def downside_rms(frame):
    """Unannualized descriptive RMS of downside, across ALL valid returns."""
    valid = frame["return_valid"].to_numpy(dtype=bool)
    values = frame.loc[valid, "net_return"].to_numpy(dtype=np.float64)
    if not np.isfinite(values).all():
        raise PatternLabDataError("downside_rms: valid net_return must be finite")
    if not values.size:
        return None
    return float(np.sqrt(np.mean(np.minimum(values, 0.0) ** 2)))


BREAKOUT = contracts.HypothesisDescriptor(
    "prior_high_breakout", "1", breakout, validate_parameters=parameters,
    prior_bars=lambda params: params["lookback"],
    description="Close strictly exceeds the high of every prior lookback bar; gaps rewarm.",
)
DOWNSIDE_RMS = contracts.MetricDescriptor(
    "downside_rms", "1", required_columns=("net_return", "return_valid"),
    unit="fraction", compute=downside_rms,
    description="Descriptive unannualized downside RMS over all valid returns; not Sortino or inference.",
)


def register(context):
    context.register_hypothesis(BREAKOUT)
    context.register_metric(DOWNSIDE_RMS)
