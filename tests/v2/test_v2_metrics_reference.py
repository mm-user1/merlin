import numpy as np
import pandas as pd
import pytest
from backtesting import _stats

from core.engine_v2.metrics_kernel import (
    compute_drawdown_duration_peaks_reference,
    drawdown_series_from_equity,
    max_drawdown_pct_reference,
)


def _assert_matches_backtesting(drawdown):
    expected_duration, expected_peak = _stats.compute_drawdown_duration_peaks(drawdown)
    duration, peak = compute_drawdown_duration_peaks_reference(drawdown)

    pd.testing.assert_series_equal(duration, expected_duration, check_names=False)
    pd.testing.assert_series_equal(peak, expected_peak, check_names=False)


def test_one_bar_unrecovered_trailing_drawdown_convention():
    drawdown = pd.Series([0.0, 0.2, 0.0, 0.1])

    _, peak = compute_drawdown_duration_peaks_reference(drawdown)

    assert peak.iloc[2] == pytest.approx(0.2)
    assert np.isnan(peak.iloc[3])


def test_unrecovered_multi_bar_trailing_drawdown_is_documented():
    drawdown = pd.Series([0.0, 0.05, 0.10, 0.30])

    _, peak = compute_drawdown_duration_peaks_reference(drawdown)

    assert peak.iloc[-1] == pytest.approx(0.30)


def test_reference_helper_agrees_with_current_backtesting_behavior():
    cases = [
        pd.Series([0.0, 0.1, 0.0, 0.2, 0.05, 0.0]),
        pd.Series([0.0, 0.1]),
        pd.Series([0.0, 0.1, 0.3, 0.2]),
    ]

    for drawdown in cases:
        _assert_matches_backtesting(drawdown)


def test_reference_helper_agrees_with_backtesting_on_seeded_curves():
    rng = np.random.default_rng(42)
    for _ in range(5):
        returns = rng.normal(loc=0.001, scale=0.03, size=50)
        equity = 100.0 * np.cumprod(1.0 + returns)
        drawdown = drawdown_series_from_equity(equity)
        _assert_matches_backtesting(drawdown)


def test_max_drawdown_pct_reference_uses_pinned_episode_behavior():
    equity = [100.0, 90.0, 100.0, 95.0]

    assert max_drawdown_pct_reference(equity) == pytest.approx(10.0)
