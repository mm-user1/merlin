from pathlib import Path
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.post_process import (
    annotate_ft_threshold,
    calculate_comparison_metrics,
    calculate_ft_dates,
    calculate_period_dates,
    calculate_profit_degradation,
    filter_ft_passed_results,
    ft_result_meets_threshold,
    normalize_ft_reject_action,
)


def test_calculate_ft_dates_basic():
    start = pd.Timestamp("2025-05-01", tz="UTC")
    end = pd.Timestamp("2025-09-01", tz="UTC")
    is_end, ft_start, ft_end, is_days, ft_days = calculate_ft_dates(start, end, 30)
    assert ft_end == end
    assert ft_start == end - pd.Timedelta(days=30)
    assert is_end == ft_start
    assert is_days == (is_end - start).days
    assert ft_days == 30


def test_calculate_ft_dates_invalid():
    start = pd.Timestamp("2025-05-01", tz="UTC")
    end = pd.Timestamp("2025-05-10", tz="UTC")
    try:
        calculate_ft_dates(start, end, 10)
    except ValueError as exc:
        assert "FT period" in str(exc)
    else:
        raise AssertionError("Expected ValueError for FT period >= range")


def test_calculate_profit_degradation_annualized():
    is_profit = 10.0
    ft_profit = 5.0
    ratio = calculate_profit_degradation(is_profit, ft_profit, 100, 50)
    assert abs(ratio - 1.0) < 1e-6


def test_calculate_period_dates_oos_only():
    start = pd.Timestamp("2025-05-01", tz="UTC")
    end = pd.Timestamp("2025-11-20", tz="UTC")
    result = calculate_period_dates(
        start,
        end,
        ft_enabled=False,
        oos_enabled=True,
        oos_period_days=30,
    )
    assert result["oos_end"] == end
    assert result["oos_start"] == end - pd.Timedelta(days=30)
    assert result["is_end"] == result["oos_start"]
    assert result["is_days"] == (result["is_end"] - start).days


def test_calculate_period_dates_ft_and_oos():
    start = pd.Timestamp("2025-05-01", tz="UTC")
    end = pd.Timestamp("2025-11-20", tz="UTC")
    result = calculate_period_dates(
        start,
        end,
        ft_enabled=True,
        ft_period_days=15,
        oos_enabled=True,
        oos_period_days=30,
    )
    assert result["oos_start"] == end - pd.Timedelta(days=30)
    assert result["ft_end"] == result["oos_start"]
    assert result["ft_start"] == result["ft_end"] - pd.Timedelta(days=15)
    assert result["is_end"] == result["ft_start"]


def test_calculate_comparison_metrics():
    is_metrics = {
        "net_profit_pct": 20.0,
        "max_drawdown_pct": 5.0,
        "romad": 4.0,
        "sharpe_ratio": 1.5,
        "profit_factor": 1.8,
    }
    ft_metrics = {
        "net_profit_pct": 10.0,
        "max_drawdown_pct": 7.0,
        "romad": 2.0,
        "sharpe_ratio": 1.0,
        "profit_factor": 1.2,
    }
    comparison = calculate_comparison_metrics(is_metrics, ft_metrics, 100, 50)
    assert comparison["max_dd_change"] == 2.0
    assert comparison["romad_change"] == -2.0
    assert comparison["sharpe_change"] == -0.5
    assert comparison["pf_change"] == pytest.approx(-0.6)


def test_ft_threshold_helpers_support_signed_thresholds():
    results = [
        {"trial_number": 1, "ft_net_profit_pct": -4.9},
        {"trial_number": 2, "ft_net_profit_pct": -5.1},
        {"trial_number": 3, "ft_net_profit_pct": 6.0},
    ]

    assert ft_result_meets_threshold(results[0], -5.0) is True
    assert ft_result_meets_threshold(results[1], -5.0) is False
    assert ft_result_meets_threshold(results[2], 5.0) is True

    annotated = annotate_ft_threshold(results, -5.0)
    assert [item["ft_passes_threshold"] for item in annotated] == [True, False, True]
    assert [item["trial_number"] for item in filter_ft_passed_results(annotated)] == [1, 3]


def test_normalize_ft_reject_action_accepts_ui_labels():
    assert normalize_ft_reject_action("Cooldown + Re-optimize") == "cooldown_reoptimize"
    assert normalize_ft_reject_action("no_trade") == "no_trade"
