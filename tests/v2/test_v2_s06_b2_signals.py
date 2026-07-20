import json
from pathlib import Path

import numpy as np
import pandas as pd

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from strategies.s06_r_trend_v02.strategy import S06Params, _build_strategy_arrays, _pine_rma
from strategies.s06_r_trend_v02_b2.signals import (
    S06B2Params,
    build_indicator_arrays,
    build_s06_b2_execution_data,
    normalize_parameter_aliases,
    pine_rma,
    trail_ma,
)
from strategies.s06_r_trend_v02_b2.strategy import normalized_params


REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_ROOT = REPO_ROOT / "data" / "baseline_v2" / "s06_r_trend_v02"


def _reference_params(name):
    with (BASELINE_ROOT / name / "params.json").open(encoding="utf-8") as handle:
        return json.load(handle)["strategy_inputs"]


def _prepared_df():
    df = load_data(REPO_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv")
    return prepare_dataset_with_warmup(
        df,
        pd.Timestamp("2025-08-01T00:00:00Z"),
        pd.Timestamp("2025-12-01T00:00:00Z"),
        1000,
    )[0]


def test_alias_mapper_handles_baseline_and_v1_names():
    mapped = normalize_parameter_aliases(
        {
            "fastSmoothing": 8,
            "slowSmoothing": 4,
            "trailMAOffsetPct": 0.5,
            "stopLP": 2.0,
        }
    )

    assert mapped["fastSmooth"] == 8
    assert mapped["slowSmooth"] == 4
    assert mapped["trailMAOffsetEx"] == 0.5
    assert mapped["stopLP"] == 2


def test_adapter_normalized_params_applies_aliases_before_defaults():
    mapped = normalized_params(
        {
            "fastSmoothing": 8,
            "slowSmoothing": 4,
            "trailMAOffsetPct": 0.5,
            "stopLP": 2.0,
        }
    )
    canonical = normalized_params({"fastSmooth": 9, "fastSmoothing": 8})

    assert mapped["fastSmooth"] == 8
    assert mapped["slowSmooth"] == 4
    assert mapped["trailMAOffsetEx"] == 0.5
    assert mapped["stopLP"] == 2
    assert canonical["fastSmooth"] == 9


def test_generated_arrays_match_prepared_dataframe_length_for_references():
    df = _prepared_df()

    for reference in ("reference_a_reversal_trail", "reference_b_trend_bracket"):
        params = S06B2Params.from_dict(_reference_params(reference))
        data = build_s06_b2_execution_data(df, params)

        assert len(data.signals.long_entries) == len(df)
        assert len(data.atr) == len(df)
        assert data.signals.long_entries.dtype == np.bool_
        assert data.signals.short_entries.dtype == np.bool_


def test_signal_prefix_stability_on_sampled_baseline_window():
    df = _prepared_df().iloc[:1250]
    params = S06B2Params.from_dict(_reference_params("reference_b_trend_bracket"))
    full = build_indicator_arrays(df, params)

    for end in (900, 1000, 1100, 1200):
        prefix = build_indicator_arrays(df.iloc[:end], params)
        np.testing.assert_allclose(prefix["atr"][-1:], full["atr"][end - 1 : end], equal_nan=True)
        assert prefix["long_signal"][-1] == full["long_signal"][end - 1]
        assert prefix["short_signal"][-1] == full["short_signal"][end - 1]


def test_atr_rma_convention_matches_existing_pure_helper():
    values = np.array([2.0, 4.0, 6.0, 8.0, 10.0], dtype=float)

    np.testing.assert_allclose(pine_rma(values, 3), _pine_rma(values, 3), equal_nan=True)


def test_indicator_arrays_match_existing_pure_s06_dataprep_on_deterministic_data():
    df = pd.DataFrame(
        {
            "Open": np.linspace(10.0, 20.0, 240),
            "High": np.linspace(10.5, 20.5, 240),
            "Low": np.linspace(9.5, 19.5, 240),
            "Close": np.linspace(10.1, 20.1, 240),
            "Volume": np.ones(240),
        },
        index=pd.date_range("2025-01-01", periods=240, freq="30min", tz="UTC"),
    )
    b2_params = S06B2Params.from_dict(
        {
            "fastLength": 5,
            "fastSmooth": 2,
            "slowLength": 8,
            "slowSmooth": 3,
            "trailMALength": 20,
            "stopLP": 2,
        }
    )
    v1_params = S06Params.from_dict(
        {
            "fastLength": 5,
            "fastSmoothing": 2,
            "slowLength": 8,
            "slowSmoothing": 3,
            "trailMALength": 20,
            "stopLP": 2,
        }
    )

    b2 = build_indicator_arrays(df, b2_params)
    v1 = _build_strategy_arrays(df, v1_params)

    np.testing.assert_allclose(b2["atr"], v1.atr, equal_nan=True)
    np.testing.assert_allclose(b2["rolling_low"], v1.lowest, equal_nan=True)
    np.testing.assert_allclose(b2["rolling_high"], v1.highest, equal_nan=True)
    np.testing.assert_array_equal(b2["long_signal"], v1.long_signal)
    np.testing.assert_array_equal(b2["short_signal"], v1.short_signal)


def test_trail_band_formula_keeps_builtin_one_percent_offset():
    close = pd.Series([100.0, 101.0, 102.0, 103.0])
    ma = trail_ma(close, "SMA", 2).to_numpy()
    params = S06B2Params.from_dict({"trailMALength": 2, "trailMAOffsetEx": 0.0})
    df = pd.DataFrame(
        {
            "Open": close,
            "High": close + 1.0,
            "Low": close - 1.0,
            "Close": close,
            "Volume": 1.0,
        }
    )

    arrays = build_indicator_arrays(df, params)

    np.testing.assert_allclose(arrays["trail_long"], ma * 0.99, equal_nan=True)
    np.testing.assert_allclose(arrays["trail_short"], ma * 1.01, equal_nan=True)
