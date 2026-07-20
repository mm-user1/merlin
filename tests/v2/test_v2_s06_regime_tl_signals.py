"""Unit tests for the S06 Regime-TL params, pivot detection, and regime state."""

import numpy as np
import pytest

from strategies.s06_r_trend_v02_b2.signals import build_indicator_arrays
from strategies.s06_r_trend_v02_regime_trendlines_b2.signals import (
    S06RegimeTLParams,
    build_regime_indicator_arrays,
    pivot_confirmations,
    regime_state_array,
)
from strategies.s06_r_trend_v02_regime_trendlines_b2.strategy import normalized_params

from s06_regime_tl_test_helpers import (
    REFERENCE_A,
    REFERENCE_B,
    load_reference,
    merged_reference_params,
    prepared_reference_dataset,
)


def test_params_parse_baseline_aliases_and_regime_fields():
    _, raw_params, _ = load_reference(REFERENCE_B)
    merged = normalized_params(dict(raw_params))
    parsed = S06RegimeTLParams.from_dict(merged)

    # params.json uses the TradingView-facing aliases trailMAOffsetPct and a
    # float stopLP; both must normalize exactly like the S06 B2 package.
    assert parsed.trailMAOffsetEx == 0.0
    assert parsed.stopLP == 2
    assert parsed.useRegime is True
    assert parsed.regimePivotLen == 15
    assert parsed.regimeSlopeFactor == 0.25
    assert parsed.regimeBreakBufferX == 0.5
    assert parsed.entryMode == "Trend @ Square"
    assert parsed.useTrailMA is False


def test_adapter_normalized_params_applies_aliases_before_defaults():
    merged = normalized_params(
        {
            "fastSmoothing": 8,
            "slowSmoothing": 4,
            "trailMAOffsetPct": 0.5,
            "stopLP": 2.0,
        }
    )
    canonical = normalized_params({"trailMAOffsetEx": 0.7, "trailMAOffsetPct": 0.5})

    assert merged["fastSmooth"] == 8
    assert merged["slowSmooth"] == 4
    assert merged["trailMAOffsetEx"] == 0.5
    assert merged["stopLP"] == 2
    assert canonical["trailMAOffsetEx"] == 0.7


def test_params_defaults_preserve_pine_source_defaults():
    parsed = S06RegimeTLParams.from_dict(normalized_params({}))

    assert parsed.useRegime is True
    assert parsed.regimePivotLen == 15
    assert parsed.regimeSlopeFactor == 1.0
    assert parsed.regimeBreakBufferX == 0.0


@pytest.mark.parametrize(
    "overrides",
    [
        {"regimePivotLen": 0},
        {"regimeSlopeFactor": 0.0},
        {"regimeSlopeFactor": float("nan")},
        {"regimeBreakBufferX": -0.25},
    ],
)
def test_params_reject_invalid_regime_values(overrides):
    with pytest.raises(ValueError):
        S06RegimeTLParams.from_dict(normalized_params(overrides))


def test_pivot_confirmations_confirm_exactly_pivot_len_bars_late():
    values = np.array([1.0, 2.0, 5.0, 2.0, 1.0, 1.5, 1.2, 1.0, 0.8], dtype=float)
    pivots = pivot_confirmations(values, 2, "high")

    pivot_indices = np.flatnonzero(np.isfinite(pivots))
    assert pivot_indices.tolist() == [4]
    assert pivots[4] == 5.0  # pivot bar 2, confirmed at bar 2 + pivot_len


def test_pivot_confirmations_reject_ties():
    # Two equal maxima inside each other's window: strict comparison on both
    # sides means neither confirms (Pine ta.pivothigh tie behavior).
    values = np.array([1.0, 5.0, 2.0, 5.0, 1.0, 1.0, 1.0], dtype=float)
    pivots = pivot_confirmations(values, 2, "high")

    assert not np.isfinite(pivots).any()


def test_pivot_confirmations_low_mirror():
    values = np.array([3.0, 2.0, 0.5, 2.0, 3.0, 2.5, 2.8], dtype=float)
    pivots = pivot_confirmations(values, 2, "low")

    pivot_indices = np.flatnonzero(np.isfinite(pivots))
    assert pivot_indices.tolist() == [4]
    assert pivots[4] == 0.5


def _flat_atr(n, value=1.0):
    return np.full(n, value, dtype=float)


def test_regime_state_is_zero_until_first_break_and_flips_up():
    n = 12
    close = np.full(n, 1.0)
    pivot_high = np.full(n, np.nan)
    pivot_low = np.full(n, np.nan)
    # Resistance anchored at bar 2 (pivot bar 0), price 10, slope 2*1/2 = 1/bar:
    # line(t) = 10 - (t - 0).
    pivot_high[2] = 10.0
    close[8] = 4.5  # line(8) = 2 → 4.5 > 2 breaks resistance

    state = regime_state_array(close, _flat_atr(n), pivot_high, pivot_low, 2, 2.0, 0.0)

    assert state[:8].tolist() == [0] * 8
    assert state[8:].tolist() == [1] * (n - 8)


def test_regime_slope_is_frozen_from_confirmation_bar_atr():
    # ATR at the confirmation bar (2.0) gives slope 2*2/2 = 2/bar → line(4) = 2,
    # so close 4.0 at t=4 breaks. If the slope were wrongly taken from the
    # pivot-bar ATR (1.0 → slope 1/bar → line(4) = 6) no break would occur.
    n = 6
    close = np.full(n, 1.0)
    close[4] = 4.0
    atr = np.array([1.0, 1.0, 2.0, 2.0, 2.0, 2.0])
    pivot_high = np.full(n, np.nan)
    pivot_high[2] = 10.0
    pivot_low = np.full(n, np.nan)

    state = regime_state_array(close, atr, pivot_high, pivot_low, 2, 2.0, 0.0)

    assert state[4] == 1


def test_regime_break_buffer_uses_current_bar_atr():
    # line(4) = 10 - 4 = 6. With buffer 0.5 and current ATR 2 the threshold is
    # 7; close 6.5 must NOT break. With current ATR 0.5 the threshold is 6.25
    # and the same close breaks.
    n = 6
    pivot_high = np.full(n, np.nan)
    pivot_high[2] = 10.0
    pivot_low = np.full(n, np.nan)
    close = np.full(n, 1.0)
    close[4] = 6.5

    atr_wide = np.array([1.0, 1.0, 1.0, 1.0, 2.0, 2.0])
    atr_tight = np.array([1.0, 1.0, 1.0, 1.0, 0.5, 0.5])
    state_wide = regime_state_array(close, atr_wide, pivot_high, pivot_low, 2, 2.0, 0.5)
    state_tight = regime_state_array(close, atr_tight, pivot_high, pivot_low, 2, 2.0, 0.5)

    assert state_wide[4] == 0
    assert state_tight[4] == 1


def test_double_break_holds_state_and_consumes_both_lines():
    # res: anchored at conf bar 2 (pivot bar 0), price 10, slope 1 → 10 - t.
    # sup: anchored at conf bar 3 (pivot bar 1), price 0, slope 1 → t - 1.
    # The lines cross at t = 5.5; close 4.5 at t = 6 breaks BOTH (res = 4,
    # sup = 5). Pine holds the previous regime but consumes both lines.
    n = 16
    pivot_high = np.full(n, np.nan)
    pivot_low = np.full(n, np.nan)
    pivot_high[2] = 10.0
    pivot_low[3] = 0.0
    close = np.array([5.0, 5.0, 5.0, 4.0, 4.5, 4.5, 4.5, 9.9, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 20.0, 1.0])
    # t=3: res 7 / sup 2 → close 4.0 inside. t=4: res 6 / sup 3 → 4.5 inside.
    # t=5: res 5 / sup 4 → 4.5 inside. t=6: res 4 / sup 5 → 4.5 breaks both.
    # t=7: close 9.9 would break the old res line (3) — but it was consumed,
    # so the state must stay 0. A fresh pivot high confirming at t=12 (price
    # 15, slope 1 → line 15 - (t - 10)) re-arms resistance; close 20 at t=14
    # then flips UP, proving re-arming after consumption.
    pivot_high[12] = 15.0

    state = regime_state_array(close, _flat_atr(n), pivot_high, pivot_low, 2, 2.0, 0.0)

    assert state[:14].tolist() == [0] * 14
    assert state[14:].tolist() == [1] * (n - 14)


def test_flip_bar_pivot_re_anchors_fresh_line_evaluated_next_bar():
    # Break resistance at t=8 (flip UP, line consumed) while a new pivot high
    # confirms on the same bar. The fresh line (price 9, slope 1, anchor bar 6)
    # must not be evaluated on the flip bar itself; the first possible break of
    # it is t>=9. close(9)=8 > 9-(9-6)=6 breaks it again — state stays UP, but
    # the consumption/re-anchor sequencing is exercised end to end.
    n = 12
    pivot_high = np.full(n, np.nan)
    pivot_low = np.full(n, np.nan)
    pivot_high[2] = 10.0   # line A: 10 - t
    pivot_high[8] = 9.0    # confirms on the flip bar, anchor bar 6: 9 - (t - 6)
    close = np.array([1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 4.5, 8.0, 1.0, 1.0])
    # t=8: line A = 2 → close 4.5 breaks (fresh line 9 - 2 = 7 NOT evaluated
    # this bar even though 4.5 < 7 would not break anyway; the ordering matters
    # for the consumed side). t=9: fresh line = 6 → close 8 breaks again.
    pivot_low_state = regime_state_array(close, _flat_atr(n), pivot_high, pivot_low, 2, 2.0, 0.0)

    assert pivot_low_state[8] == 1
    assert (pivot_low_state[8:] == 1).all()


def test_use_regime_false_returns_exact_s06_b2_baseline_arrays():
    params = S06RegimeTLParams.from_dict(merged_reference_params(REFERENCE_A))
    assert params.useRegime is False
    prepared, _ = prepared_reference_dataset()

    base_arrays = build_indicator_arrays(prepared, params)
    regime_arrays = build_regime_indicator_arrays(prepared, params)

    assert "regime_state" not in regime_arrays
    for name in ("long_signal", "short_signal", "atr", "rolling_low", "rolling_high", "trail_long", "trail_short"):
        np.testing.assert_array_equal(regime_arrays[name], base_arrays[name])


def test_use_regime_true_only_removes_entry_signals():
    params = S06RegimeTLParams.from_dict(merged_reference_params(REFERENCE_B))
    assert params.useRegime is True
    prepared, _ = prepared_reference_dataset()

    base_arrays = build_indicator_arrays(prepared, params)
    regime_arrays = build_regime_indicator_arrays(prepared, params)

    for name in ("long_signal", "short_signal"):
        gated = regime_arrays[name]
        ungated = base_arrays[name]
        assert not (gated & ~ungated).any(), f"{name} gained signals from gating"
        assert gated.sum() < ungated.sum()
    # Execution inputs are untouched by the regime filter.
    for name in ("atr", "rolling_low", "rolling_high", "trail_long", "trail_short"):
        np.testing.assert_array_equal(regime_arrays[name], base_arrays[name])
    state = regime_arrays["regime_state"]
    assert set(np.unique(state)).issubset({-1, 0, 1})
    assert (regime_arrays["long_signal"] <= (state == 1)).all()
    assert (regime_arrays["short_signal"] <= (state == -1)).all()
