import numpy as np
import pytest

from core.engine_v2 import (
    EXECUTION_REASON_BOUNDARY_STATE,
    EXECUTION_REASON_MARGIN_REJECT,
    EXECUTION_REASON_MAX_DAYS_CLOSE_NEXT_OPEN,
    EXECUTION_REASON_NO_CAPITAL_HALT,
    EXECUTION_REASON_SIGNAL_ENTRY_NEXT_OPEN,
    EXECUTION_REASON_STOP_HIT,
    EXECUTION_REASON_TARGET_HIT,
    EXECUTION_REASON_TRAIL_RATCHET,
    GUARDRAIL_FLAG_CLAMP_MODE_USED,
    GUARDRAIL_FLAG_CORRECTED_FILL,
    GUARDRAIL_FLAG_INVALID_STOP_DISTANCE,
    GUARDRAIL_FLAG_LIQUIDATION,
    GUARDRAIL_FLAG_MARGIN_REJECT,
    GUARDRAIL_FLAG_NO_CAPITAL_HALT,
    GUARDRAIL_FLAG_REJECTED_FILL,
    GUARDRAIL_FLAG_ZERO_SIZE_ENTRY,
    ExecutionIntent,
    Signals,
    StandingState,
)


def test_signals_accepts_valid_bool_arrays():
    signals = Signals(
        long_entries=np.array([True, False, False], dtype=bool),
        short_entries=np.array([False, False, True], dtype=bool),
        long_exits=np.array([False, True, False], dtype=bool),
        short_exits=np.array([False, False, False], dtype=bool),
    )

    assert signals.long_entries.dtype == np.bool_
    assert signals.short_entries.tolist() == [False, False, True]


def test_signals_rejects_unequal_entry_lengths():
    with pytest.raises(ValueError, match="short_entries length"):
        Signals(
            long_entries=np.array([True, False], dtype=bool),
            short_entries=np.array([False], dtype=bool),
        )


def test_signals_rejects_non_bool_entry_arrays():
    with pytest.raises(ValueError, match="long_entries must be a boolean array"):
        Signals(
            long_entries=np.array([1, 0], dtype=np.int64),
            short_entries=np.array([False, True], dtype=bool),
        )


def test_signal_level_arrays_are_float_and_allow_nan():
    signals = Signals(
        long_entries=np.array([True, False], dtype=bool),
        short_entries=np.array([False, True], dtype=bool),
        long_entry_levels=np.array([10.0, np.nan], dtype=float),
        short_entry_levels=np.array([np.nan, 9.5], dtype=float),
    )

    assert np.isnan(signals.long_entry_levels[1])
    assert signals.short_entry_levels.dtype == np.float64


def test_signal_level_arrays_reject_non_float_dtype():
    with pytest.raises(ValueError, match="long_entry_levels must be a float array"):
        Signals(
            long_entries=np.array([True, False], dtype=bool),
            short_entries=np.array([False, True], dtype=bool),
            long_entry_levels=np.array([10, 11], dtype=np.int64),
        )


def test_guardrail_flag_constants_are_stable():
    assert GUARDRAIL_FLAG_CORRECTED_FILL == 1
    assert GUARDRAIL_FLAG_REJECTED_FILL == 2
    assert GUARDRAIL_FLAG_INVALID_STOP_DISTANCE == 4
    assert GUARDRAIL_FLAG_ZERO_SIZE_ENTRY == 8
    assert GUARDRAIL_FLAG_MARGIN_REJECT == 16
    assert GUARDRAIL_FLAG_LIQUIDATION == 32
    assert GUARDRAIL_FLAG_NO_CAPITAL_HALT == 64
    assert GUARDRAIL_FLAG_CLAMP_MODE_USED == 128


def test_standing_state_includes_pending_market_close():
    state = StandingState(pending_market_close=True)

    assert state.pending_market_close is True


def test_execution_intent_reason_code_constants_exist():
    reasons = {
        EXECUTION_REASON_SIGNAL_ENTRY_NEXT_OPEN,
        EXECUTION_REASON_MAX_DAYS_CLOSE_NEXT_OPEN,
        EXECUTION_REASON_TRAIL_RATCHET,
        EXECUTION_REASON_STOP_HIT,
        EXECUTION_REASON_TARGET_HIT,
        EXECUTION_REASON_BOUNDARY_STATE,
        EXECUTION_REASON_MARGIN_REJECT,
        EXECUTION_REASON_NO_CAPITAL_HALT,
    }
    intent = ExecutionIntent(action="hold", reason_code=EXECUTION_REASON_BOUNDARY_STATE)

    assert intent.reason_code in reasons
    assert len(reasons) == 8
